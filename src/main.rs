use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bollard::container::ListContainersOptions;
use bollard::Docker;
use hickory_proto::op::{Header, MessageType, OpCode, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA};
use hickory_proto::rr::{RData, Record, RecordType};
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
use hickory_resolver::error::ResolveErrorKind;
use hickory_resolver::TokioAsyncResolver;
use hickory_server::authority::MessageResponseBuilder;
use hickory_server::server::{Request, RequestHandler, ResponseHandler, ResponseInfo, ServerFuture};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Configuration, read from environment variables.
struct Config {
    /// Root domain that is stripped from incoming queries (e.g. "xxx.yy").
    root_domain: String,
    /// Address the DNS server listens on (UDP + TCP).
    listen_addr: SocketAddr,
    /// TTL for returned records.
    ttl: u32,
    /// Preferred Docker network; if a container is attached to it, only
    /// addresses from that network are returned.
    prefer_network: Option<String>,
    /// Optional fallback DNS server (e.g. Docker's embedded DNS 127.0.0.11:53).
    fallback_dns: Option<SocketAddr>,
    /// How long the container list from the Docker API is cached.
    cache_ttl: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let root_domain = std::env::var("ROOT_DOMAIN")
            .context("ROOT_DOMAIN environment variable is required (e.g. ROOT_DOMAIN=xxx.yy)")?
            .trim_matches('.')
            .to_ascii_lowercase();
        anyhow::ensure!(!root_domain.is_empty(), "ROOT_DOMAIN must not be empty");

        let listen_addr = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:53".to_string())
            .parse()
            .context("LISTEN_ADDR must be a socket address like 0.0.0.0:53")?;

        let ttl = std::env::var("TTL")
            .ok()
            .map(|v| v.parse::<u32>())
            .transpose()
            .context("TTL must be a number of seconds")?
            .unwrap_or(30);

        let prefer_network = std::env::var("DOCKER_NETWORK")
            .ok()
            .map(|n| n.to_ascii_lowercase());

        let fallback_dns = std::env::var("FALLBACK_DNS")
            .ok()
            .map(|v| {
                v.parse::<SocketAddr>()
                    .or_else(|_| v.parse::<IpAddr>().map(|ip| SocketAddr::new(ip, 53)))
            })
            .transpose()
            .context("FALLBACK_DNS must be an IP or socket address like 127.0.0.11:53")?;

        let cache_ttl = std::env::var("DOCKER_CACHE_SECS")
            .ok()
            .map(|v| v.parse::<u64>())
            .transpose()
            .context("DOCKER_CACHE_SECS must be a number of seconds")?
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(2));

        Ok(Self {
            root_domain,
            listen_addr,
            ttl,
            prefer_network,
            fallback_dns,
            cache_ttl,
        })
    }
}

/// One running container: all names it is reachable under, plus its
/// addresses per network.
struct ContainerEntry {
    names: HashSet<String>,
    /// (network name, addresses on that network)
    networks: Vec<(String, Vec<IpAddr>)>,
}

struct ContainerIndex {
    entries: Vec<ContainerEntry>,
}

impl ContainerIndex {
    fn lookup(&self, name: &str, prefer_network: Option<&str>) -> Option<Vec<IpAddr>> {
        let entry = self.entries.iter().find(|e| e.names.contains(name))?;

        if let Some(pref) = prefer_network {
            if let Some((_, ips)) = entry
                .networks
                .iter()
                .find(|(net, ips)| net == pref && !ips.is_empty())
            {
                return Some(ips.clone());
            }
        }

        let mut all: Vec<IpAddr> = entry
            .networks
            .iter()
            .flat_map(|(_, ips)| ips.iter().copied())
            .collect();
        all.sort();
        all.dedup();
        Some(all)
    }
}

/// Resolves container names to IPs via the Docker socket, with a short-lived
/// cache of the container list.
struct DockerResolver {
    docker: Docker,
    cache: Mutex<Option<(Instant, Arc<ContainerIndex>)>>,
    cache_ttl: Duration,
}

impl DockerResolver {
    async fn index(&self) -> Result<Arc<ContainerIndex>> {
        let mut cache = self.cache.lock().await;
        if let Some((built, index)) = cache.as_ref() {
            if built.elapsed() < self.cache_ttl {
                return Ok(index.clone());
            }
        }

        let containers = self
            .docker
            .list_containers(Some(ListContainersOptions::<String> {
                all: false,
                ..Default::default()
            }))
            .await
            .context("listing containers via Docker socket failed")?;

        let mut entries = Vec::with_capacity(containers.len());
        for c in containers {
            let mut names = HashSet::new();
            for n in c.names.unwrap_or_default() {
                names.insert(n.trim_start_matches('/').to_ascii_lowercase());
            }
            // docker compose: the service name is the name you usually want.
            if let Some(labels) = &c.labels {
                if let Some(service) = labels.get("com.docker.compose.service") {
                    names.insert(service.to_ascii_lowercase());
                }
            }

            let mut networks = Vec::new();
            if let Some(nets) = c.network_settings.and_then(|s| s.networks) {
                for (net_name, endpoint) in nets {
                    if let Some(aliases) = &endpoint.aliases {
                        for a in aliases {
                            names.insert(a.to_ascii_lowercase());
                        }
                    }
                    let ips: Vec<IpAddr> = [&endpoint.ip_address, &endpoint.global_ipv6_address]
                        .into_iter()
                        .flatten()
                        .filter_map(|ip_str| ip_str.parse().ok())
                        .collect();
                    networks.push((net_name.to_ascii_lowercase(), ips));
                }
            }

            entries.push(ContainerEntry { names, networks });
        }

        let index = Arc::new(ContainerIndex { entries });
        *cache = Some((Instant::now(), index.clone()));
        Ok(index)
    }
}

struct Handler {
    root_domain: String,
    ttl: u32,
    prefer_network: Option<String>,
    docker: Option<DockerResolver>,
    fallback: Option<TokioAsyncResolver>,
}

impl Handler {
    /// Resolve a container name (root domain already stripped) to addresses.
    /// Empty result means: name unknown.
    async fn resolve(&self, name: &str) -> Vec<IpAddr> {
        if let Some(docker) = &self.docker {
            match docker.index().await {
                Ok(index) => {
                    if let Some(ips) = index.lookup(name, self.prefer_network.as_deref()) {
                        debug!("resolved '{name}' via Docker socket: {ips:?}");
                        return ips;
                    }
                }
                Err(e) => warn!("Docker socket lookup failed, trying fallback: {e:#}"),
            }
        }

        if let Some(resolver) = &self.fallback {
            // Trailing dot: keep the resolver from appending search domains.
            match resolver.lookup_ip(format!("{name}.")).await {
                Ok(lookup) => {
                    let ips: Vec<IpAddr> = lookup.iter().collect();
                    debug!("resolved '{name}' via fallback DNS: {ips:?}");
                    return ips;
                }
                Err(e) if matches!(e.kind(), ResolveErrorKind::NoRecordsFound { .. }) => {}
                Err(e) => warn!("fallback DNS lookup for '{name}' failed: {e:#}"),
            }
        }

        Vec::new()
    }

    async fn handle<R: ResponseHandler>(
        &self,
        request: &Request,
        response_handle: &mut R,
    ) -> Result<ResponseInfo> {
        let builder = MessageResponseBuilder::from_message_request(request);

        if request.header().op_code() != OpCode::Query
            || request.header().message_type() != MessageType::Query
        {
            let response = builder.error_msg(request.header(), ResponseCode::NotImp);
            return Ok(response_handle.send_response(response).await?);
        }

        let query = request.query();
        let qname = query
            .name()
            .to_string()
            .trim_end_matches('.')
            .to_ascii_lowercase();

        // Strip the root domain: "weft.xxx.yy" -> "weft".
        let suffix = format!(".{}", self.root_domain);
        let container_name = match qname.strip_suffix(&suffix) {
            Some(stripped) if !stripped.is_empty() => stripped,
            _ if qname == self.root_domain => {
                // The root domain itself has no address.
                let response = builder.error_msg(request.header(), ResponseCode::NXDomain);
                return Ok(response_handle.send_response(response).await?);
            }
            _ => {
                // Not our zone.
                debug!("refusing out-of-zone query for '{qname}'");
                let response = builder.error_msg(request.header(), ResponseCode::Refused);
                return Ok(response_handle.send_response(response).await?);
            }
        };

        let ips = self.resolve(container_name).await;
        if ips.is_empty() {
            info!("NXDOMAIN for '{qname}' (container '{container_name}' not found)");
            let response = builder.error_msg(request.header(), ResponseCode::NXDomain);
            return Ok(response_handle.send_response(response).await?);
        }

        let record_name = query.original().name().clone();
        let records: Vec<Record> = ips
            .iter()
            .filter_map(|ip| match (query.query_type(), ip) {
                (RecordType::A, IpAddr::V4(v4)) => Some(RData::A(A(*v4))),
                (RecordType::AAAA, IpAddr::V6(v6)) => Some(RData::AAAA(AAAA(*v6))),
                _ => None,
            })
            .map(|rdata| Record::from_rdata(record_name.clone(), self.ttl, rdata))
            .collect();

        info!(
            "{} '{qname}' -> container '{container_name}' -> {} record(s)",
            query.query_type(),
            records.len()
        );

        let mut header = Header::response_from_request(request.header());
        header.set_authoritative(true);
        header.set_recursion_available(false);

        let response = builder.build(header, records.iter(), &[], &[], &[]);
        Ok(response_handle.send_response(response).await?)
    }
}

#[async_trait::async_trait]
impl RequestHandler for Handler {
    async fn handle_request<R: ResponseHandler>(
        &self,
        request: &Request,
        mut response_handle: R,
    ) -> ResponseInfo {
        match self.handle(request, &mut response_handle).await {
            Ok(info) => info,
            Err(e) => {
                warn!("failed to handle request: {e:#}");
                let mut header = Header::response_from_request(request.header());
                header.set_response_code(ResponseCode::ServFail);
                header.into()
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::from_env()?;

    let docker = match Docker::connect_with_local_defaults() {
        Ok(docker) => match docker.ping().await {
            Ok(_) => {
                info!("connected to Docker daemon via local socket");
                Some(docker)
            }
            Err(e) => {
                warn!("Docker socket not reachable ({e}); will keep trying per request");
                Some(docker)
            }
        },
        Err(e) => {
            warn!("cannot set up Docker client: {e}; relying on FALLBACK_DNS only");
            None
        }
    };

    let fallback = config.fallback_dns.map(|addr| {
        info!("fallback DNS: {addr}");
        let resolver_config = ResolverConfig::from_parts(
            None,
            Vec::new(),
            NameServerConfigGroup::from_ips_clear(&[addr.ip()], addr.port(), true),
        );
        let mut opts = ResolverOpts::default();
        opts.ip_strategy = hickory_resolver::config::LookupIpStrategy::Ipv4AndIpv6;
        TokioAsyncResolver::tokio(resolver_config, opts)
    });

    if docker.is_none() && fallback.is_none() {
        anyhow::bail!("neither Docker socket nor FALLBACK_DNS available - nothing to resolve with");
    }

    let handler = Handler {
        root_domain: config.root_domain.clone(),
        ttl: config.ttl,
        prefer_network: config.prefer_network.clone(),
        docker: docker.map(|docker| DockerResolver {
            docker,
            cache: Mutex::new(None),
            cache_ttl: config.cache_ttl,
        }),
        fallback,
    };

    let mut server = ServerFuture::new(handler);
    server.register_socket(
        UdpSocket::bind(config.listen_addr)
            .await
            .with_context(|| format!("binding UDP {}", config.listen_addr))?,
    );
    server.register_listener(
        TcpListener::bind(config.listen_addr)
            .await
            .with_context(|| format!("binding TCP {}", config.listen_addr))?,
        Duration::from_secs(5),
    );

    info!(
        "docker-dns listening on {} (zone: *.{}, TTL {}s)",
        config.listen_addr, config.root_domain, config.ttl
    );

    server.block_until_done().await?;
    Ok(())
}
