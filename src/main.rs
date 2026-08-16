use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bollard::container::ListContainersOptions;
use bollard::models::PortTypeEnum;
use bollard::Docker;
use hickory_proto::op::{Header, MessageType, OpCode, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA, SRV};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
use hickory_resolver::error::ResolveErrorKind;
use hickory_resolver::TokioAsyncResolver;
use hickory_server::authority::MessageResponseBuilder;
use hickory_server::server::{
    Request, RequestHandler, ResponseHandler, ResponseInfo, ServerFuture,
};
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

/// Transport protocol of an SRV name (`_tcp` / `_udp`) or an exposed port.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Proto {
    Tcp,
    Udp,
}

/// Label prefix for explicit per-service SRV ports: `docker-dns.srv.http=8080`.
const LABEL_SRV_PREFIX: &str = "docker-dns.srv.";
/// Label for the default SRV port of a container: `docker-dns.port=8080`.
const LABEL_PORT: &str = "docker-dns.port";

/// Well-known service names -> port, used to pick among several exposed
/// ports when no label says otherwise (`_https._tcp.web` -> 443 if exposed).
const WELL_KNOWN_PORTS: &[(&str, u16)] = &[
    ("http", 80),
    ("https", 443),
    ("ldap", 389),
    ("ldaps", 636),
    ("smtp", 25),
    ("submission", 587),
    ("smtps", 465),
    ("imap", 143),
    ("imaps", 993),
    ("pop3", 110),
    ("pop3s", 995),
    ("ssh", 22),
    ("dns", 53),
    ("postgresql", 5432),
    ("mysql", 3306),
    ("redis", 6379),
    ("amqp", 5672),
    ("mqtt", 1883),
];

/// One running container: all names it is reachable under, its addresses
/// per network, its exposed ports and labels.
struct ContainerEntry {
    names: HashSet<String>,
    /// (network name, addresses on that network)
    networks: Vec<(String, Vec<IpAddr>)>,
    /// Exposed container ports (EXPOSE / --expose / -p), deduplicated.
    ports: Vec<(u16, Proto)>,
    labels: HashMap<String, String>,
}

/// Result of a container lookup, detached from the index so it can be
/// returned across the cache lock. The fallback-DNS path yields IPs only.
#[derive(Debug, Default)]
struct Resolved {
    ips: Vec<IpAddr>,
    ports: Vec<(u16, Proto)>,
    labels: HashMap<String, String>,
}

impl Resolved {
    /// Pick the port for an SRV query `_<service>._<proto>.<name>`.
    /// Precedence: label `docker-dns.srv.<service>`, then label
    /// `docker-dns.port`, then the well-known port of `<service>` if exposed,
    /// then the lowest exposed port of that protocol.
    fn srv_port(&self, service: Option<&str>, proto: Proto) -> Option<u16> {
        if let Some(service) = service {
            if let Some(p) = self
                .labels
                .get(&format!("{LABEL_SRV_PREFIX}{service}"))
                .and_then(|v| v.trim().parse::<u16>().ok())
            {
                return Some(p);
            }
        }
        if let Some(p) = self
            .labels
            .get(LABEL_PORT)
            .and_then(|v| v.trim().parse::<u16>().ok())
        {
            return Some(p);
        }

        let exposed = || {
            self.ports
                .iter()
                .filter(|(_, pr)| *pr == proto)
                .map(|(p, _)| *p)
        };

        if let Some(service) = service {
            if let Some((_, wk)) = WELL_KNOWN_PORTS.iter().find(|(s, _)| *s == service) {
                if exposed().any(|p| p == *wk) {
                    return Some(*wk);
                }
            }
        }

        exposed().min()
    }
}

struct ContainerIndex {
    entries: Vec<ContainerEntry>,
}

impl ContainerIndex {
    fn lookup(&self, name: &str, prefer_network: Option<&str>) -> Option<Resolved> {
        let entry = self.entries.iter().find(|e| e.names.contains(name))?;

        let preferred = prefer_network.and_then(|pref| {
            entry
                .networks
                .iter()
                .find(|(net, ips)| net == pref && !ips.is_empty())
                .map(|(_, ips)| ips.clone())
        });

        let ips = preferred.unwrap_or_else(|| {
            let mut all: Vec<IpAddr> = entry
                .networks
                .iter()
                .flat_map(|(_, ips)| ips.iter().copied())
                .collect();
            all.sort();
            all.dedup();
            all
        });

        Some(Resolved {
            ips,
            ports: entry.ports.clone(),
            labels: entry.labels.clone(),
        })
    }
}

/// A query name with the root domain already stripped, split into an
/// optional SRV prefix and the container name:
/// `_http._tcp.weft` -> service "http", proto Tcp, name "weft";
/// `weft` -> no service, proto Tcp, name "weft".
#[derive(Debug, PartialEq, Eq)]
struct ParsedName<'a> {
    service: Option<&'a str>,
    proto: Proto,
    name: &'a str,
}

fn parse_name(stripped: &str) -> Option<ParsedName<'_>> {
    if stripped.is_empty() {
        return None;
    }
    let mut labels = stripped.splitn(3, '.');
    let first = labels.next()?;
    if let Some(service) = first.strip_prefix('_') {
        let proto = match labels.next() {
            Some("_tcp") => Proto::Tcp,
            Some("_udp") => Proto::Udp,
            _ => return None,
        };
        let name = labels.next().filter(|n| !n.is_empty())?;
        if service.is_empty() {
            return None;
        }
        return Some(ParsedName {
            service: Some(service),
            proto,
            name,
        });
    }
    Some(ParsedName {
        service: None,
        proto: Proto::Tcp,
        name: stripped,
    })
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
            let labels = c.labels.unwrap_or_default();
            // docker compose: the service name is the name you usually want.
            if let Some(service) = labels.get("com.docker.compose.service") {
                names.insert(service.to_ascii_lowercase());
            }

            let mut ports: Vec<(u16, Proto)> = c
                .ports
                .unwrap_or_default()
                .into_iter()
                .filter_map(|p| {
                    let proto = match p.typ {
                        Some(PortTypeEnum::TCP) => Proto::Tcp,
                        Some(PortTypeEnum::UDP) => Proto::Udp,
                        _ => return None,
                    };
                    Some((p.private_port, proto))
                })
                .collect();
            ports.sort();
            ports.dedup();

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

            entries.push(ContainerEntry {
                names,
                networks,
                ports,
                labels,
            });
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
    /// Resolve a container name (root domain and any SRV prefix already
    /// stripped). `None` means: name unknown.
    async fn resolve(&self, name: &str) -> Option<Resolved> {
        if let Some(docker) = &self.docker {
            match docker.index().await {
                Ok(index) => {
                    if let Some(resolved) = index.lookup(name, self.prefer_network.as_deref()) {
                        debug!("resolved '{name}' via Docker socket: {resolved:?}");
                        return Some(resolved);
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
                    if !ips.is_empty() {
                        return Some(Resolved {
                            ips,
                            ..Default::default()
                        });
                    }
                }
                Err(e) if matches!(e.kind(), ResolveErrorKind::NoRecordsFound { .. }) => {}
                Err(e) => warn!("fallback DNS lookup for '{name}' failed: {e:#}"),
            }
        }

        None
    }

    fn address_records(&self, name: &Name, ips: &[IpAddr], qtype: RecordType) -> Vec<Record> {
        ips.iter()
            .filter_map(|ip| match (qtype, ip) {
                (RecordType::A, IpAddr::V4(v4)) => Some(RData::A(A(*v4))),
                (RecordType::AAAA, IpAddr::V6(v6)) => Some(RData::AAAA(AAAA(*v6))),
                _ => None,
            })
            .map(|rdata| Record::from_rdata(name.clone(), self.ttl, rdata))
            .collect()
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

        // Strip the root domain: "weft.xxx.yy" -> "weft",
        // "_http._tcp.weft.xxx.yy" -> "_http._tcp.weft".
        let suffix = format!(".{}", self.root_domain);
        let stripped = match qname.strip_suffix(&suffix) {
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

        let Some(parsed) = parse_name(stripped) else {
            info!("NXDOMAIN for '{qname}' (malformed name)");
            let response = builder.error_msg(request.header(), ResponseCode::NXDomain);
            return Ok(response_handle.send_response(response).await?);
        };
        let container_name = parsed.name;

        let Some(resolved) = self.resolve(container_name).await else {
            info!("NXDOMAIN for '{qname}' (container '{container_name}' not found)");
            let response = builder.error_msg(request.header(), ResponseCode::NXDomain);
            return Ok(response_handle.send_response(response).await?);
        };

        let qtype = query.query_type();
        let record_name = query.original().name().clone();
        let mut answers: Vec<Record> = Vec::new();
        let mut additionals: Vec<Record> = Vec::new();

        match (qtype, parsed.service) {
            // SRV: `_http._tcp.weft.xxx.yy` -> target weft.xxx.yy + port.
            (RecordType::SRV, _) => {
                match resolved.srv_port(parsed.service, parsed.proto) {
                    Some(port) => {
                        let target = Name::from_ascii(format!(
                            "{container_name}.{}.",
                            self.root_domain
                        ))
                        .context("building SRV target name")?;
                        answers.push(Record::from_rdata(
                            record_name.clone(),
                            self.ttl,
                            RData::SRV(SRV::new(0, 0, port, target.clone())),
                        ));
                        // Glue: address records for the target, saves the
                        // client a round trip.
                        additionals.extend(self.address_records(&target, &resolved.ips, RecordType::A));
                        additionals.extend(self.address_records(&target, &resolved.ips, RecordType::AAAA));
                    }
                    None => info!(
                        "no port for SRV '{qname}': container '{container_name}' exposes no {:?} port \
                         and has no {LABEL_PORT}/{LABEL_SRV_PREFIX}* label",
                        parsed.proto
                    ),
                }
            }
            // A/AAAA on the plain container name.
            (RecordType::A | RecordType::AAAA, None) => {
                answers = self.address_records(&record_name, &resolved.ips, qtype);
            }
            // Anything else (A on an SRV name, MX, TXT, ...): the name exists,
            // but has no such records -> NODATA (NOERROR, empty answer).
            _ => {}
        }

        info!(
            "{qtype} '{qname}' -> container '{container_name}' -> {} record(s)",
            answers.len()
        );

        let mut header = Header::response_from_request(request.header());
        header.set_authoritative(true);
        header.set_recursion_available(false);

        let response = builder.build(header, answers.iter(), &[], &[], additionals.iter());
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::from_env()?;

    // Honors DOCKER_HOST (unix://, tcp://, ...); defaults to the local socket.
    let docker = match Docker::connect_with_defaults() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_name() {
        assert_eq!(
            parse_name("weft"),
            Some(ParsedName {
                service: None,
                proto: Proto::Tcp,
                name: "weft"
            })
        );
        // Dots inside container names stay part of the name.
        assert_eq!(parse_name("a.b").unwrap().name, "a.b");
    }

    #[test]
    fn parse_srv_name() {
        assert_eq!(
            parse_name("_http._tcp.weft"),
            Some(ParsedName {
                service: Some("http"),
                proto: Proto::Tcp,
                name: "weft"
            })
        );
        assert_eq!(parse_name("_dns._udp.coredns").unwrap().proto, Proto::Udp);
        assert_eq!(
            parse_name("_ldap._tcp.ldap.stack").unwrap().name,
            "ldap.stack"
        );
    }

    #[test]
    fn parse_rejects_malformed_srv() {
        assert_eq!(parse_name("_http._sctp.weft"), None);
        assert_eq!(parse_name("_http.weft"), None);
        assert_eq!(parse_name("_http._tcp"), None);
        assert_eq!(parse_name("_._tcp.weft"), None);
        assert_eq!(parse_name(""), None);
    }

    fn resolved(ports: &[(u16, Proto)], labels: &[(&str, &str)]) -> Resolved {
        Resolved {
            ips: vec![],
            ports: ports.to_vec(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn srv_port_prefers_service_label() {
        let r = resolved(
            &[(80, Proto::Tcp)],
            &[("docker-dns.srv.http", "8080"), ("docker-dns.port", "9000")],
        );
        assert_eq!(r.srv_port(Some("http"), Proto::Tcp), Some(8080));
        // Other services fall through to the generic port label.
        assert_eq!(r.srv_port(Some("grpc"), Proto::Tcp), Some(9000));
        assert_eq!(r.srv_port(None, Proto::Tcp), Some(9000));
    }

    #[test]
    fn srv_port_uses_well_known_when_exposed() {
        let r = resolved(&[(80, Proto::Tcp), (443, Proto::Tcp)], &[]);
        assert_eq!(r.srv_port(Some("http"), Proto::Tcp), Some(80));
        assert_eq!(r.srv_port(Some("https"), Proto::Tcp), Some(443));
        // Unknown service: lowest exposed port of that protocol.
        assert_eq!(r.srv_port(Some("grpc"), Proto::Tcp), Some(80));
        assert_eq!(r.srv_port(None, Proto::Tcp), Some(80));
    }

    #[test]
    fn srv_port_respects_protocol() {
        let r = resolved(&[(8080, Proto::Tcp), (53, Proto::Udp)], &[]);
        assert_eq!(r.srv_port(Some("dns"), Proto::Udp), Some(53));
        assert_eq!(r.srv_port(Some("dns"), Proto::Tcp), Some(8080));
        assert_eq!(resolved(&[], &[]).srv_port(Some("http"), Proto::Tcp), None);
    }

    #[test]
    fn srv_port_ignores_invalid_label() {
        let r = resolved(&[(3000, Proto::Tcp)], &[("docker-dns.port", "nope")]);
        assert_eq!(r.srv_port(None, Proto::Tcp), Some(3000));
    }
}
