# docker-dns

A small DNS server that resolves queries under a root domain to the IPs of
Docker containers: with `ROOT_DOMAIN=xxx.yy`, a query for `weft.xxx.yy` is
stripped to the container name `weft`, and its IP is returned as an A/AAAA
record.

## Resolution

1. **Docker socket (preferred):** The list of running containers is fetched
   via the Docker API and the name is matched against container names,
   Compose service names (`com.docker.compose.service` label), and network
   aliases. Container IPs are taken directly from the network endpoints. The
   list is cached briefly (`DOCKER_CACHE_SECS`).
2. **Fallback (optional):** If `FALLBACK_DNS` is set (e.g. Docker's embedded
   DNS `127.0.0.11`, only reachable from inside a container), the stripped
   name is queried there.

Queries outside the root domain are answered with REFUSED, unknown names
inside the zone with NXDOMAIN.

## SRV records (port discovery)

Reverse proxies like Caddy or Ferron can discover upstream *ports* via SRV.
docker-dns answers SRV queries of the form `_<service>._<proto>.<name>.<root>`
with target `<name>.<root>` and the container's port; the target's A/AAAA
records are included as glue in the additional section:

```
$ dig _http._tcp.weft.xxx.yy SRV
_http._tcp.weft.xxx.yy.  30  IN  SRV  0 0 8080 weft.xxx.yy.
weft.xxx.yy.             30  IN  A    172.20.0.5
```

The port is chosen, in this order:

1. Container label `docker-dns.srv.<service>` (e.g. `docker-dns.srv.http=8080`)
2. Container label `docker-dns.port` (default port for any service)
3. The well-known port of `<service>` (http→80, https→443, ldap→389, …) if
   the container exposes it
4. The lowest exposed port (`EXPOSE` / `--expose` / `-p`) of the requested
   protocol (`_tcp` / `_udp`)

If none applies, the answer is empty (NODATA). Set a label when a container
exposes several ports and the guess would be wrong. SRV needs the Docker
socket; the `FALLBACK_DNS` path knows no ports.

Caddy example, using docker-dns via the system resolver (see the unbound
section below):

```
reverse_proxy {
    dynamic srv _http._tcp.weft.xxx.yy
}
```

## Configuration (environment)

| Variable            | Required | Default      | Meaning                                                            |
| ------------------- | -------- | ------------ | ------------------------------------------------------------------ |
| `ROOT_DOMAIN`       | yes      | –            | Root domain stripped from incoming queries (`xxx.yy`)               |
| `LISTEN_ADDR`       | no       | `0.0.0.0:53` | Listen address for UDP **and** TCP                                  |
| `TTL`               | no       | `30`         | TTL of answer records in seconds                                    |
| `DOCKER_NETWORK`    | no       | –            | If the container is attached to this network, only its IPs are returned |
| `FALLBACK_DNS`      | no       | –            | Fallback DNS server (`127.0.0.11` or `1.2.3.4:5353`)                |
| `DOCKER_CACHE_SECS` | no       | `2`          | How long the container list is cached                               |
| `DOCKER_HOST`       | no       | local socket | Honored by the Docker client library                                |
| `RUST_LOG`          | no       | `info`       | Log level (`debug` shows individual lookups)                        |

## Running locally

```bash
ROOT_DOMAIN=xxx.yy LISTEN_ADDR=0.0.0.0:5353 cargo run --release
```

Test:

```bash
dig -p 5353 @127.0.0.1 weft.xxx.yy A
```

## As a container (docker compose)

```yaml
services:
  docker-dns:
    build: .
    restart: unless-stopped
    environment:
      ROOT_DOMAIN: xxx.yy
      FALLBACK_DNS: 127.0.0.11   # Docker's embedded DNS as fallback
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    ports:
      - "53:53/udp"
      - "53:53/tcp"
```

Mounting the socket read-only is sufficient; the program only calls `ping`
and `containers/json`.

## As a container with host networking (Linux only)

```yaml
services:
  docker-dns:
    build: .
    restart: unless-stopped
    network_mode: host
    environment:
      ROOT_DOMAIN: xxx.yy
      # LISTEN_ADDR: 192.168.1.10:53   # optional: bind to a single host IP,
                                       # e.g. when systemd-resolved occupies 127.0.0.53:53
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
```

With host networking the server binds port 53 directly on the host, so there
is no `ports:` section (`-p` is ignored in this mode). `FALLBACK_DNS` is
omitted because Docker's embedded DNS (`127.0.0.11`) only exists inside
bridge-network namespaces — resolution runs purely via the Docker socket.
Note that the returned addresses are bridge-network IPs (e.g. `172.18.x.x`):
reachable from the host itself, but LAN clients need a route to the Docker
host to reach them.

## Delegating the zone from unbound

docker-dns only answers its own zone (everything else is REFUSED), so it is
not a general-purpose resolver. The clean setup is to keep a recursive
resolver like unbound as the LAN DNS and delegate just the container zone.
Run docker-dns on a separate port, e.g. `LISTEN_ADDR=127.0.0.1:5353`, and add
to `unbound.conf`:

```
server:
    do-not-query-localhost: no
    local-zone: "xxx.yy." nodefault

forward-zone:
    name: "xxx.yy."
    forward-addr: 127.0.0.1@5353
```

unbound keeps serving everything else (recursion, caching, DNSSEC); only
`*.xxx.yy` is forwarded to docker-dns. If docker-dns is down, the LAN only
loses the container zone, not general name resolution.
