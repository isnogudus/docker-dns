FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/docker-dns /usr/local/bin/docker-dns
EXPOSE 53/udp 53/tcp
ENTRYPOINT ["/usr/local/bin/docker-dns"]
