FROM rust:slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
 openssh-client \
 git \
 libssl-dev \
 pkg-config \
 librdkafka-dev \
 cmake \
 libzstd-dev   \
 zlib1g-dev \
 libsasl2-dev \
 clang \
 libclang-dev

# Use the cargo git fetch with CLI 
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

COPY . .

# Copy the SSH key
COPY .ssh/id_rsa /root/.ssh/id_rsa

# Setup the ssh config and add github to known hosts 
RUN chmod 600 /root/.ssh/id_rsa && \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN cargo build --release

# Just in case, remove the SSH key
RUN rm -f /root/.ssh/id_rsa

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y \
  libssl3 \
  librdkafka1 \
  libsasl2-2 \
  libzstd1 \
  zlib1g \
  libclang1 \
  ca-certificates \
  && update-ca-certificates \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/msv-tokio-binance-scraper .

RUN mkdir -p ./resources

ENTRYPOINT ["./msv-tokio-binance-scraper"]
