# CGO is on because the cold tier links DuckDB. The builder is the bookworm
# variant so the binary links glibc that debian:bookworm-slim actually has;
# the default golang image is newer and the binary then fails at startup
# with GLIBC_2.38 not found. The runtime image carries CA certificates for
# Pelican and for the one-time extension download.
FROM golang:1.26-bookworm AS builder
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY *.go .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o influxdb-data-api

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/influxdb-data-api /influxdb-data-api
ENTRYPOINT [ "/influxdb-data-api" ]
