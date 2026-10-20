# The builder tag must satisfy the go directive in go.mod; GOTOOLCHAIN=local fails fast instead of downloading one.
FROM golang:1.27-alpine AS builder
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY apis/ apis/
COPY gateway/ gateway/
COPY peer/ peer/
COPY internal/ internal/

FROM builder AS build
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags='-s -w' -o /out/ ./cmd/gateway ./cmd/peer

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 xet-pd \
    && install -d -o xet-pd -g xet-pd /data
USER xet-pd
WORKDIR /data
VOLUME /data

FROM runtime AS gateway
COPY --from=build /out/gateway /usr/local/bin/gateway
ENTRYPOINT ["/usr/local/bin/gateway"]

FROM runtime AS peer
COPY --from=build /out/peer /usr/local/bin/peer
ENTRYPOINT ["/usr/local/bin/peer"]
