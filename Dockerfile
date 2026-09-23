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
COPY e2e/cluster_test.go e2e/

FROM builder AS build-app
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags='-s -w' -o /out/xet-pd ./cmd/xet-pd

FROM builder AS build-verify
RUN --mount=type=cache,target=/root/.cache/go-build \
    go test -tags=e2e_cluster -c -o /out/cluster.test ./e2e

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 xet-pd \
    && install -d -o xet-pd -g xet-pd /data
USER xet-pd

FROM runtime AS app
COPY --from=build-app /out/xet-pd /usr/local/bin/xet-pd
VOLUME /data
ENTRYPOINT ["/usr/local/bin/xet-pd"]

# glibc base: hf_xet ships manylinux wheels for this interpreter.
FROM python:3.13-slim AS verify
ENV PYTHONDONTWRITEBYTECODE=1 HF_HUB_DISABLE_TELEMETRY=1 HF_HUB_DISABLE_UPDATE_CHECK=1
RUN python -m pip install --no-cache-dir 'huggingface_hub==1.32.0' 'hf_xet==1.6.0' \
    && useradd --uid 10001 --create-home xet-pd
COPY --from=build-verify /out/cluster.test /usr/local/bin/cluster.test
USER xet-pd
ENTRYPOINT ["/usr/local/bin/cluster.test", "-test.v", "-test.timeout=5m", "-test.run=^TestComposeCluster$"]
