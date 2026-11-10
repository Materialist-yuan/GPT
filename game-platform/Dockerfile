# syntax=docker/dockerfile:1
FROM golang:1.26.10-alpine AS build
WORKDIR /src
COPY . .
# Optional proxy CA for managed cloud builds. Normal local builds need no secret.
RUN --mount=type=secret,id=proxy_ca,target=/run/proxy-ca.pem \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    if [ -s /run/proxy-ca.pem ]; then SSL_CERT_FILE=/run/proxy-ca.pem go mod download; else go mod download; fi && \
    CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/login ./cmd/login && \
    CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/migrate ./cmd/migrate && \
    CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/robot ./cmd/robot

FROM gcr.io/distroless/static-debian12:nonroot AS base
WORKDIR /app
USER 65532:65532

FROM base AS login
COPY --from=build /out/login /app/login
EXPOSE 8080
ENTRYPOINT ["/app/login"]

FROM base AS gateway
COPY --from=build /out/gateway /app/gateway
EXPOSE 9000 9091
ENTRYPOINT ["/app/gateway"]

FROM base AS migrate
COPY --from=build /out/migrate /app/migrate
ENTRYPOINT ["/app/migrate"]

FROM base AS robot
COPY --from=build /out/robot /app/robot
ENTRYPOINT ["/app/robot"]
