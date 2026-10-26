# Stage 1: build a static binary
FROM --platform=linux/amd64 golang:1.26-alpine AS builder

WORKDIR /src

# Dependencies are vendored, so the whole tree is copied and built offline
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/main ./cmd/gofakes3

# Stage 2: minimal runtime image
FROM --platform=linux/amd64 gcr.io/distroless/static-debian12
COPY ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/main /main
EXPOSE 9000
CMD ["/main", "-backend", "logiq"]
