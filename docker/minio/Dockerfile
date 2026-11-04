FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src
RUN git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git .
RUN CGO_ENABLED=0 go build -o /minio .

FROM alpine:3.23
RUN apk add --no-cache ca-certificates
COPY --from=builder /minio /usr/local/bin/minio
ENTRYPOINT ["minio"]
