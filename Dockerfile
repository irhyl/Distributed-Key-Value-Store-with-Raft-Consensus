FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /raftkv-server .
RUN go build -o /raftkv-cli ./client/

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /raftkv-server /usr/local/bin/raftkv-server
COPY --from=builder /raftkv-cli    /usr/local/bin/raftkv-cli

# Data directory for WAL and SSTables
VOLUME ["/data"]

EXPOSE 7001

ENTRYPOINT ["raftkv-server"]
