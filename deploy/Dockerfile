# The stock temporalio/auto-setup image cannot run this store: a custom
# datastore is registered in Go via temporal.WithCustomDataStoreFactory(), not
# by a config string, so the binary has to be built with the store compiled in.

FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependency layer first, so source edits do not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/temporal-aerospike-server ./cmd/temporal-aerospike-server

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 temporal

COPY --from=build /out/temporal-aerospike-server /usr/local/bin/temporal-aerospike-server

USER temporal
WORKDIR /etc/temporal

EXPOSE 7233 7243

ENTRYPOINT ["temporal-aerospike-server"]
CMD ["--config-dir", "/etc/temporal/config", "--allow-no-auth", "start"]
