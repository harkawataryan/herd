# Builds every Herd binary into one small image. Select the binary with the
# container command, e.g. `gateway -config /etc/herd/herd.yaml` or
# `simbackend -addr :9001`.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/gateway ./cmd/gateway \
    && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/simbackend ./cmd/simbackend \
    && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY ui /ui
EXPOSE 8080
ENTRYPOINT ["gateway"]
CMD ["-config", "/etc/herd/herd.yaml"]
