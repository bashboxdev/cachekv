# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/cachekv ./cmd/cachekv

# distroless/static: no shell, no package manager, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cachekv /cachekv
EXPOSE 6379
USER nonroot:nonroot
ENTRYPOINT ["/cachekv"]
CMD ["-addr", ":6379", "-log-json"]
