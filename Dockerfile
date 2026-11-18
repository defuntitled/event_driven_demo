FROM golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0 AS build
WORKDIR /src
ENV CGO_ENABLED=1 GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN --mount=type=cache,target=/root/.cache/go-build go test ./... && go build -trimpath -o /out/demo ./cmd/demo

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN groupadd --gid 1000 demo && useradd --uid 1000 --gid 1000 --no-create-home demo && mkdir /data && chown 1000:1000 /data
COPY --from=build /out/demo /usr/local/bin/demo
USER 1000:1000
ENTRYPOINT ["demo"]
