# Control-plane image for Fly. Build context is the repo root:
#   fly deploy --dockerfile deploy/fly/Dockerfile   (or set in fly.toml)
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ushr-controller ./cmd/controller

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ushr-controller /usr/local/bin/ushr-controller
COPY deploy/fly/controller.yaml /etc/ushr/controller.yaml
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ushr-controller", "-config", "/etc/ushr/controller.yaml"]
