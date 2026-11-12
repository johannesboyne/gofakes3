FROM --platform=$BUILDPLATFORM golang:1.24 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -installsuffix cgo -o /out/main ./cmd/gofakes3

FROM scratch
ADD ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/main /
EXPOSE 9000
CMD ["/main", "-backend", "mem"]
