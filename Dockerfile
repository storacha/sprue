FROM golang:1.25-bookworm AS build

WORKDIR /go/src/sprue

# Copy go mod files first for caching
COPY go.mod go.sum* ./
RUN go mod download || true

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 go build -o /sprue ./cmd/main.go

# Final image with wget for health checks
FROM alpine:3.19

RUN apk --no-cache add ca-certificates wget curl

COPY --from=build /sprue /usr/bin/sprue

EXPOSE 8080

ENTRYPOINT ["/usr/bin/sprue", "serve"]
