FROM golang:1.21-alpine AS build
WORKDIR /app

# Copy go.mod and go.sum first (for caching)
COPY go.mod go.sum ./
RUN go mod tidy || true

# Copy source
COPY . .

# Ensure dependencies are fetched
RUN go mod download

# Build binary
RUN go build -o notification

FROM alpine:3.18
COPY --from=build /app/notification /notification
ENV PORT=8004
EXPOSE 8004
ENTRYPOINT ["/notification"]
