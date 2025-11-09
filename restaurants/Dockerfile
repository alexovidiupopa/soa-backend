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
RUN go build -o restaurants

FROM alpine:3.18
COPY --from=build /app/restaurants /restaurants
ENV PORT=8002
EXPOSE 8002
ENTRYPOINT ["/restaurants"]
