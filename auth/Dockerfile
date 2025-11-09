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
RUN go build -o auth

FROM alpine:3.18
COPY --from=build /app/auth /auth
ENV PORT=8000
EXPOSE 8000
ENTRYPOINT ["/auth"]
