
# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /logman \
    ./cmd/cluster

# Runtime stage
FROM alpine:3.22

WORKDIR /app

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S logman \
    && adduser -S logman -G logman

COPY --from=builder /logman /app/logman
COPY configs/ /app/configs/

RUN mkdir -p /app/data/node1 /app/data/node2 /app/data/node3 \
    && chown -R logman:logman /app

USER logman

EXPOSE 50051 8081

ENTRYPOINT ["/app/logman"]
