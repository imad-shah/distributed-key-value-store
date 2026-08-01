FROM golang:1.26 AS build

WORKDIR /app

# caches dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /kvstore-server ./cmd/kvstore

FROM alpine:3.22
COPY --from=build /kvstore-server /usr/local/bin/kvstore-server

ENTRYPOINT ["kvstore-server"]
