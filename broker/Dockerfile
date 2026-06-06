FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o dtq-broker ./broker

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/dtq-broker .

CMD ["./dtq-broker"]