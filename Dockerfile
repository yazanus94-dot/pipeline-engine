FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /pipeline-engine .

FROM alpine:3.19

RUN addgroup -g 1000 pipeline && adduser -D -u 1000 -G pipeline pipeline
COPY --from=builder /pipeline-engine /usr/local/bin/pipeline-engine
RUN chown pipeline:pipeline /usr/local/bin/pipeline-engine
USER pipeline

ENTRYPOINT ["pipeline-engine"]