FROM golang:1.21.6-alpine AS go-builder

RUN apk upgrade
RUN apk add --no-cache tzdata

WORKDIR /reverse-agent

COPY . .

RUN go build -trimpath -o ./reverse-agent-universal  .

ENTRYPOINT ["./reverse-agent-universal"]
