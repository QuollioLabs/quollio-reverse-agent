FROM alpine:3
ADD ./main /go/bin/main
RUN apk upgrade
RUN apk add --no-cache tzdata

ENTRYPOINT ["/go/bin/main"]
