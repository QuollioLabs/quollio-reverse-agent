include .env

.PHONY: help
help:
		@grep "^[a-zA-Z\-]*:" Makefile | grep -v "grep" | sed -e 's/^/make /' | sed -e 's/://'

.PHONY: test
test:
		go test -cover -v ./...

.PHONY: fmt
fmt:
		go fmt ./...

.PHONY: build
build:
		docker build --platform linux/amd64 --no-cache -f ./Dockerfile -t quollio-reverse-agent-universal ./.

.PHONY: run
run:
		go run main.go -system-name=$(SYSTEM_NAME)
