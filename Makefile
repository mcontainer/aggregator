BINARY = aggregator
DIR = aggregator
GOARCH = amd64
PREFIX="[INFO]::"
VERSION=1.0
COMMIT=$(shell git rev-parse HEAD)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
SRCS = $(shell git ls-files '*.go')
BUILD_DIR=${GOPATH}/src/docker-visualizer/${DIR}/dist

LDFLAGS = -ldflags "-X main.VERSION=${VERSION} -X main.COMMIT=${COMMIT} -X main.BRANCH=${BRANCH} -linkmode external -extldflags -static"

.PHONY: all

all: clean format linux

proto:
	protoc -I events events/events.proto --go_out=plugins=grpc:events

linux:
	mkdir -p ${BUILD_DIR}; \
	cd ${BUILD_DIR}; \
	GOOS=linux GOARCH=${GOARCH} go build ${LDFLAGS} -o ${BINARY}-linux-${GOARCH} ../ ; \
	cd .. >/dev/null

clean:
	@-rm -f ${BINARY}-*

format:
	gofmt -s -l -w $(SRCS)
