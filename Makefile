
all: build

build: deps
	go build

deps:
	glide install
