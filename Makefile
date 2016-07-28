
all: build

build: deps
	go build

deps:
	go get github.com/Masterminds/glide
	glide install
