
all: build

build:
	go build -o "dtv-rabbit" client.go

deps:
	gvt restore
