.PHONY: client tracing monitor ack1 ack2 clean all

all: server coord client tracing

server:
	go build -o bin/server ./cmd/server

coord:
	go build -o bin/coord ./cmd/coord

client:
	go build -o bin/client ./cmd/client

tracing:
	go build -o bin/tracing ./cmd/tracing-server

example:
	go build -o bin/monitor ./cmd/fcheck-monitor

ackexample:
	go build -o bin/ack1 ./cmd/fcheck-ack1

ackexample:
	go build -o bin/ack2 ./cmd/fcheck-ack2

clean:
	rm -f bin/*
