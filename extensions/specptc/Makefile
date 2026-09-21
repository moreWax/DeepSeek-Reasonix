.PHONY: build test race clean

build:
	mkdir -p bin
	go build -trimpath -buildvcs=false -o bin/reasonix-spec-ptc .

test:
	go test ./...

race:
	go test -race ./...

clean:
	rm -rf bin
