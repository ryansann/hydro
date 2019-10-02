run: build
	./hydrokv

build:
	go build -o hydrokv cmd/server/main.go

protoc:
	protoc --go_out=. ./pb/*.proto

test:
	go test -race ./...

testv:
	go test -race -v ./...

bench:
	go test -race -benchmem -bench=. ./...

benchv:
	go test -race -v -benchmem -bench=. ./...