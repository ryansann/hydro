run: build
	./hydrokv

build:
	go build -o hydrokv cmd/server/main.go

clean: rm-bin rm-file-data rm-fileseg-data

rm-bin:
	-rm -f hydrokv

rm-file-data:
	-test -f $(HYDRO_FILE_STORAGE_FILEPATH) && rm -f $(HYDRO_FILE_STORAGE_FILEPATH)

rm-fileseg-data:
	-test -d $(HYDRO_FILESEG_STORAGE_DIR) && rm -rf $(HYDRO_FILESEG_STORAGE_DIR)

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