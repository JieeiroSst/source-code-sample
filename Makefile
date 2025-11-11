.PHONY: run build test clean install

run:
	go run cmd/api/main.go

build:
	go build -o bin/app cmd/api/main.go

test:
	go test -v ./...

clean:
	rm -rf bin/

install:
	go mod download

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down
