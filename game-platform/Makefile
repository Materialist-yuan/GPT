.PHONY: build test check up down loadtest
build:
	mkdir -p bin
	go build -buildvcs=false -o bin/login ./cmd/login
	go build -buildvcs=false -o bin/gateway ./cmd/gateway
	go build -buildvcs=false -o bin/migrate ./cmd/migrate
	go build -buildvcs=false -o bin/robot ./cmd/robot
test:
	go test -race ./...
check:
	go vet ./...
	go test -race ./...
up:
	./scripts/init-env.sh
	docker compose up --build -d
down:
	docker compose down
loadtest:
	docker compose --profile tools run --rm robot
