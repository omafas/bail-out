all: test lint

.PHONY: test
test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic .

.PHONY: lint
lint:
	golangci-lint run
