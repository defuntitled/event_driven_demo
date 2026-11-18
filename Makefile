GO ?= go

.PHONY: test build lint
test:
	GOTOOLCHAIN=local $(GO) test ./...
build:
	GOTOOLCHAIN=local $(GO) build -o /tmp/last-ticket-go-demo ./cmd/demo
lint:
	test -z "$$(gofmt -l cmd internal migrations tests/integration)"
	test -z "$$(goimports -l cmd internal migrations tests/integration)"
	GOTOOLCHAIN=local $(GO) vet -tags=integration ./...
