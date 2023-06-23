POSTGRES_VERSION?=14
POSTGRES_DB=pgfs_test
POSTGRES_USER=pgfs
POSTGRES_PASSWORD=password
POSTGRES_PORT=5432
POSTGRES_URL=postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)

COVERALLS_TOKEN?=$(shell cat .COVERALLS_REPO_TOKEN)

export


.PHONY: test
test:
	@./testing/setup.sh go test ./... --cover


.PHONY: coverage
coverage:
	@./testing/setup.sh goveralls


.PHONY: vet
vet: coverage
	go vet ./...
	gosec --quiet ./...
	govulncheck ./...
	golint ./...
