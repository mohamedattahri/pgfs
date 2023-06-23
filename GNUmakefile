POSTGRES_VERSION?=14
POSTGRES_DB=pgfs_test
POSTGRES_USER=pgfs
POSTGRES_PASSWORD=password
POSTGRES_PORT=5432
POSTGRES_URL=postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)

COVERALLS_TOKEN?=$(shell cat COVERALLS_REPO_TOKEN)

export


# Start a Postgres container, run the tests, then exit.
.PHONY: test
test:
	@./testing/setup.sh go test ./...


# Start a Postgres constainer.
.PHONY: db
db: DOCKER_IMAGE=postgres:$(POSTGRES_VERSION)-alpine
db:
	docker run --env POSTGRES_DB=$(POSTGRES_DB) --env POSTGRES_USER=$(POSTGRES_USER) --env POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) --publish 5432:$(POSTGRES_PORT) $(DOCKER_IMAGE)


# Send test coverage data to coveralls.
.PHONY: coverage
coverage:
	@./testing/setup.sh goveralls


# Vet the codebase.
.PHONY: vet
vet: coverage
	go vet ./...
	gosec --quiet ./...
	govulncheck ./...
	golint ./...
