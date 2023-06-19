.PHONY: test
test: export POSTGRES_DB=pgfs_test
test: export POSTGRES_USER=pgfs
test: export POSTGRES_PASSWORD=password
test: export POSTGRES_PORT=5432
test: export POSTGRES_URL=postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)
test:
	@./testing/setup.sh go test ./...
