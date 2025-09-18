# Sort targets alphabetically.
.PHONY: build code_gen format go_lint go_lint_fix go_mod_tidy install lint multiline_sql_strings_lint_fix run sqlc sql_lint sql_lint_fix user-install vendor test test-simple

build:
	go build -o pg-schema-diff ./cmd/pg-schema-diff

code_gen: go_mod_tidy sqlc

go_lint:
	golangci-lint run

go_lint_fix:
	golangci-lint run --fix

go_mod_tidy:
	go mod tidy

install: build
	@echo "Installing pg-schema-diff to /usr/local/bin. This may require sudo."
	install -m 0755 pg-schema-diff /usr/local/bin/pg-schema-diff

lint: go_lint multiline_sql_strings_lint_fix sql_lint

lint_fix: go_lint_fix multiline_sql_strings_lint_fix sql_lint_fix

multiline_sql_strings_lint_fix:
	go run ./scripts/lint/multiline_sql_strings_lint.go --fix

run:
	go run ./cmd/pg-schema-diff

sqlc:
	cd internal/queries && sqlc generate

sql_lint:
	sqlfluff lint

sql_lint_fix:
	sqlfluff fix

user-install:
	@echo "Installing pg-schema-diff to $(shell go env GOPATH)/bin"
	go install ./cmd/pg-schema-diff/...

vendor:
	go mod vendor

test: build
	./pg-schema-diff plan --verbose --debug \
		--from-file /home/prateek/PROJECTS/ExpenseFlow/pg-schema-diff/schema-versioning/staged-version/_init.sql \
		--from-file /home/prateek/PROJECTS/ExpenseFlow/pg-schema-diff/schema-versioning/staged-version/public_schema.sql \
		--to-file /home/prateek/PROJECTS/ExpenseFlow/pg-schema-diff/schema-versioning/next-version/compare/_init.sql \
		--to-file /home/prateek/PROJECTS/ExpenseFlow/pg-schema-diff/schema-versioning/next-version/compare/public_schema.sql \
		--temp-db-dsn 'postgresql://xflow_user:rat12cat@localhost:5432/xflow_temp?sslmode=disable' \
		--output-format sql \
		--output-file ./schema-versioning/migrations/0068_up.sql

