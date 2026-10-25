# Developer entry points. CI runs the same commands.

export FS_TEST_DATABASE_URL ?= postgres://fusescope:fusescope@localhost:5433/fusescope

.PHONY: dev-up dev-down check test lint deny ci

dev-up:        ## start Postgres and MinIO for local development and tests
	docker compose up -d --wait

dev-down:      ## stop them, keeping data volumes
	docker compose down

check:
	cargo check --workspace --all-targets

test:          ## runs database tests when FS_TEST_DATABASE_URL is reachable
	cargo test --workspace

lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings

deny:
	cargo deny check licenses advisories sources bans

ci: lint test deny
