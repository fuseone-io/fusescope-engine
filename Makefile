# Developer entry points. CI runs the same commands.

export FS_TEST_DATABASE_URL ?= postgres://fusescope:fusescope@localhost:5433/fusescope
export FS_TEST_S3_ENDPOINT  ?= http://localhost:9100

.PHONY: dev-up dev-down check test lint deny ci run check-env tenant token bench-stages bench-load bench-query

dev-up:        ## start Postgres and an S3-compatible store, create the dev bucket
	docker compose up -d --wait postgres s3
	docker compose run --rm bucket-init

dev-down:      ## stop them, keeping data volumes
	docker compose down

check:
	cargo check --workspace --all-targets

test:          ## database and bucket tests run when the dev services are reachable
	cargo test --workspace

lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings

deny:
	cargo deny check licenses advisories sources bans

ci: lint test deny

run:           ## run every role against the dev services
	FS_CATALOG_URL=$(FS_TEST_DATABASE_URL) FS_BUCKET_ACCESS_KEY=fusescope FS_BUCKET_SECRET_KEY=fusescope-dev \
	cargo run -p fs-server -- --config config/dev.toml --role all

check-env:     ## validate configuration and connectivity, then exit
	FS_CATALOG_URL=$(FS_TEST_DATABASE_URL) FS_BUCKET_ACCESS_KEY=fusescope FS_BUCKET_SECRET_KEY=fusescope-dev \
	cargo run -p fs-server -- --config config/dev.toml --check

tenant:        ## create a tenant: make tenant NAME=acme
	FS_CATALOG_URL=$(FS_TEST_DATABASE_URL) FS_BUCKET_ACCESS_KEY=fusescope FS_BUCKET_SECRET_KEY=fusescope-dev \
	cargo run -q -p fs-server -- --config config/dev.toml admin tenant create $(NAME)

token:         ## issue a token: make token TENANT=<uuid> NAME=collector [SCOPE=ingest|query]
	FS_CATALOG_URL=$(FS_TEST_DATABASE_URL) FS_BUCKET_ACCESS_KEY=fusescope FS_BUCKET_SECRET_KEY=fusescope-dev \
	cargo run -q -p fs-server -- --config config/dev.toml admin token create $(TENANT) $(NAME) $(or $(SCOPE),ingest)

bench-stages:  ## ingest decomposed by stage, in process, WAL on the project disk
	cargo run --release -q -p fs-bench -- stages --requests $(or $(REQUESTS),5000) $(BENCH_ARGS)

bench-load:    ## OTLP load against a running ingester: make bench-load TOKEN=fst_... [DURATION=30] [CONCURRENCY=8]
	cargo run --release -q -p fs-bench -- load --endpoint 127.0.0.1:4317 --token $(TOKEN) --duration-secs $(or $(DURATION),30) --concurrency $(or $(CONCURRENCY),8) --metrics 127.0.0.1:9464 $(BENCH_ARGS)

bench-query:   ## DESIGN.md query suite over Flight SQL: make bench-query TOKEN=fst_... [ITERATIONS=10]
	cargo run --release -q -p fs-bench -- query --endpoint 127.0.0.1:8815 --token $(TOKEN) --iterations $(or $(ITERATIONS),10) $(BENCH_ARGS)
