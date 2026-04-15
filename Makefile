FUNCTION_NAME ?= deltalake-ingestion
ROLE_ARN      ?= arn:aws:iam::$(AWS_ACCOUNT_ID):role/$(FUNCTION_NAME)-role
REGION        ?= us-east-1
MEMORY        ?= 1024
TIMEOUT       ?= 300
S3_BUCKET     ?= my-delta-lake-bucket
TABLE_PATH    ?= tables/events

.PHONY: build release deploy invoke-create invoke-insert invoke-schema \
        invoke-history invoke-upsert invoke-vacuum invoke-optimize \
        invoke-time-travel fmt lint test clean check

# ── Build ─────────────────────────────────────────────────────────────────────

## Build debug binary for Lambda (ARM64)
build:
	cargo lambda build --arm64

## Build release binary for Lambda (ARM64, optimized for size)
release:
	cargo lambda build --release --arm64

## Check Arrow version alignment (must resolve to a single version)
check:
	cargo tree -i arrow-array 2>/dev/null || cargo tree | grep arrow-array

# ── Deploy ────────────────────────────────────────────────────────────────────

## Deploy to AWS Lambda (runs release build first)
deploy: release
	cargo lambda deploy \
		--region $(REGION) \
		--iam-role $(ROLE_ARN) \
		--memory $(MEMORY) \
		--timeout $(TIMEOUT) \
		--env-var AWS_REGION=$(REGION) \
		--env-var RUST_LOG=info \
		--env-var AWS_S3_ALLOW_UNSAFE_RENAME=true \
		$(FUNCTION_NAME)

# ── Local invocations (cargo lambda invoke) ───────────────────────────────────

TABLE_URI := s3://$(S3_BUCKET)/$(TABLE_PATH)

## Create Delta table with 4 columns, partitioned by event_type
invoke-create:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"create_table","table_uri":"$(TABLE_URI)","payload":{"schema":[{"name":"id","data_type":"long","nullable":false},{"name":"event_type","data_type":"string","nullable":true},{"name":"value","data_type":"double","nullable":true},{"name":"created_at","data_type":"timestamp","nullable":true}],"partition_columns":["event_type"]}}'

## Insert 3 sample records
invoke-insert:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"insert","table_uri":"$(TABLE_URI)","payload":{"records":[{"id":1,"event_type":"click","value":1.5,"created_at":1704067200000000},{"id":2,"event_type":"view","value":2.0,"created_at":1704067260000000},{"id":3,"event_type":"click","value":0.75,"created_at":1704067320000000}]}}'

## Upsert: update id=1 value, insert id=4 (new)
invoke-upsert:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"upsert","table_uri":"$(TABLE_URI)","payload":{"records":[{"id":1,"event_type":"click","value":9.99,"created_at":1704067200000000},{"id":4,"event_type":"purchase","value":49.99,"created_at":1704153600000000}],"merge_predicate":"target.id = source.id","match_columns":["id"]}}'

## Return current schema
invoke-schema:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"get_schema","table_uri":"$(TABLE_URI)","payload":{}}'

## Return last 10 commits from the transaction log
invoke-history:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"table_history","table_uri":"$(TABLE_URI)","payload":{"limit":10}}'

## Dry-run VACUUM with 7-day retention
invoke-vacuum:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"vacuum","table_uri":"$(TABLE_URI)","payload":{"retention_hours":168,"dry_run":true}}'

## Optimize with Z-order on event_type
invoke-optimize:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"optimize","table_uri":"$(TABLE_URI)","payload":{"zorder_columns":["event_type"]}}'

## Time travel: read table at version 0
invoke-time-travel:
	cargo lambda invoke $(FUNCTION_NAME) --data-ascii \
	'{"operation":"time_travel","table_uri":"$(TABLE_URI)","payload":{"version":0}}'

# ── Development ───────────────────────────────────────────────────────────────

fmt:
	cargo fmt --all

lint:
	cargo clippy --all-targets -- -D warnings

test:
	cargo test

clean:
	cargo clean
	rm -rf target/lambda
