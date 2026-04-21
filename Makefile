FUNCTION_NAME ?= deltalake-ingestion
ROLE_ARN      ?= arn:aws:iam::$(AWS_ACCOUNT_ID):role/$(FUNCTION_NAME)-role
REGION        ?= us-east-1
MEMORY        ?= 1024
TIMEOUT       ?= 300
S3_BUCKET     ?= my-delta-lake-bucket
TABLE_PATH    ?= tables/events

# ── MiniStack / local ─────────────────────────────────────────────────────────
LOCAL_ENDPOINT  ?= http://localhost:4566
LOCAL_REGION    ?= us-east-1
LOCAL_ACCOUNT   ?= 000000000000
BOOTSTRAP_DIR    = target/lambda/bootstrap
CROSS_BINARY     = target/aarch64-unknown-linux-gnu/release/bootstrap
LOCAL_TABLE_URI  = s3://$(S3_BUCKET)/$(TABLE_PATH)

# AWS CLI pre-wired to MiniStack (any credentials work — IAM is not enforced)
AWS_LOCAL = AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
            aws --endpoint-url=$(LOCAL_ENDPOINT) --region $(LOCAL_REGION) --no-cli-pager

# ECR / container image settings
ECR_REGISTRY     = localhost:4566
IMAGE_TAG        ?= latest
LOCAL_IMAGE_URI   = $(ECR_REGISTRY)/$(FUNCTION_NAME):$(IMAGE_TAG)

.PHONY: build release local-release deploy invoke-create invoke-insert invoke-schema \
        invoke-history invoke-upsert invoke-vacuum invoke-optimize \
        invoke-time-travel fmt lint test clean check \
        dev local-start local-stop local-setup local-deploy local-image-deploy local-status \
        local-logs local-logs-last local-list local-browse-s3 local-e2e \
        local-invoke-create local-invoke-insert local-invoke-upsert \
        local-invoke-schema local-invoke-history local-invoke-vacuum \
        local-invoke-optimize local-invoke-time-travel local-invoke-delete

# ── Dev (one-command local setup) ────────────────────────────────────────────

## Start MiniStack, create infra, build and deploy Lambda — full local dev env in one shot
dev: local-setup local-deploy
	@echo ""
	@echo "Dev environment ready:"
	@echo "  Function : $(FUNCTION_NAME)"
	@echo "  S3 bucket: s3://$(S3_BUCKET)/$(TABLE_PATH)"
	@echo "  Endpoint : $(LOCAL_ENDPOINT)"
	@echo ""
	@echo "Useful next steps:"
	@echo "  make local-e2e          — run end-to-end tests"
	@echo "  make local-logs         — tail MiniStack logs"
	@echo "  make local-invoke-schema TABLE_PATH=tables/mytable"

# ── Build ─────────────────────────────────────────────────────────────────────

## Build debug binary for Lambda (ARM64)
build:
	cargo lambda build --arm64

## Build release binary for Lambda (ARM64, optimized for size)
release:
	cargo lambda build --release --arm64

## Build release binary for Linux ARM64 using zigbuild (handles OpenSSL and all C deps — for local deploy)
local-release:
	rustup target add aarch64-unknown-linux-gnu 2>/dev/null || true
	cargo zigbuild --release --target aarch64-unknown-linux-gnu.2.17
	mkdir -p $(BOOTSTRAP_DIR)
	cp $(CROSS_BINARY) $(BOOTSTRAP_DIR)/bootstrap

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

# ── Local / MiniStack ─────────────────────────────────────────────────────────

## Start MiniStack (docker compose) and wait until healthy
local-start:
	docker compose up -d
	@echo "Waiting for MiniStack at $(LOCAL_ENDPOINT) ..."
	@until curl -sf $(LOCAL_ENDPOINT)/_ministack/health > /dev/null 2>&1; do \
		printf '.'; sleep 1; \
	done
	@echo ""
	@echo "MiniStack is ready — $(LOCAL_ENDPOINT)"

## Stop MiniStack
local-stop:
	docker compose down

## Create S3 bucket + IAM execution role in MiniStack
local-setup: local-start
	@echo "--- Creating IAM role ---"
	@$(AWS_LOCAL) iam create-role \
		--role-name $(FUNCTION_NAME)-role \
		--assume-role-policy-document \
		  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
		--query 'Role.Arn' --output text 2>/dev/null || echo "  (role already exists)"
	@echo "--- Creating S3 bucket ---"
	@$(AWS_LOCAL) s3 mb s3://$(S3_BUCKET) 2>/dev/null || echo "  (bucket already exists)"
	@echo "Setup complete"

## Build release binary, package it, and deploy (create or update) to MiniStack
local-deploy: local-release
	@echo "--- Packaging bootstrap binary ---"
	cd $(BOOTSTRAP_DIR) && zip -j bootstrap.zip bootstrap
	@echo "--- Deploying $(FUNCTION_NAME) to MiniStack ---"
	@if $(AWS_LOCAL) lambda get-function --function-name $(FUNCTION_NAME) > /dev/null 2>&1; then \
		echo "  Function exists — updating code..."; \
		$(AWS_LOCAL) lambda update-function-code \
			--function-name $(FUNCTION_NAME) \
			--zip-file fileb://$(BOOTSTRAP_DIR)/bootstrap.zip; \
	else \
		echo "  Creating new function..."; \
		$(AWS_LOCAL) lambda create-function \
			--function-name $(FUNCTION_NAME) \
			--runtime provided.al2023 \
			--architectures arm64 \
			--role arn:aws:iam::$(LOCAL_ACCOUNT):role/$(FUNCTION_NAME)-role \
			--handler bootstrap \
			--zip-file fileb://$(BOOTSTRAP_DIR)/bootstrap.zip \
			--memory-size $(MEMORY) \
			--timeout $(TIMEOUT) \
			--environment '{"Variables":{"RUST_LOG":"info","AWS_REGION":"$(LOCAL_REGION)","AWS_S3_ALLOW_UNSAFE_RENAME":"true","AWS_ALLOW_HTTP":"true","AWS_ENDPOINT_URL":"http://ministack:4566"}}'; \
	fi
	@echo "Deployment done"

## Build Docker image and deploy Lambda as container image (no ECR push needed)
## MiniStack docker executor pulls images directly from the host Docker daemon by name.
local-image-deploy: local-setup local-release
	@echo "--- Building Docker image (linux/arm64) ---"
	docker build --platform linux/arm64 -t $(FUNCTION_NAME):$(IMAGE_TAG) .
	@echo "--- Deploying $(FUNCTION_NAME) as container image ---"
	@$(AWS_LOCAL) lambda delete-function --function-name $(FUNCTION_NAME) 2>/dev/null || true
	@$(AWS_LOCAL) lambda create-function \
		--function-name $(FUNCTION_NAME) \
		--package-type Image \
		--code ImageUri=$(FUNCTION_NAME):$(IMAGE_TAG) \
		--role arn:aws:iam::$(LOCAL_ACCOUNT):role/$(FUNCTION_NAME)-role \
		--architectures arm64 \
		--memory-size $(MEMORY) \
		--timeout $(TIMEOUT) \
		--environment '{"Variables":{"RUST_LOG":"info","AWS_REGION":"$(LOCAL_REGION)","AWS_S3_ALLOW_UNSAFE_RENAME":"true","AWS_ALLOW_HTTP":"true","AWS_ENDPOINT_URL":"http://ministack:4566"}}'
	@echo "Image deployment done"

## Show MiniStack health and service status
local-status:
	@curl -sf $(LOCAL_ENDPOINT)/_ministack/health | python3 -m json.tool

## List deployed Lambda functions and S3 buckets
local-list:
	@echo "=== Lambda functions ==="
	$(AWS_LOCAL) lambda list-functions \
		--query 'Functions[*].[FunctionName,Runtime,State,LastModified]' \
		--output table
	@echo "=== S3 buckets ==="
	$(AWS_LOCAL) s3 ls

## Browse Delta table files written to local S3
local-browse-s3:
	$(AWS_LOCAL) s3 ls s3://$(S3_BUCKET)/$(TABLE_PATH)/ --recursive

## Tail MiniStack container logs (MiniStack does not forward Lambda output to CloudWatch)
local-logs:
	docker logs -f ministack

## Decode the LogResult from a Lambda invocation and print it (reads current schema as probe)
local-logs-last:
	@$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"get_schema","table_uri":"$(LOCAL_TABLE_URI)","payload":{}}' \
		/dev/null \
	| python3 -c "import sys,json,base64; d=json.load(sys.stdin); print(base64.b64decode(d.get('LogResult','')).decode('utf-8','replace'))"

# ── Local invocations (aws lambda invoke → MiniStack) ─────────────────────────

## Create Delta table
local-invoke-create:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"create_table","table_uri":"$(LOCAL_TABLE_URI)","payload":{"schema":[{"name":"id","data_type":"long","nullable":false},{"name":"event_type","data_type":"string","nullable":true},{"name":"value","data_type":"double","nullable":true},{"name":"created_at","data_type":"timestamp","nullable":true}],"partition_columns":["event_type"]}}' \
		/dev/stdout

## Insert sample records
local-invoke-insert:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"insert","table_uri":"$(LOCAL_TABLE_URI)","payload":{"records":[{"id":1,"event_type":"click","value":1.5,"created_at":1704067200000000},{"id":2,"event_type":"view","value":2.0,"created_at":1704067260000000},{"id":3,"event_type":"click","value":0.75,"created_at":1704067320000000}],"partition_columns":["event_type"]}}' \
		/dev/stdout

## Upsert records (update id=1, insert id=4)
local-invoke-upsert:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"upsert","table_uri":"$(LOCAL_TABLE_URI)","payload":{"records":[{"id":1,"event_type":"click","value":9.99,"created_at":1704067200000000},{"id":4,"event_type":"purchase","value":49.99,"created_at":1704153600000000}],"merge_predicate":"target.id = source.id","match_columns":["id"]}}' \
		/dev/stdout

## Return current schema
local-invoke-schema:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"get_schema","table_uri":"$(LOCAL_TABLE_URI)","payload":{}}' \
		/dev/stdout

## Return last 10 commits from the transaction log
local-invoke-history:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"table_history","table_uri":"$(LOCAL_TABLE_URI)","payload":{"limit":10}}' \
		/dev/stdout

## Dry-run VACUUM with 7-day retention
local-invoke-vacuum:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"vacuum","table_uri":"$(LOCAL_TABLE_URI)","payload":{"retention_hours":168,"dry_run":true}}' \
		/dev/stdout

## Optimize with Z-order on event_type
local-invoke-optimize:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"optimize","table_uri":"$(LOCAL_TABLE_URI)","payload":{"zorder_columns":["id"]}}' \
		/dev/stdout

## Time travel: read table at version 0
local-invoke-time-travel:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"time_travel","table_uri":"$(LOCAL_TABLE_URI)","payload":{"version":0}}' \
		/dev/stdout

## Run end-to-end tests against MiniStack (requires: make dev)
local-e2e:
	@LOCAL_ENDPOINT=$(LOCAL_ENDPOINT) LOCAL_REGION=$(LOCAL_REGION) \
	 FUNCTION_NAME=$(FUNCTION_NAME) S3_BUCKET=$(S3_BUCKET) \
	 bash scripts/e2e_test.sh

## Delete all objects in the Delta table prefix on S3
local-invoke-delete:
	$(AWS_LOCAL) lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--cli-binary-format raw-in-base64-out \
		--payload '{"operation":"delete_table","table_uri":"$(LOCAL_TABLE_URI)","payload":{}}' \
		/dev/stdout
