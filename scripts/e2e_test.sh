#!/usr/bin/env bash
# End-to-end tests — exercises every Lambda operation against a running MiniStack.
# Requires: curl, python3, a deployed function (run `make dev` first).
#
# Uses curl instead of `aws lambda invoke` because MiniStack always includes
# X-Amz-Log-Result (accumulated Lambda logs) in every response header, which
# quickly exceeds Python http.client's 65536-byte per-header-line limit and
# causes botocore to raise ConnectionClosedError.
#
# Usage:
#   make local-e2e
#   FUNCTION_NAME=my-fn S3_BUCKET=my-bucket scripts/e2e_test.sh
set -euo pipefail

ENDPOINT="${LOCAL_ENDPOINT:-http://localhost:4566}"
REGION="${LOCAL_REGION:-us-east-1}"
FUNCTION="${FUNCTION_NAME:-deltalake-ingestion}"
S3_BUCKET="${S3_BUCKET:-my-delta-lake-bucket}"
TABLE_URI="s3://${S3_BUCKET}/tables/e2e-test-$$"

TMPDIR_E2E=$(mktemp -d)
trap 'rm -rf "$TMPDIR_E2E"' EXIT

PASS=0
FAIL=0

# ── helpers ───────────────────────────────────────────────────────────────────

green() { printf '\033[32m%s\033[0m' "$*"; }
red()   { printf '\033[31m%s\033[0m' "$*"; }

invoke_lambda() {
    local payload_file="$1"
    local response_file="$2"
    # MiniStack always includes X-Amz-Log-Result (all accumulated Lambda logs,
    # unbounded) in every invocation response. Both `aws lambda invoke` (Python
    # http.client, 64KB limit) and curl (CURLE_TOO_LARGE, ~100KB) fail once
    # enough invocations accumulate. Raw socket bypasses all header size limits.
    python3 - "$ENDPOINT" "$FUNCTION" "$payload_file" "$response_file" <<'PYEOF'
import socket, sys, urllib.parse

endpoint, function, payload_file, output_file = sys.argv[1:]
parsed = urllib.parse.urlparse(endpoint)
host, port = parsed.hostname, parsed.port or 80

with open(payload_file, "rb") as f:
    payload = f.read()

request = (
    f"POST /2015-03-31/functions/{function}/invocations HTTP/1.1\r\n"
    f"Host: {host}:{port}\r\n"
    f"Content-Type: application/json\r\n"
    f"Content-Length: {len(payload)}\r\n"
    f"Connection: close\r\n"
    f"\r\n"
).encode() + payload

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
s.settimeout(30.0)
s.sendall(request)

data = b""
while True:
    chunk = s.recv(65536)
    if not chunk:
        break
    data += chunk
s.close()

sep = data.find(b"\r\n\r\n")
if sep < 0:
    sys.exit(1)

status = data[:data.find(b"\r\n")].decode()
if "200" not in status:
    sys.exit(1)

with open(output_file, "wb") as f:
    f.write(data[sep + 4:])
PYEOF
}

# run_test LABEL PAYLOAD_JSON PYTHON_ASSERT
# PYTHON_ASSERT: a Python expression using `d` (parsed response dict); must be truthy to pass.
run_test() {
    local label="$1"
    local payload="$2"
    local check="${3:-d.get('success') is True}"

    local payload_file="$TMPDIR_E2E/payload.json"
    local response_file="$TMPDIR_E2E/response.json"

    printf '  %-52s' "$label"

    printf '%s' "$payload" > "$payload_file"

    if ! invoke_lambda "$payload_file" "$response_file" 2>/dev/null; then
        echo "$(red FAIL) (invoke error — is MiniStack running and function deployed?)"
        FAIL=$((FAIL + 1))
        return
    fi

    local ok
    ok=$(python3 - "$response_file" "$check" <<'PYEOF'
import json, sys
path, expr = sys.argv[1], sys.argv[2]
d = json.loads(open(path).read())
try:
    result = eval(expr)
    print("1" if result else "0")
    if not result:
        print(json.dumps(d, indent=2), file=sys.stderr)
except Exception as e:
    print("0")
    print(f"assert error: {e}", file=sys.stderr)
    print(json.dumps(d, indent=2), file=sys.stderr)
PYEOF
    )

    if [ "$ok" = "1" ]; then
        echo "$(green PASS)"
        PASS=$((PASS + 1))
    else
        echo "$(red FAIL)"
        echo "         response: $(cat "$response_file")"
        FAIL=$((FAIL + 1))
    fi
}

# ── preflight ─────────────────────────────────────────────────────────────────

echo ""
echo "=== E2E: $FUNCTION  @  $ENDPOINT ==="
echo "    table: $TABLE_URI"
echo ""

if ! curl -sf "${ENDPOINT}/_ministack/health" > /dev/null 2>&1; then
    echo "$(red ERROR) MiniStack not reachable at $ENDPOINT — run \`make local-start\` first."
    exit 1
fi

if ! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
   aws --endpoint-url="$ENDPOINT" --region "$REGION" --no-cli-pager \
       lambda get-function --function-name "$FUNCTION" > /dev/null 2>&1; then
    echo "$(red ERROR) Function '$FUNCTION' not found — run \`make dev\` first."
    exit 1
fi

# ── test cases ────────────────────────────────────────────────────────────────

# 1. create_table
run_test "create_table (schema + partitions)" \
    "{\"operation\":\"create_table\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"schema\":[{\"name\":\"id\",\"data_type\":\"long\",\"nullable\":false},{\"name\":\"event_type\",\"data_type\":\"string\",\"nullable\":true},{\"name\":\"value\",\"data_type\":\"double\",\"nullable\":true},{\"name\":\"created_at\",\"data_type\":\"timestamp\",\"nullable\":true}],\"partition_columns\":[\"event_type\"]}}" \
    "d.get('success') is True and d['result']['version'] == 0"

# 2. create_table idempotent (same URI again — must not error)
run_test "create_table (idempotent re-create)" \
    "{\"operation\":\"create_table\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"schema\":[{\"name\":\"id\",\"data_type\":\"long\",\"nullable\":false},{\"name\":\"event_type\",\"data_type\":\"string\",\"nullable\":true},{\"name\":\"value\",\"data_type\":\"double\",\"nullable\":true},{\"name\":\"created_at\",\"data_type\":\"timestamp\",\"nullable\":true}],\"partition_columns\":[\"event_type\"]}}" \
    "d.get('success') is True"

# 3. insert 3 rows
run_test "insert (3 rows)" \
    "{\"operation\":\"insert\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"records\":[{\"id\":1,\"event_type\":\"click\",\"value\":1.5,\"created_at\":1704067200000000},{\"id\":2,\"event_type\":\"view\",\"value\":2.0,\"created_at\":1704067260000000},{\"id\":3,\"event_type\":\"click\",\"value\":0.75,\"created_at\":1704067320000000}],\"partition_columns\":[\"event_type\"]}}" \
    "d.get('success') is True and d['result']['rows_written'] == 3 and d['result']['version'] == 1"

# 4. insert empty (no-op)
run_test "insert (empty — no-op)" \
    "{\"operation\":\"insert\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"records\":[]}}" \
    "d.get('success') is True and d['result']['rows_written'] == 0"

# 5. upsert: update id=1, insert id=4
run_test "upsert (update id=1, insert id=4)" \
    "{\"operation\":\"upsert\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"records\":[{\"id\":1,\"event_type\":\"click\",\"value\":9.99,\"created_at\":1704067200000000},{\"id\":4,\"event_type\":\"purchase\",\"value\":49.99,\"created_at\":1704153600000000}],\"merge_predicate\":\"target.id = source.id\",\"match_columns\":[\"id\"]}}" \
    "d.get('success') is True and d['result']['num_target_rows_updated'] == 1 and d['result']['num_target_rows_inserted'] == 1"

# 6. get_schema
run_test "get_schema (4 fields)" \
    "{\"operation\":\"get_schema\",\"table_uri\":\"$TABLE_URI\",\"payload\":{}}" \
    "d.get('success') is True and len(d['result']['schema']['fields']) == 4"

# 7. table_history
run_test "table_history (>= 3 commits)" \
    "{\"operation\":\"table_history\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"limit\":10}}" \
    "d.get('success') is True and d['result']['total_commits'] >= 3"

# 8. table_history with limit=2
run_test "table_history (limit=2 respected)" \
    "{\"operation\":\"table_history\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"limit\":2}}" \
    "d.get('success') is True and d['result']['total_commits'] == 2"

# 9. time_travel to version 0 (empty table)
run_test "time_travel (version=0)" \
    "{\"operation\":\"time_travel\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"version\":0}}" \
    "d.get('success') is True and d['result']['version'] == 0"

# 10. time_travel to version 1 (after first insert)
run_test "time_travel (version=1)" \
    "{\"operation\":\"time_travel\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"version\":1}}" \
    "d.get('success') is True and d['result']['version'] == 1"

# 11. optimize (z-order by id — partition columns are not allowed in z-order)
run_test "optimize (z-order by id)" \
    "{\"operation\":\"optimize\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"zorder_columns\":[\"id\"]}}" \
    "d.get('success') is True"

# 12. vacuum dry-run
run_test "vacuum (dry_run=true)" \
    "{\"operation\":\"vacuum\",\"table_uri\":\"$TABLE_URI\",\"payload\":{\"retention_hours\":168,\"dry_run\":true}}" \
    "d.get('success') is True and d['result']['dry_run'] is True"

# 13. unknown operation → success=false, not a crash
run_test "unknown operation (error path)" \
    "{\"operation\":\"does_not_exist\",\"table_uri\":\"$TABLE_URI\",\"payload\":{}}" \
    "d.get('success') is False and d.get('error') is not None"

# 14. delete_table (must be last)
run_test "delete_table" \
    "{\"operation\":\"delete_table\",\"table_uri\":\"$TABLE_URI\",\"payload\":{}}" \
    "d.get('success') is True"

# ── summary ───────────────────────────────────────────────────────────────────

echo ""
echo "─────────────────────────────────────────────────"
if [ "$FAIL" -eq 0 ]; then
    echo "$(green "ALL $PASS TESTS PASSED")"
else
    echo "$(red "$FAIL FAILED"), $PASS passed"
fi
echo ""

[ "$FAIL" -eq 0 ]
