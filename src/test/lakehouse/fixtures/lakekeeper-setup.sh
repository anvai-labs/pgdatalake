#!/bin/sh
# One-time Lakekeeper fixture setup: bootstrap + create the `smoke` warehouse
# pointing at the SeaweedFS S3 fixture. Idempotent. Run by `make lakehouse-up`
# after Lakekeeper is serving (M1 slice 1 verification: GET /v1/config).
#
# Route notes (Lakekeeper 0.15): management API lives under /management/v1;
# the catalog API under /catalog/v1; GET /catalog/v1/config REQUIRES a
# `warehouse` query parameter (profile quirk, doc 03 §2).
set -eu

LK=http://localhost:8181
WH_NAME=smoke

# 1. Bootstrap (idempotent: 204 first time, then "already bootstrapped").
curl -sf -X POST "$LK/management/v1/bootstrap" \
  -H 'Content-Type: application/json' \
  -d '{"accept-telemetry": false, "accept-terms-of-use": true,
       "new-user": {"name": "smoke-admin", "email": "smoke@example.com"}}' \
  > /dev/null 2>&1 || true

# 2. Create the warehouse (skip if it already exists).
CODE=$(curl -s -o /tmp/lk-wh-create.json -w '%{http_code}' -X POST \
  "$LK/management/v1/warehouse" \
  -H 'Content-Type: application/json' \
  -d "{\"warehouse-name\": \"$WH_NAME\",
       \"storage-profile\": {\"type\": \"s3\", \"region\": \"us-east-1\",
         \"bucket\": \"warehouse\", \"endpoint\": \"http://s3:9000\",
         \"path-style-access\": true, \"sts-enabled\": false},
       \"storage-credential\": {\"type\": \"s3\", \"credential-type\": \"access-key\",
         \"aws-access-key-id\": \"lakehouse-test\",
         \"aws-secret-access-key\": \"lakehouse-test-password\"}}")
if [ "$CODE" = 409 ] || grep -qE "already exists|StorageProfileOverlap" /tmp/lk-wh-create.json 2>/dev/null; then
  echo "lakekeeper: warehouse '$WH_NAME' already exists"
elif [ "$CODE" != 200 ] && [ "$CODE" != 201 ]; then
  cat /tmp/lk-wh-create.json; echo
  echo "lakekeeper-setup: warehouse creation failed (HTTP $CODE)" >&2
  exit 1
fi

# 3. Smoke the catalog config endpoint the way the Rust client calls it.
curl -sf "$LK/catalog/v1/config?warehouse=$WH_NAME" > /dev/null
echo "lakekeeper: bootstrapped, warehouse '$WH_NAME' ready, /v1/config OK"
