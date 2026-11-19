#!/bin/sh
# Fixture usability smoke: one put/get roundtrip against EVERY fixture store
# (specs/lakehouse/10-testing.md §1 — Azurite and fake-gcs are mandatory
# parity targets, so they must be proven usable in CI-fast, not just up).
#
# Run by `make lakehouse-up` after the containers are healthy; can also be
# re-run standalone: src/test/lakehouse/fixtures/smoke.sh
set -eu

NET=$(docker compose -f "$(dirname "$0")/../docker-compose.yml" -p pgdatalake ls --format json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Network"])' 2>/dev/null || echo pgdatalake_default)

RCLONE_ENV="-e RCLONE_CONFIG_S3_TYPE=s3 -e RCLONE_CONFIG_S3_PROVIDER=Other
 -e RCLONE_CONFIG_S3_ACCESS_KEY_ID=lakehouse-test
 -e RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=lakehouse-test-password
 -e RCLONE_CONFIG_S3_ENDPOINT=http://s3:9000
 -e RCLONE_CONFIG_AZ_TYPE=azureblob
 -e RCLONE_CONFIG_AZ_ACCOUNT=devstoreaccount1
 -e RCLONE_CONFIG_AZ_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
 -e RCLONE_CONFIG_AZ_ENDPOINT=http://azurite:10000/devstoreaccount1"

rclone() { docker run --rm --network "$NET" $RCLONE_ENV rclone/rclone:latest "$@"; }

# --- S3 (SeaweedFS) ---------------------------------------------------------
rclone mkdir s3:warehouse
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
echo "s3 smoke $STAMP" | docker run --rm -i --network "$NET" \
  $RCLONE_ENV rclone/rclone:latest rcat s3:warehouse/_smoke/latest.txt
[ "$(rclone cat s3:warehouse/_smoke/latest.txt)" = "s3 smoke $STAMP" ]
echo "s3: put/get OK"

# --- ADLS Gen2 parity target (Azurite blob) ---------------------------------
rclone mkdir az:warehouse
echo "azurite smoke $(date -u +%Y%m%dT%H%M%SZ)" | docker run --rm -i --network "$NET" \
  $RCLONE_ENV rclone/rclone:latest rcat az:warehouse/_smoke/latest.txt
rclone cat az:warehouse/_smoke/latest.txt > /dev/null
echo "azurite: put/get OK"

# --- GCS parity target (fake-gcs-server) ------------------------------------
# JSON API roundtrip via curl: create bucket (idempotent), upload, download.
GCS=http://fake-gcs:4443
docker run --rm --network "$NET" curlimages/curl:latest -sf -X POST \
  -d '{"name":"warehouse"}' "$GCS/storage/v1/b" > /dev/null || true
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
docker run --rm --network "$NET" curlimages/curl:latest -sf -X POST \
  --data-binary "gcs smoke $STAMP" \
  "$GCS/upload/storage/v1/b/warehouse/o?uploadType=media&name=_smoke/latest.txt" > /dev/null
BODY=$(docker run --rm --network "$NET" curlimages/curl:latest -sf \
  "$GCS/storage/v1/b/warehouse/o/_smoke/latest.txt?alt=media")
[ "$BODY" = "gcs smoke $STAMP" ]
echo "fake-gcs: put/get OK"

echo "fixture smoke: all stores roundtrip OK"
