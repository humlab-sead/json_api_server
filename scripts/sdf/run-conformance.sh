#!/usr/bin/env bash
#
# Runs, or resumes, the SDF conformance check over every site (T1, export and empty
# re-import) against the local json_api_server, with the credentials from
# sead-deployment's .env. Safe to start again after any interruption: sites already in
# sdf-conformance.jsonl are skipped. Extra arguments go to conformance.mjs, e.g.
# --retry-failed. Detached, so it outlives the terminal:
#
#   setsid nohup scripts/sdf/run-conformance.sh >> sdf-conformance.log 2>&1 < /dev/null &
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
ENV_FILE=../.env
val() { grep -E "^$1=" "$ENV_FILE" | cut -d= -f2-; }
export JAS_PROTECTED_ENDPOINTS_USER="$(val JAS_PROTECTED_ENDPOINTS_USER)"
export JAS_PROTECTED_ENDPOINTS_PASS="$(val JAS_PROTECTED_ENDPOINTS_PASS)"
export POSTGRES_HOST=localhost POSTGRES_PORT="$(val POSTGRESQL_PORT)" POSTGRES_DATABASE="$(val JAS_POSTGRES_DATABASE)"
export POSTGRES_USER="$(val DATABASE_READ_ONLY_USER)" POSTGRES_PASS="$(val DATABASE_READ_ONLY_PASSWORD)"
exec node scripts/sdf/conformance.mjs --all --import --out sdf-conformance.jsonl "$@"
