#!/usr/bin/env bash
#
# A throwaway copy of sead_staging, for SDF tests that write to a database: deploying,
# verifying and reverting change requests with Sqitch (plans/sdf-review-report.md, test plan
# items 3-5).
#
# The copy runs in its own container, outside the compose cluster, from the same image as
# the postgresql service, so PostGIS and Sqitch match. It is filled with pg_dump from the
# running postgresql service, which only reads from it. Nothing else is touched. It listens
# on 127.0.0.1 only and trusts every connection, so no passwords are copied or needed.
#
# Inside the container, /sead_change_control is a copy of the local working tree, so a test
# can add change requests there and deploy them with `podman exec … sqitch` without
# touching the repository. It disappears with the container.
#
# Usage:
#   scripts/sdf/scratch-db.sh run <command…>   create the copy, run the command against it,
#                                               then remove the copy, whatever the outcome
#   scripts/sdf/scratch-db.sh up | down | psql | env
#
# The command runs with PG* (as the owner, for writes) and POSTGRES_* (as json_api_server's
# read-only role, as the conformance scripts expect) pointing at the copy.
#
# Settings: SCRATCH_PORT (55432), SCRATCH_CONTAINER (sead-sdf-scratch),
#           SOURCE_CONTAINER (sead-postgresql-1), SOURCE_DATABASE (sead_staging).

set -euo pipefail

SEAD_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRATCH_PORT="${SCRATCH_PORT:-55432}"
SCRATCH_CONTAINER="${SCRATCH_CONTAINER:-sead-sdf-scratch}"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-sead-postgresql-1}"
SOURCE_DATABASE="${SOURCE_DATABASE:-sead_staging}"
CHANGE_CONTROL="${SEAD_CHANGE_CONTROL:-$SEAD_ROOT/sead_change_control}"

READ_ONLY_USER="$(grep -E '^DATABASE_READ_ONLY_USER=' "$SEAD_ROOT/.env" 2>/dev/null | cut -d= -f2- || true)"
READ_ONLY_USER="${READ_ONLY_USER:-sead_read}"

log() { echo "scratch-db: $*" >&2; }

source_psql() { podman exec -u postgres "$SOURCE_CONTAINER" psql -U postgres -XAt "$@"; }
scratch_exec() { podman exec -u postgres "$@"; }

up() {
    if podman container exists "$SCRATCH_CONTAINER"; then
        log "$SCRATCH_CONTAINER already exists; run '$0 down' first"
        exit 1
    fi
    local image owner started
    image="$(podman inspect --format '{{.ImageName}}' "$SOURCE_CONTAINER")"
    owner="$(source_psql -c "select pg_get_userbyid(datdba) from pg_database where datname = '$SOURCE_DATABASE'")"
    started=$SECONDS

    log "starting $SCRATCH_CONTAINER from $image on 127.0.0.1:$SCRATCH_PORT"
    podman run -d --rm --name "$SCRATCH_CONTAINER" \
        -p "127.0.0.1:$SCRATCH_PORT:5432" \
        --shm-size=1g \
        -e POSTGRES_HOST_AUTH_METHOD=trust \
        -e PGDATA=/pgdata \
        "$image" >/dev/null

    # The image's init runs a temporary server on the socket only; TCP answers once the
    # real server is up.
    until scratch_exec "$SCRATCH_CONTAINER" pg_isready -q -h 127.0.0.1; do sleep 1; done

    log "copying sead_change_control into the container"
    podman exec "$SCRATCH_CONTAINER" rm -rf /sead_change_control
    podman cp "$CHANGE_CONTROL" "$SCRATCH_CONTAINER:/sead_change_control"

    log "copying roles (without passwords)"
    podman exec -u postgres "$SOURCE_CONTAINER" pg_dumpall -U postgres --roles-only --no-role-passwords \
        | grep -v '^CREATE ROLE postgres;$' \
        | scratch_exec -i "$SCRATCH_CONTAINER" psql -U postgres -X -q -v ON_ERROR_STOP=1 -d postgres >/dev/null

    log "copying $SOURCE_DATABASE (owner $owner)"
    podman exec -u postgres "$SOURCE_CONTAINER" pg_dump -U postgres -Fc "$SOURCE_DATABASE" \
        | scratch_exec -i "$SCRATCH_CONTAINER" pg_restore -U postgres --exit-on-error -C -d postgres

    log "ready after $((SECONDS - started)) s: $(scratch_exec "$SCRATCH_CONTAINER" psql -U postgres -XAt -d "$SOURCE_DATABASE" \
        -c "select pg_size_pretty(pg_database_size(current_database())) || ', ' || (select count(*) from sqitch.changes) || ' Sqitch changes'")"
}

down() {
    if podman container exists "$SCRATCH_CONTAINER"; then
        log "removing $SCRATCH_CONTAINER"
        podman rm -f -v "$SCRATCH_CONTAINER" >/dev/null
    fi
}

print_env() {
    cat <<EOF
PGHOST=127.0.0.1
PGPORT=$SCRATCH_PORT
PGDATABASE=$SOURCE_DATABASE
PGUSER=$(source_psql -c "select pg_get_userbyid(datdba) from pg_database where datname = '$SOURCE_DATABASE'")
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=$SCRATCH_PORT
POSTGRES_DATABASE=$SOURCE_DATABASE
POSTGRES_USER=$READ_ONLY_USER
POSTGRES_PASS=
SCRATCH_CONTAINER=$SCRATCH_CONTAINER
EOF
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    env) print_env ;;
    psql) shift; podman exec -it -u postgres "$SCRATCH_CONTAINER" psql -U postgres -X -d "$SOURCE_DATABASE" "$@" ;;
    run)
        shift
        [ $# -gt 0 ] || { log "run needs a command"; exit 2; }
        trap down EXIT
        up
        env $(print_env | xargs) "$@"
        ;;
    *)
        sed -n '3,25p' "$0" | sed 's/^# \{0,1\}//'
        exit 2
        ;;
esac
