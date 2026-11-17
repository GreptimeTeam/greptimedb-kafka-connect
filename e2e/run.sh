#!/usr/bin/env bash
# Black-box end-to-end test: runs the packaged plugin in a real Connect worker against real Kafka and
# GreptimeDB, and checks the results only through their external interfaces.
#
# Usage: e2e/run.sh [plugin.zip]   (default: target/greptimedb-kafka-connect-*.zip)
# Requires: docker compose, curl, jq, unzip.

set -euo pipefail

cd "$(dirname "$0")"

ZIP="${1:-$(ls ../target/greptimedb-kafka-connect-*.zip 2>/dev/null | head -1 || true)}"
if [[ -z "$ZIP" || ! -f "$ZIP" ]]; then
    echo "Plugin zip not found; run 'mvn package -DskipTests' first or pass its path." >&2
    exit 1
fi

export COMPOSE_PROJECT_NAME=greptimedb-kafka-connect-e2e
GREPTIMEDB_URL="http://localhost:${GREPTIMEDB_HTTP_PORT:-14000}"
CONNECT_URL="http://localhost:${CONNECT_PORT:-18083}"
TOPIC=sensor
DLQ_TOPIC=sensor-dlq
TABLE=e2e_sensor

cleanup() {
    local status=$?
    if [[ $status -ne 0 ]]; then
        echo "--- connector status ---"
        curl -s "$CONNECT_URL/connectors/greptimedb-sink/status" || true
        echo
        echo "--- connect logs (tail) ---"
        docker compose logs --no-color --tail 200 connect || true
    fi
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf .plugins
    exit $status
}
trap cleanup EXIT

log() {
    echo "[e2e] $*"
}

# wait_for <description> <timeout seconds> <command...>
wait_for() {
    local what=$1 timeout=$2
    shift 2
    local deadline=$((SECONDS + timeout))
    until "$@" >/dev/null 2>&1; do
        if ((SECONDS >= deadline)); then
            echo "Timed out after ${timeout}s waiting for $what" >&2
            return 1
        fi
        sleep 1
    done
}

sql() {
    curl -sf "$GREPTIMEDB_URL/v1/sql" --data-urlencode "sql=$1"
}

row_count() {
    sql "SELECT count(*) FROM $TABLE" | jq -r '.output[0].records.rows[0][0] // empty'
}

count_is() {
    [[ "$(row_count)" == "$1" ]]
}

kafka() {
    docker compose exec -T kafka "/opt/kafka/bin/$@"
}

# produce <first index> <count>: JSON records with an integer epoch-millisecond ts
produce() {
    local first=$1 count=$2
    for ((i = first; i < first + count; i++)); do
        printf '{"ts":%d,"device":"d%d","temperature":%d.5,"debug":"x"}\n' $((1700000000000 + i)) $((i % 5)) $((20 + i % 10))
    done | kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic "$TOPIC"
}

connector_running() {
    local status
    status=$(curl -sf "$CONNECT_URL/connectors/greptimedb-sink/status")
    [[ "$(jq -r '.connector.state' <<<"$status")" == RUNNING ]] &&
        [[ "$(jq '[.tasks[] | select(.state == "RUNNING")] | length' <<<"$status")" == 2 ]]
}

log "unpacking $ZIP"
rm -rf .plugins
mkdir .plugins
unzip -q "$ZIP" -d .plugins

log "starting Kafka, GreptimeDB and Connect"
docker compose up -d
wait_for "GreptimeDB" 120 curl -sf "$GREPTIMEDB_URL/health"
wait_for "Connect REST API" 180 bash -c "curl -sf '$CONNECT_URL/connector-plugins' | grep -q GreptimeDBSinkConnector"

kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic "$TOPIC" --partitions 3 --replication-factor 1

log "creating connector"
curl -sf -X PUT -H 'Content-Type: application/json' --data @connector.json \
    "$CONNECT_URL/connectors/greptimedb-sink/config" >/dev/null
wait_for "connector and 2 tasks RUNNING" 60 connector_running

log "producing 200 records and 2 bad records"
produce 0 200
printf '%s\n' \
    '{"device":"d0","temperature":1.5}' \
    '{"ts":1800000000000,"device":"d0","temperature":"hot"}' |
    kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic "$TOPIC"
wait_for "200 rows in $TABLE" 60 count_is 200

log "checking mapped schema"
columns=$(sql "SELECT column_name, semantic_type FROM information_schema.columns WHERE table_name = '$TABLE' ORDER BY column_name" |
    jq -c '.output[0].records.rows')
expected='[["device","TAG"],["temperature","FIELD"],["ts","TIMESTAMP"]]'
if [[ "$columns" != "$expected" ]]; then
    echo "Unexpected columns: $columns, expected $expected" >&2
    exit 1
fi

log "checking dead letter queue"
dlq=$(kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic "$DLQ_TOPIC" \
    --from-beginning --max-messages 2 --timeout-ms 30000 2>/dev/null | wc -l | tr -d ' ')
if [[ "$dlq" != 2 ]]; then
    echo "Expected 2 records in $DLQ_TOPIC, got $dlq" >&2
    exit 1
fi

log "stopping GreptimeDB and producing 100 records during the outage"
docker compose stop greptimedb
produce 200 100
wait_for "the task to retry a failed write" 60 bash -c "docker compose logs connect | grep -q 'retrying the batch'"
docker compose start greptimedb
wait_for "GreptimeDB after restart" 120 curl -sf "$GREPTIMEDB_URL/health"
wait_for "300 rows in $TABLE after the outage" 120 count_is 300
connector_running || { echo "Connector or tasks not RUNNING after the outage" >&2; exit 1; }

log "passed"
