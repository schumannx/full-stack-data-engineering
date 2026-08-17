#!/usr/bin/env bash
# Pause or unpause ALL DAGs in one Airflow stack (compose project).
# "Active" = DAGs unpaused. With a SHARED data plane, exactly ONE stack may be
# active at a time, or the two would double-write the warehouse.
#
# Usage:  ./dags-set-state.sh <compose_project> <pause|unpause>
#   e.g.  ./dags-set-state.sh streaming_blue pause
set -euo pipefail

PROJECT="${1:?compose project, e.g. streaming_blue}"
ACTION="${2:?action: pause | unpause}"
[ "$ACTION" = "pause" ] || [ "$ACTION" = "unpause" ] || { echo "action must be pause|unpause"; exit 1; }

cd "$(dirname "$0")/../../airflow"   # run compose from the airflow/ dir

echo "[$PROJECT] ${ACTION} all DAGs"
# Airflow 3's CLI mixes [info] log lines into stdout, so parse the `-o plain`
# table: skip the header and the timestamped log lines (which start with a digit);
# a real dag_id is the first field and starts with a letter.
docker compose -p "$PROJECT" exec -T airflow-scheduler airflow dags list -o plain 2>/dev/null \
  | awk '$1 != "dag_id" && $1 ~ /^[A-Za-z]/ {print $1}' \
  | while read -r dag; do
      [ -z "$dag" ] && continue
      # </dev/null so `exec` doesn't swallow the while-loop's piped stdin.
      docker compose -p "$PROJECT" exec -T airflow-scheduler airflow dags "$ACTION" "$dag" </dev/null >/dev/null 2>&1 \
        && echo "  $ACTION  $dag"
    done
