#!/usr/bin/env bash
# Blue/green cutover: hand ownership of the shared warehouse from one stack to the
# other. Pauses the current active stack FIRST (so it stops triggering), then
# unpauses the target. Rollback = run it again with the args swapped.
#
# Usage:  ./cutover.sh <from_project> <to_project>
#   go live:   ./cutover.sh streaming_blue  streaming_green
#   rollback:  ./cutover.sh streaming_green streaming_blue
set -euo pipefail

FROM="${1:?from project (active now, will be paused)}"
TO="${2:?to project (will become active)}"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "== Cutover: $FROM  ->  $TO =="
echo "1/2  pausing $FROM (it stops owning the warehouse)"
"$HERE/dags-set-state.sh" "$FROM" pause
echo "2/2  unpausing $TO (it becomes active)"
"$HERE/dags-set-state.sh" "$TO" unpause
echo
echo "Done. ACTIVE = $TO   STANDBY = $FROM (still running, for rollback)."
echo "Rollback if needed:  $HERE/cutover.sh $TO $FROM"
