#!/usr/bin/env bash
# End to end with release binaries: builds bank-views, facet and the
# driver, then runs a three-node chron group in process with the daemon
# and the CLI as child processes while the leader is killed and
# restarted. Logs go to $FACET_E2E_LOGS, or a directory under
# /Volumes/chronlogs when that RAM disk exists, else a temp dir.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

secs="${1:-30}"
kills="${2:-3}"
logs="${FACET_E2E_LOGS:-}"
if [[ -z "$logs" && -d /Volumes/chronlogs ]]; then
    logs="/Volumes/chronlogs/facet-e2e-$(date +%H%M%S)"
fi

cargo build --release -p bank-views -p facet-cli -p facet-e2e
args=(--bank-views target/release/bank-views --facet target/release/facet --secs "$secs" --kills "$kills")
if [[ -n "$logs" ]]; then
    args+=(--logs "$logs")
fi
target/release/facet-e2e "${args[@]}"
