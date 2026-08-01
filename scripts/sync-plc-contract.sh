#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="sync"
if [[ "${1:-}" == "--check" ]]; then
  mode="check"
  shift
fi

plc_repo="${1:-"$repo_root/../aquarium-controller-plc"}"
source_contract="$plc_repo/contracts/mqtt.json"
destination="$repo_root/reef_plc_normalizer/app/contracts/plc_mqtt.json"

if [[ ! -f "$source_contract" ]]; then
  printf 'error: PLC contract not found: %s\n' "$source_contract" >&2
  exit 1
fi

python3 -c '
import json, pathlib, sys
document = json.loads(pathlib.Path(sys.argv[1]).read_text())
if document.get("schema_version") != 1:
    raise SystemExit("error: expected PLC contract schema version 1")
if document.get("generated_by") != "aquarium-controller-plc-mqtt-contract":
    raise SystemExit("error: unexpected PLC contract generator")
source = document.get("source", {})
if source.get("file") != "aquarium_controller.adpro" or len(source.get("sha256", "")) != 64:
    raise SystemExit("error: invalid PLC contract provenance")
' "$source_contract"

if [[ "$mode" == "check" ]]; then
  if ! cmp -s "$source_contract" "$destination"; then
    printf 'error: vendored PLC contract is out of date\n' >&2
    exit 1
  fi
  printf 'vendored PLC contract is current\n'
else
  install -m 0644 "$source_contract" "$destination"
  printf 'updated %s\n' "$destination"
fi
