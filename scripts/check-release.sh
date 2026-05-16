#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
app_dir="$repo_root/reef_plc_normalizer"
cargo_toml="$app_dir/app/Cargo.toml"
cargo_lock="$app_dir/app/Cargo.lock"
config_yaml="$app_dir/config.yaml"
changelog="$app_dir/CHANGELOG.md"

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

config_version="$(
  sed -nE 's/^version:[[:space:]]*"?([0-9]+\.[0-9]+\.[0-9]+)"?[[:space:]]*$/\1/p' "$config_yaml" | head -n1
)"

if [[ -z "$config_version" ]]; then
  fail "could not read SemVer version from $config_yaml"
fi

cargo_version="$(
  sed -nE 's/^version = "([0-9]+\.[0-9]+\.[0-9]+)"$/\1/p' "$cargo_toml" | head -n1
)"

if [[ "$cargo_version" != "$config_version" ]]; then
  fail "$cargo_toml version $cargo_version does not match $config_yaml version $config_version"
fi

lock_version="$(
  awk '
    $0 == "[[package]]" { in_package = 1; name = ""; version = ""; next }
    in_package && /^name = / { name = $3; gsub(/"/, "", name) }
    in_package && /^version = / { version = $3; gsub(/"/, "", version) }
    in_package && name == "reef-plc-normalizer" && version != "" { print version; exit }
  ' "$cargo_lock"
)"

if [[ "$lock_version" != "$config_version" ]]; then
  fail "$cargo_lock version $lock_version does not match $config_yaml version $config_version"
fi

if ! grep -Fqx "## $config_version" "$changelog"; then
  fail "$changelog is missing heading: ## $config_version"
fi

tag_name=""
if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  tag_name="${GITHUB_REF_NAME:-}"
fi

if [[ -n "$tag_name" ]]; then
  if [[ ! "$tag_name" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    fail "release tag $tag_name is not a stable SemVer tag like v$config_version"
  fi

  if [[ "$tag_name" != "v$config_version" ]]; then
    fail "release tag $tag_name does not match $config_yaml version $config_version"
  fi
fi

printf 'release metadata is consistent for %s\n' "$config_version"
