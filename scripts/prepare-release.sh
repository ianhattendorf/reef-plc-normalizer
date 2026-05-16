#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
app_dir="$repo_root/reef_plc_normalizer"
cargo_toml="$app_dir/app/Cargo.toml"
config_yaml="$app_dir/config.yaml"
changelog="$app_dir/CHANGELOG.md"

usage() {
  cat >&2 <<'EOF'
usage: scripts/prepare-release.sh VERSION_OR_BUMP

VERSION_OR_BUMP may be:
  X.Y.Z   explicit stable SemVer version
  patch   increment current X.Y.Z to X.Y.(Z+1)
  minor   increment current X.Y.Z to X.(Y+1).0
  major   increment current X.Y.Z to (X+1).0.0
EOF
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  fail "working tree must be clean before preparing a release"
fi

"$repo_root/scripts/check-release.sh"

current_version="$(
  sed -nE 's/^version:[[:space:]]*"?([0-9]+)\.([0-9]+)\.([0-9]+)"?[[:space:]]*$/\1.\2.\3/p' "$config_yaml" | head -n1
)"

if [[ ! "$current_version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  fail "could not read current SemVer version from $config_yaml"
fi

major="${BASH_REMATCH[1]}"
minor="${BASH_REMATCH[2]}"
patch="${BASH_REMATCH[3]}"
requested="$1"

case "$requested" in
  patch)
    target_version="$major.$minor.$((patch + 1))"
    ;;
  minor)
    target_version="$major.$((minor + 1)).0"
    ;;
  major)
    target_version="$((major + 1)).0.0"
    ;;
  *)
    if [[ ! "$requested" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      usage
      fail "invalid version or bump: $requested"
    fi
    target_version="$requested"
    ;;
esac

if [[ "$target_version" == "$current_version" ]]; then
  fail "target version is already current: $target_version"
fi

sed -i -E "s/^version:[[:space:]]*\"?[0-9]+\.[0-9]+\.[0-9]+\"?[[:space:]]*$/version: \"$target_version\"/" "$config_yaml"

cargo_tmp="$(mktemp)"
awk -v version="$target_version" '
  ! updated && /^version = "[0-9]+\.[0-9]+\.[0-9]+"/ {
    print "version = \"" version "\""
    updated = 1
    next
  }
  { print }
' "$cargo_toml" > "$cargo_tmp"
mv "$cargo_tmp" "$cargo_toml"

if ! grep -Fqx "## $target_version" "$changelog"; then
  changelog_tmp="$(mktemp)"
  awk -v version="$target_version" '
    NR == 1 {
      print
      print ""
      print "## " version
      print ""
      print "- TODO"
      inserted = 1
      next
    }
    inserted && NR == 2 && $0 == "" { next }
    { print }
  ' "$changelog" > "$changelog_tmp"
  mv "$changelog_tmp" "$changelog"
fi

cargo generate-lockfile --manifest-path "$cargo_toml"

"$repo_root/scripts/check-release.sh"
cargo fmt --manifest-path "$cargo_toml" -- --check
cargo test --manifest-path "$cargo_toml" --locked

printf 'prepared release %s\n' "$target_version"
