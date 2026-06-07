#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
app_dir="$repo_root/reef_plc_normalizer"
cargo_toml="$app_dir/app/Cargo.toml"
config_yaml="$app_dir/config.yaml"
changelog="$app_dir/CHANGELOG.md"
remote="origin"
branch="main"
push=1
version_or_bump=""

usage() {
  cat >&2 <<'EOF'
usage: scripts/release.sh VERSION_OR_BUMP [OPTIONS]

VERSION_OR_BUMP may be:
  X.Y.Z   explicit stable SemVer version
  patch   increment current X.Y.Z to X.Y.(Z+1)
  minor   increment current X.Y.Z to X.(Y+1).0
  major   increment current X.Y.Z to (X+1).0.0

Options:
  --no-push         Commit and tag locally, but do not push.
  --remote NAME     Git remote to push to. Defaults to origin.
  --branch NAME     Release branch. Defaults to main.
  -h, --help        Show this help.
EOF
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-push)
      push=0
      shift
      ;;
    --remote)
      [[ $# -ge 2 ]] || fail "--remote requires a value"
      remote="$2"
      shift 2
      ;;
    --branch)
      [[ $# -ge 2 ]] || fail "--branch requires a value"
      branch="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      usage
      fail "unknown option: $1"
      ;;
    *)
      if [[ -n "$version_or_bump" ]]; then
        usage
        fail "multiple versions or bumps provided: $version_or_bump and $1"
      fi
      version_or_bump="$1"
      shift
      ;;
  esac
done

if [[ -z "$version_or_bump" ]]; then
  usage
  fail "VERSION_OR_BUMP is required"
fi

if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  fail "working tree must be clean before releasing"
fi

current_branch="$(git -C "$repo_root" branch --show-current)"
if [[ "$current_branch" != "$branch" ]]; then
  fail "release must be run from branch $branch, currently on ${current_branch:-detached HEAD}"
fi

"$repo_root/scripts/check-release.sh"

if ! git -C "$repo_root" fetch --quiet "$remote" "$branch"; then
  fail "failed to fetch $remote/$branch"
fi

remote_head="$(git -C "$repo_root" rev-parse FETCH_HEAD)"
if ! git -C "$repo_root" merge-base --is-ancestor "$remote_head" HEAD; then
  fail "local $branch does not contain $remote/$branch"
fi

current_version="$(
  sed -nE 's/^version:[[:space:]]*"?([0-9]+)\.([0-9]+)\.([0-9]+)"?[[:space:]]*$/\1.\2.\3/p' "$config_yaml" | head -n1
)"

if [[ ! "$current_version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  fail "could not read current SemVer version from $config_yaml"
fi

major="${BASH_REMATCH[1]}"
minor="${BASH_REMATCH[2]}"
patch="${BASH_REMATCH[3]}"

case "$version_or_bump" in
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
    if [[ ! "$version_or_bump" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      usage
      fail "invalid version or bump: $version_or_bump"
    fi
    target_version="$version_or_bump"
    ;;
esac

if [[ "$target_version" == "$current_version" ]]; then
  fail "target version is already current: $target_version"
fi

tag_name="v$target_version"

if git -C "$repo_root" rev-parse -q --verify "refs/tags/$tag_name" >/dev/null; then
  fail "local tag already exists: $tag_name"
fi

if git -C "$repo_root" ls-remote --exit-code --tags "$remote" "$tag_name" >/dev/null 2>&1; then
  fail "remote tag already exists on $remote: $tag_name"
fi

if ! awk '
  $0 == "## Unreleased" {
    found = 1
    in_unreleased = 1
    next
  }
  in_unreleased && /^## / {
    in_unreleased = 0
  }
  in_unreleased && $0 !~ /^[[:space:]]*$/ {
    has_content = 1
  }
  END {
    exit(found && has_content ? 0 : 1)
  }
' "$changelog"; then
  fail "$changelog must contain a non-empty ## Unreleased section before releasing"
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

changelog_tmp="$(mktemp)"
awk -v version="$target_version" '
  $0 == "## Unreleased" {
    print "## Unreleased"
    print ""
    print "## " version
    print ""
    in_unreleased = 1
    seen_content = 0
    last_blank = 0
    next
  }
  in_unreleased && /^## / {
    if (!last_blank) {
      print ""
    }
    print
    in_unreleased = 0
    next
  }
  in_unreleased {
    if (!seen_content && $0 ~ /^[[:space:]]*$/) {
      next
    }
    print
    seen_content = 1
    last_blank = ($0 ~ /^[[:space:]]*$/)
    next
  }
  { print }
' "$changelog" > "$changelog_tmp"
mv "$changelog_tmp" "$changelog"

cargo generate-lockfile --manifest-path "$cargo_toml"

"$repo_root/scripts/check-release.sh"
cargo fmt --manifest-path "$cargo_toml" -- --check
cargo test --manifest-path "$cargo_toml" --locked

git -C "$repo_root" add "$config_yaml" "$cargo_toml" "$app_dir/app/Cargo.lock" "$changelog"
git -C "$repo_root" commit -m "Release reef PLC normalizer $target_version"
git -C "$repo_root" tag -a "$tag_name" -m "Release reef PLC normalizer $target_version"

if [[ "$push" -eq 1 ]]; then
  git -C "$repo_root" push "$remote" "$branch"
  if ! git -C "$repo_root" push "$remote" "$tag_name"; then
    fail "failed to push $tag_name; retry with: git push $remote $tag_name"
  fi
  printf 'released %s and pushed %s/%s plus tag %s\n' "$target_version" "$remote" "$branch" "$tag_name"
else
  printf 'prepared local release %s with tag %s; push with:\n' "$target_version" "$tag_name"
  printf '  git push %s %s\n' "$remote" "$branch"
  printf '  git push %s %s\n' "$remote" "$tag_name"
fi
