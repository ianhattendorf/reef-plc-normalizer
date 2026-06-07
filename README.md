# Home Assistant Apps

This repository is a dedicated Home Assistant App repository layout.

Add this repository to Home Assistant as a custom App repository, or copy
`reef_plc_normalizer/` to `/addons/reef_plc_normalizer` on the Home Assistant OS VM
for local app installation.

## Release

Run releases from a clean `main` branch:

```sh
scripts/release.sh patch
```

Add user-visible changes to `reef_plc_normalizer/CHANGELOG.md` under
`## Unreleased` before releasing. The release script updates version metadata,
promotes the unreleased changelog notes into the new version, runs the release
checks and tests, commits, creates the matching `vX.Y.Z` tag, and pushes both
`main` and the tag. The pushed tag triggers the GitHub Actions image publish
workflow.
