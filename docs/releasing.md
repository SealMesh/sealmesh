# Releasing

**One version tag releases everything.** Pushing `vX.Y.Z` runs
`.github/workflows/release.yml`, generated (with two hand edits, below) by
[cargo-dist](https://opensource.axo.dev/cargo-dist/) from `dist-workspace.toml`:

| Channel | What ships | Job |
|---|---|---|
| GitHub Releases | `sealmesh`, `sealmesh-mcp` and `sealmesh-server` for macOS (arm64, x86_64) and Linux (arm64, x86_64), shell installers, checksums | dist `build-*` and `host` |
| Homebrew | a formula per program in `allocmemory/homebrew-tap` (`brew install allocmemory/tap/sealmesh`) | dist `publish-homebrew-formula` |
| ghcr.io | `ghcr.io/allocmemory/sealmesh-server:<version>` and `:latest`, plus `:<version>-dynamodb` (built with the `dynamodb` feature, for [deploy/aws](../deploy/aws/README.md)) | `publish-image.yml` |
| crates.io | every workspace crate, in dependency order | `publish-crates.yml` |

A pull request runs `dist plan` only; nothing is built or published.

`release.yml` is generated, plus two hand edits dist can't express. The token
is read-only by default (`permissions: contents: read` at the top) and only
the `host` job, which creates the GitHub Release and uploads to it, gets
`contents: write`. Both edits are marked `HAND EDIT` in the file.
`allow-dirty = ["ci"]` in `dist-workspace.toml` tells dist to accept the
difference instead of failing `dist plan`; it also stops `dist generate` from
touching the file.

To regenerate (a dist upgrade, a new target, an action bump), temporarily drop
`"ci"` from `allow-dirty`, run `dist generate`, re-apply the two `HAND EDIT`
blocks, then restore `allow-dirty`. Check the diff: anything else that
changed came from dist. Keep `[dist.github-custom-job-permissions]` matching
the `permissions:` of `publish-crates.yml` and `publish-image.yml`.

A Dependabot pull request that bumps an action inside `release.yml` is applied
through `dist-workspace.toml` (`[dist.github-action-commits]`) and the
regenerate steps above, and the Dependabot PR is closed.

## Cutting a release

1. Update `version` in `[workspace.package]` and the `version` of each
   workspace dependency in the root `Cargo.toml`, then `cargo build` to refresh
   `Cargo.lock`.
2. Add the version's section to `docs/CHANGELOG.md`.
3. Land steps 1–2 through a pull request (main only takes PRs), then tag the
   merge commit and push the tag: `git tag vX.Y.Z && git push origin vX.Y.Z`.
4. Once the release workflow finishes, install it the way a user would:
   `gh workflow run install-smoke.yml -f version=X.Y.Z`. It runs
   `brew install allocmemory/tap/sealmesh allocmemory/tap/sealmesh-mcp` on a
   clean macOS runner and both router images (`X.Y.Z`, `X.Y.Z-dynamodb`) on
   Linux, and checks every binary reports X.Y.Z.

### Before the first release

- Create the tap repository `allocmemory/homebrew-tap`.
- Add the repository secrets `HOMEBREW_TAP_TOKEN` (a token that can push to the
  tap) and `CARGO_REGISTRY_TOKEN` (a crates.io token with publish scope).
- After the first image push, make the `sealmesh-server` package on ghcr.io
  public.

### Checking it locally

```sh
dist plan                                   # what one tag would build and publish
cargo publish --workspace --dry-run --locked
docker build -t sealmesh-server:local .
```

## When a release is bad

**A fix is always a new version.** A version number is never reused: crates.io
refuses it even after a yank, and reusing it anywhere else would make two
different builds answer to one name.

1. Ship the fix as the next patch version (`vX.Y.Z+1`).
2. Pull the bad one back, channel by channel:

| Channel | Undo |
|---|---|
| crates.io | `cargo yank --version X.Y.Z <crate>` for each crate. Existing lockfiles keep working; new resolves skip it. |
| GitHub Releases | Mark the release as a pre-release (or delete it) so `latest` points at the good one. |
| ghcr.io | Point `latest` back at the previous version: `docker buildx imagetools create -t ghcr.io/allocmemory/sealmesh-server:latest ghcr.io/allocmemory/sealmesh-server:<previous>`. |
| Homebrew | Revert the formula commit in `allocmemory/homebrew-tap`. |

### A release that stopped halfway

The publish jobs run independently once the artifacts are hosted, so a failure
can leave some channels published and others not. Reconcile, then finish the
release:

- **crates.io stopped partway** (most often HTTP 429: crates.io rate-limits
  brand-new crate names). Wait out the time in the error, then run
  `publish-crates.yml` by hand with the release tag:
  `gh workflow run publish-crates.yml -f tag=vX.Y.Z`. It skips every crate
  whose version is already on crates.io and publishes the rest. Rerunning the
  failed job from the release run does not do this, because a rerun uses the
  workflow as it was at the tag.
- **Any other channel:** rerun the failed job from the workflow run.
- **The released code itself is wrong:** fix forward with the next patch
  version.
