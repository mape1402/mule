# Release Automation

Mule releases are requested with the `.release` marker file.

## Request a release

1. Update `CHANGELOG.md` with a section for the tag:

   ```markdown
   ## [v1.0.0] - 2026-08-08
   ```

2. Add or update `.release` with exactly one non-empty line:

   ```text
   v1.0.0
   ```

3. Open a pull request to `main`.
4. Merge the pull request after approval and a passing build.

After the merge, the `Build and Release` workflow restores, builds, and tests the merged `main` commit first. Only after that succeeds does it check whether `.release` changed in that push. If it changed, the workflow validates the tag, validates the changelog entry, creates `releases/v1.0.0` as a release marker branch, packs the NuGet packages, creates the GitHub release, and publishes to NuGet.

## Manual release

The same `Build and Release` workflow can be run manually from a `releases/v*.*.*` branch. Manual releases derive the package version from the selected branch name instead of `.release`.

## Repository setup

The repository must define these settings before automatic releases can run:

- `NUGET_USER` repository variable with the nuget.org username configured for Trusted Publishing.

The NuGet Trusted Publishing policy should point to `build-and-release.yml`, because it is the only workflow that publishes packages.
