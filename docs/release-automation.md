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

After the merge, the `Build` workflow checks whether `.release` changed in that push. If it changed, the workflow validates the tag, validates the changelog entry, and creates `releases/v1.0.0`.

Creating the release branch triggers `Release to NuGet`, which builds, tests, packs, creates the GitHub release, and publishes the NuGet packages.

## Repository setup

The repository must define these settings before automatic releases can run:

- `RELEASE_BOT_TOKEN` secret with permission to push branches and trigger workflows.
- `NUGET_USER` repository variable with the nuget.org username configured for Trusted Publishing.

The release workflow still supports manual `workflow_dispatch` runs from a `releases/v*.*.*` branch.
