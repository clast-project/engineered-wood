# Releasing

Pushing a `vX.Y.Z` tag runs [`release.yml`](../.github/workflows/release.yml). The workflow builds and tests
on Windows, packs all 15 packages at the tag's version, pushes them to nuget.org and creates the GitHub
Release. Nothing is pushed if an earlier step fails.

## Before tagging

1. **Check the API against the previous release.** The release fails if a package breaks compatibility
   with the previous release in a way its `CompatibilitySuppressions.xml` doesn't list. To see what has
   changed, and to accept the changes, run this with the previous release's version:

   ```
   dotnet pack engineered-wood.slnx -c Release -o ../ew-apicheck -p:ApiBaselineVersion=0.4.0 -p:ApiCompatGenerateSuppressionFile=true
   ```

   This rewrites `src/*/CompatibilitySuppressions.xml` to list every difference, and drops entries that no
   longer match anything. Review the diff. A new entry whose `<Left>` names the baseline package is a
   breaking change, so describe it in `PackageReleaseNotes` (`src/Directory.Build.props`), then commit the
   files with the notes.

   Entries whose `<Left>` and `<Right>` are two frameworks of the same new package (`lib/netstandard2.0/…`
   against `lib/net8.0/…`) compare the package with itself, not with the previous release. They don't
   belong in the release notes. Records produce some permanently: the two frameworks compile a record's
   `<Clone>$` differently (`CP0005`).

   A package that is new since the previous release has no baseline to compare against. Add
   `<PackageValidationBaselineVersion />` to its csproj for that release.
2. **Update the release notes.** Edit `PackageReleaseNotes` and the status note at the top of
   `README.md`, which nuget.org shows as each package's page.
3. **Wait for CI to pass on `main`.**

## Tagging

```
git tag -a vX.Y.Z origin/main -m "EngineeredWood X.Y.Z"
git push origin vX.Y.Z
```

The tag sets every package's version, overriding `<Version>` in `src/Directory.Build.props`.

## Publishing

nuget.org accepts the push through [trusted publishing](https://learn.microsoft.com/nuget/nuget-org/trusted-publishing).
The `engineered-wood` policy on nuget.org trusts `release.yml` in this repository, and `NuGet/login`
exchanges the workflow's OIDC token for an API key that lasts about an hour. No API key is stored in the
repository. New versions take a few minutes to appear on nuget.org after the push.

Entries left over from the previous baseline stop matching after a release. They are ignored, and the next
regeneration removes them.
