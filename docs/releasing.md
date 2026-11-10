# Release checklist

[Back to the README](../README.md) · [API reference](reference.md) · [Changelog](../CHANGELOG.md)

This checklist covers preparation and verification of the **2.0.0** npm archive. Publishing, pushing, and tagging are separate release actions.

## Prepare the release

- Confirm `package.json` and the root package metadata in `package-lock.json` agree on `2.0.0`.
- Review the [changelog](../CHANGELOG.md), public declarations, and [query-list filtering contract](reference.md#query-list-filtering). Verify omitted/`undefined` filters, exact contract types and unspecified (`null`) types, single and readonly-array statuses (including empty arrays), trimmed case-insensitive literal search over every supported field, and invalid operational-filter inputs. Combine these with network/query kind in all order modes; check filtered counts, empty results, and page reclamping.
- Verify query-list status matching agrees with returned effective statuses during read-only, processing-only, and both-side global holds, including active work settling through `PAUSING`. Preserve independently paused/failed sides; failure-threshold pauses must match `PAUSED`, not `FAILED`. Confirm filtering performs no loader or Mirror Node calls and leaves synchronization behavior unchanged.
- Preserve the optional pre-decode `params.logFilter` behavior and its tests: `false` ignores a log, `true` continues strict decoding, and a thrown error retries the page.
- Verify optional transaction `params.transactionType`: omitted/`undefined` values send no type filter, an explicit value reaches the Mirror Node request, and account/timestamp/result filters remain independent with the existing success default. Cover whitespace trimming, case preservation, future/provider string names, invalid shapes/blank strings, restart/reload source identity (including adding/removing the filter), and unchanged local skip/missing-handler behavior. Check built consumer declarations accept scalar names and reject non-string values.
- Review the [v2 migration guide](migration-v2.md) and [lifecycle guide](lifecycle.md). Confirm `querySource` is required in JavaScript and declarations, `create` does not load/start work, and public add/update/remove methods are absent.
- Verify loader failure/invalid-catalog preflight, duplicate IDs, source identity protection, static/dynamic/empty catalogs, independent global/per-query holds, processed-checkpoint carry-forward, persistence rejection, and blocked callbacks/timeouts with no overlapping generations.
- Verify sparse loader arrays and explicit `undefined` entries reject during preflight on startup and reload, preserving existing global holds, catalog, queues, and checkpoints. Confirm a valid retry and intentional empty catalog still work.
- Verify guarded `create()` calls made before and after a shutdown deadline wait for the underlying work, share one replacement after disposal, and receive the retained instance only after an aborted shutdown settles. Check that `CompositeHooks.onFailure` contains both synchronous throws and delayed promise rejections without waiting for a callback that never settles.
- Verify `pauseAll`/`resumeAll` scopes (`"read"`, `"process"`, and default/explicit `"both"`), independent hold composition, read-only cancellation with queue draining, processing-only settlement with continued ingestion/backpressure, and both gates installed before waiting. Check scoped hold restoration across successful restart/reload, inheritance by new queries, and the both-side recovery hold plus all-scope partial-replacement resume guard on failure.
- Check `globalReadPaused`, `globalProcessPaused`, `maintenanceHeld`, and active pause/resume `operation.scope`; `globalPaused` must be true only when both effective global holds are active. Consumer declarations must export `GlobalPauseScope` and `GlobalPauseOptions` and reject invalid scopes.
- Verify side-effect-free `validateQuery`, new-query `initialState` without transient reads/handlers, pause/failure counter preservation, and final checkpoint persistence. Include query-edit lifecycle, coordinator, and built consumer declaration tests.
- Keep the original coverage gates; do not lower thresholds for the new coordination paths.
- Review the working diff and ensure the npm `files` allowlist includes `dist`, `docs`, `README.md`, `CHANGELOG.md`, and `LICENSE`.

## Run the package gates

Run from the repository root with the lockfile's dependencies installed (`npm ci` for a fresh checkout):

```powershell
npm run prepack
```

The `prepack` script runs these gates in order and stops on failure:

1. `npm run typecheck`: source and test TypeScript checks.
2. `npm run test:coverage`: automated tests with coverage thresholds of 80% lines/statements, 75% functions, and 65% branches.
3. `npm run build`: rebuild the distributable JavaScript and declarations.
4. `npm run test:package`: runtime export smoke and consumer declaration checks against the built package.

Record the successful command and results for the exact release tree. The public-Mirror `test:e2e` script is a separate integration check and is not included in `prepack`.

## Pack and inspect locally

After a successful `prepack`, use a temporary directory for the archive. `--ignore-scripts` avoids repeating those already completed gates; rerun the gates first if the source, tests, dependencies, or package metadata change. Repack after any documentation change as well: the archive includes the Markdown files, so an earlier tarball does not contain later corrections.

```powershell
$repoDir = (Get-Location).Path
$releaseDir = Join-Path ([System.IO.Path]::GetTempPath()) ("hedera-event-sync-2.0.0-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $releaseDir | Out-Null
$packJson = npm pack --ignore-scripts --json --pack-destination $releaseDir
if ($LASTEXITCODE -ne 0) { throw "npm pack failed" }
$packJson | Set-Content -LiteralPath (Join-Path $releaseDir 'pack.json')
$pack = @($packJson | ConvertFrom-Json)[0]
$tarball = Join-Path $releaseDir $pack.filename
$pack | Select-Object name, version, filename, size, unpackedSize, shasum, integrity
$pack.files | Select-Object path, size
tar -tzf $tarball
tar -xOf $tarball package/package.json
```

Verify the archive identifies `@davincigraph/hedera-event-sync@2.0.0`, includes each documented export and its `.d.ts` file under `dist`, and contains the README, license, changelog, and documentation. Inspect the archive's declarations for the required `querySource`, lifecycle types (including `GlobalPauseScope`, `GlobalPauseOptions`, effective per-side holds, and optional operation scope), `beforeReplace`, and `initialState`, plus the `readStatus`, `processStatus`, and `search` query-list options, optional transaction `params.transactionType`, and retained `contractType` and `logFilter` APIs. Confirm removed mutation methods are absent. Review the complete file list for unintended credentials, local configuration, temporary artifacts, or unrelated files. Keep the archive and the `pack.json` integrity metadata with the local validation results.

## Smoke-test an installed archive

Install the tarball in a separate temporary consumer directory so imports resolve to the packed files. The following reuses the repository's runtime and declaration smoke fixtures; it does not link the repository into the consumer. Dependency installation may require registry access.

```powershell
$consumerDir = Join-Path $releaseDir 'consumer'
New-Item -ItemType Directory -Path $consumerDir | Out-Null
Copy-Item -LiteralPath (Join-Path $repoDir 'test/package-smoke.mjs') -Destination $consumerDir
Copy-Item -LiteralPath (Join-Path $repoDir 'test/package-types.mts') -Destination $consumerDir
Push-Location $consumerDir
try {
  npm init -y
  if ($LASTEXITCODE -ne 0) { throw "Consumer initialization failed" }
  npm install --ignore-scripts $tarball
  if ($LASTEXITCODE -ne 0) { throw "Tarball installation failed" }
  npm install --ignore-scripts --save-dev typescript@5.4.5 @types/node@18
  if ($LASTEXITCODE -ne 0) { throw "Consumer typecheck dependencies failed" }
  node .\package-smoke.mjs
  if ($LASTEXITCODE -ne 0) { throw "Consumer runtime smoke failed" }
  npm exec --yes --package=node@18.20.8 -- node .\package-smoke.mjs
  if ($LASTEXITCODE -ne 0) { throw "Node 18 consumer runtime smoke failed" }
  npx --no-install tsc --strict --exactOptionalPropertyTypes --target ES2022 --module NodeNext --moduleResolution NodeNext --types node --noEmit .\package-types.mts
  if ($LASTEXITCODE -ne 0) { throw "Consumer declaration smoke failed" }
} finally {
  Pop-Location
}
```

This checks both the current Node runtime and the supported Node 18 line, with the minimum documented TypeScript minor version (5.4). The extra Node runtime may need to be downloaded from npm; it does not replace the system installation.

Confirm the installed runtime fixture exercises construction without auto-start, explicit start, scoped and default global pause/resume, effective lifecycle fields, restart without loading, reload, shutdown, removed API absence, and operational query-list filters/search. The declaration fixture must accept every global scope and reject invalid scopes, expose the new lifecycle fields and optional operation scope, require a complete loader, type its context/persistence boundary and new-query state, reject the removed mutation methods, accept status names/readonly arrays/search/explicit `undefined`, reject invalid filter types, and preserve existing filter contracts with `exactOptionalPropertyTypes`. Record the tarball path, checksum/integrity, gate results, and installed-consumer results. Keep the changelog marked unreleased during preparation; update its status and date when finalizing the release, then pack and inspect the final archive again.
