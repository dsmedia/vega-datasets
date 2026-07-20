# Phase 0 implementation plan: gallery examples federation

**Status:** implementation proposal
**Authoritative architecture:** `design/gallery-examples-federation.md`
**Source implementation reviewed:** `origin/feat/rewrite-gallery-examples`, especially
`scripts/generate_gallery_examples.py` and
`tests/test_generate_gallery_examples.py`

This document turns Phase 0 of the federation design into a reviewable
vega-datasets implementation plan. It deliberately does not require changes in
Vega, Vega-Lite, Altair, or the Vega Editor.

## Recommendation in one page

1. Keep `scripts/generate_gallery_examples.py` as a small command-line wrapper,
   and move implementation into an importable `scripts/gallery_examples/`
   package.
2. Make each configured adapter return the same gallery-index document. Start
   with three `legacy-scrape` adapters and include the small `manifest` adapter
   in Phase 0 so later migrations are config-only changes plus deletions.
3. Treat `schemas/gallery-index.schema.json` as the one manifest contract.
   Validate every adapter result with JSON Schema Draft 2020-12 before any
   vega-datasets-specific transformation.
4. Derive `example.id` from the last path segment of `pageUrl`/`example_url`.
   Use `(gallery_name, example_id)` as the aggregate key. Keep `spec_url`
   unique, but do not use it as identity.
5. Preserve the robust parts of PR #776: dataset-name mapping, first-party URL
   hard failures, declarative Vega/Vega-Lite URL extraction, bounded async
   fetching, and most of their unit tests.
6. Confine presentation-shape parsing and Altair source regexes to legacy
   adapter modules. Those modules are explicitly disposable.
7. Write the aggregate with a dedicated canonical serializer. Do not rely on
   dictionary insertion order, platform newline conversion, source index order,
   or wall-clock time.
8. Keep ordinary CI offline. It validates the schema, fixtures, pure functions,
   adapter contracts, the committed aggregate, and golden output bytes. Only a
   weekly/manual workflow contacts gallery repositories.
9. Let that workflow update one dedicated branch and one PR. Large removals are
   a reviewable diff, not a count-floor failure. Aggregation errors open or
   update one deduplicated issue.

## Phase 0 data flow

```text
_data/gallery_examples.toml
          |
          v
load typed config + datapackage namespace
          |
          v
run adapters concurrently
  legacy-scrape: Vega | Vega-Lite | Altair
  manifest: fetch and parse a published gallery index (unused initially)
          |
          v
validate every adapter result against local gallery-index.schema.json
          |
          v
semantic manifest checks
  configured gallery matches; id == page URL slug; local ids/spec URLs unique
          |
          v
shared data backfill
  Vega/Vega-Lite only when data[] is absent; Altair legacy adapter supplies it
          |
          v
canonicalize vega-datasets URLs through datapackage.json
          |
          v
merge + cross-gallery invariants
          |
          v
canonical records + UTF-8/LF serializer
          |
          v
data/gallery-examples.json
```

The key seam is the first validation step: downstream code must not know whether
a gallery-index document came from a live manifest or a legacy scraper.

## Proposed file layout

```text
schemas/
  gallery-index.schema.json                 # authoritative manifest contract
scripts/
  generate_gallery_examples.py              # CLI compatibility wrapper
  summarize_gallery_examples.py             # old/new diff -> Markdown/JSON
  gallery_examples/
    __init__.py
    config.py                                # TOML parsing and typed config
    http.py                                  # Fetcher protocol + niquests client
    identity.py                              # page URL -> stable id
    validation.py                            # schema + semantic validation
    datasets.py                              # datapackage name map/canonicalizer
    specs.py                                 # Vega/Vega-Lite data URL extraction
    models.py                                # TypedDicts/dataclasses at seams
    pipeline.py                              # orchestration and invariants
    serialize.py                             # canonical aggregate records/bytes
    adapters/
      __init__.py                            # registry for adapter/name pairs
      manifest.py                            # future published-manifest reader
      legacy_vega.py                         # disposable presentation parser
      legacy_vega_lite.py                    # disposable presentation parser
      legacy_altair.py                       # disposable source parser/regexes
tests/
  gallery_examples/
    fixtures/
      datapackage.json
      manifest-valid.json
      manifest-invalid-*.json
      vega-index.json
      vega-lite-index.json
      altair-directory.json
      specs/...
      expected-gallery-examples.json
    test_config.py
    test_identity.py
    test_validation.py
    test_datasets.py
    test_specs.py
    test_legacy_adapters.py
    test_pipeline.py
    test_serialize.py
    test_summary.py
_data/
  gallery_examples.toml
.github/workflows/
  refresh-gallery-examples.yml              # installed after scripts land
```

### Why a package rather than one script

Keeping one script has a smaller initial diff, but it preserves the current
problem: gallery parsing, network transport, canonicalization, invariants, and
serialization share mutable dictionaries and cannot be tested at their real
boundaries. Three scripts plus shared helpers has the opposite problem: callers
can bypass the common pipeline.

An importable package with one CLI wrapper is the recommended middle. The
adapter interface is narrow, tests can inject an in-memory fetcher, and the
existing `uv run scripts/generate_gallery_examples.py` command remains valid.
The wrapper imports the sibling package as `gallery_examples`; add `scripts` to
pytest's `pythonpath` so tests and commands resolve the same module identity.

### Models: JSON Schema plus TypedDict, not Pydantic

The checked-in JSON Schema must remain the public source of truth. Generating it
from Pydantic would make the external contract sensitive to library upgrades;
maintaining both hand-written Pydantic and JSON Schema models would duplicate
the contract. Validate untrusted JSON with `jsonschema`, then use `TypedDict`
for editor/type-checker help and small frozen dataclasses for internal values
such as `RawDataReference`. This adds only one explicit dependency:
`jsonschema[format-nongpl]>=4.23,<5`.

## Configuration

Replace the untyped `[sources]` table with one table per gallery. Keep
`adapter = "legacy-scrape"` exactly as specified by the design; the adapter
registry dispatches on `(adapter, gallery.name)`.

```toml
schema_path = "schemas/gallery-index.schema.json"
output_path = "data/gallery-examples.json"

[galleries.vega]
name = "vega"
adapter = "legacy-scrape"
repository = "vega/vega"
ref = "main"
gallery_url = "https://vega.github.io/vega/examples/"
index_path = "docs/_data/examples.json"
spec_path_template = "docs/examples/{id}.vg.json"

[galleries.vega_lite]
name = "vega-lite"
adapter = "legacy-scrape"
repository = "vega/vega-lite"
ref = "main"
gallery_url = "https://vega.github.io/vega-lite/examples/"
index_path = "site/_data/examples.json"
spec_path_template = "examples/specs/{source_slug}.vl.json"

[galleries.altair]
name = "altair"
adapter = "legacy-scrape"
repository = "vega/altair"
ref = "main"
gallery_url = "https://altair-viz.github.io/gallery/"
examples_path = "tests/examples_methods_syntax"
```

A later migration is local and atomic:

```toml
[galleries.altair]
name = "altair"
adapter = "manifest"
url = "https://altair-viz.github.io/gallery-index.json"
require_data = true
```

`require_data = true` distinguishes an intentionally empty `data: []` from an
omitted `data` facet. The gallery-index schema keeps the facet optional for
general consumers, while the vega-datasets adapter policy may require it for
Altair because there is no reliable downstream backfill for Python source.

The legacy adapter resolves `ref` to a full commit SHA once per gallery through
the GitHub commits API and uses that SHA as internal `gallery.sourceRevision`.
Raw content URLs may remain branch-shaped in the distributed aggregate to avoid
rewriting every record on unrelated upstream commits. Static consumers that
need immutable assets should consume a published upstream manifest and pin to
its `sourceRevision`, as described in the federation design.

No token is required for a local run. The scheduled job may pass a token for
rate-limit headroom, but the code must work against public endpoints without
one and must never require credentials from another repository.

## Gallery-index schema decisions

The actual proposal is `schemas/gallery-index.schema.json`; a conforming
committed-mode example is `design/fixtures/gallery-index.example.json`.
Add `schemas` to the npm `files` list and verify it with `npm pack --dry-run` so
the contract ships beside the data artifact even before its canonical URL is
published in Phase 1.

### Chosen draft and identifier

- Use JSON Schema Draft 2020-12.
- Reserve `https://vega.github.io/schema/gallery-index/v1.json` as `$id` now.
  Phase 0 validators load the checked-in path directly; `$id` is an identifier,
  not a network dependency. Publishing that URL is Phase 1.
- Require an instance `$schema` property with that exact value and
  `manifestVersion: "1.0"`.

### Required and optional fields

- Require `gallery.name`, `gallery.url`, and immutable
  `gallery.sourceRevision`.
- Keep `gallery.libraryVersion` optional because legacy inventories do not
  expose it reliably. Upstream emitters should include it when known.
- Keep `gallery.generatedAt` optional. Committed indexes omit it unless the
  value comes from source control; site-published copies may use a timestamp.
- Require the intersection every gallery already has on each example: `id`,
  `title`, `categories`, `pageUrl`, `specUrl`, and `specFormat`.
- Keep `description`, `thumbnailUrl`, `data`, and `display` optional.
  `thumbnailUrl` is first-class but best-effort; requiring a guessed Phase 0
  URL would make the legacy adapter less truthful.
- Permit extensions only through `x-`-prefixed fields, except inside `display`,
  which is deliberately schema-opaque and gallery-owned.

### Data roles

Require `role` whenever `data[]` is present, but use an open, URL-safe string
vocabulary. Document `primary`, `lookup`, and `geo` as the recommended values.
An enum is attractive for typo detection but makes the first new legitimate
role a schema-breaking event. Adapter semantic tests catch spelling mistakes
in the three values Phase 0 emits.

Legacy role rules are intentionally modest:

- direct Vega/Vega-Lite `data.url` and signal-selected URLs: `primary`;
- lookup transform sources: `lookup`;
- Altair `alt.topo_feature(data.X.url, ...)`: `geo`;
- other recognized Altair `data.X()` / `data.X.url` uses: `primary`.

Altair's source syntax yields a vega-datasets API name rather than a literal
URL. The legacy Altair adapter receives the read-only `DatasetCatalog`, resolves
that name to the resource's real datapackage path, and emits that path as the
manifest `data[].url`. This vega-datasets-specific accommodation stays inside
the disposable adapter; neither the schema nor the shared canonicalizer learns
an Altair source convention.

Do not guess `geo` solely from a filename or file format. A single example may
contain the same dataset in multiple roles, so the aggregate deduplicates
`(dataset_name, role)` pairs rather than dataset names alone.

### Rules JSON Schema cannot express

`validate_manifest_semantics()` enforces these after schema validation and
reports every violation in one error:

1. `gallery.name` equals the configured gallery.
2. Example IDs are unique within a manifest.
3. `specUrl` values are unique within a manifest.
4. `id == example_id_from_page_url(pageUrl)`.
5. `sourceRevision` is not a known moving ref such as `main`, `master`, or
   `HEAD`; legacy adapters always supply a resolved SHA.
6. Altair manifests configured with `require_data` contain the `data` property
   on every example, including examples whose value is `[]`.

The aggregator separately enforces cross-gallery uniqueness and completeness.

## Stable ID derivation

Use one pure function for legacy records and semantic validation:

```python
def example_id_from_page_url(page_url: str) -> str:
    """Return the decoded final page-path slug, without one `.html` suffix."""
```

Algorithm:

1. Parse with `urllib.parse.urlsplit` and require an `https` URL with a host.
2. Ignore query and fragment; remove trailing `/` characters from the path.
3. Percent-decode the final non-empty path segment exactly once.
4. Remove one case-sensitive `.html` suffix. Do not strip arbitrary extensions.
5. Require `^[A-Za-z0-9][A-Za-z0-9._~-]*$` and a maximum of 128 characters.
6. Preserve case. Identity derivation must not silently normalize or rename a
   gallery route.

Examples:

| Page URL | ID |
| --- | --- |
| `https://vega.github.io/vega-lite/examples/bar.html` | `bar` |
| `https://vega.github.io/vega/examples/bar-chart/` | `bar-chart` |
| `https://altair-viz.github.io/gallery/anscombe_plot.html` | `anscombe_plot` |

This intentionally derives from `example_url` rather than source filenames.
For example, a page slug with hyphens may point at a spec filename containing
underscores; the adapter carries both facts explicitly.

## Aggregate record and deterministic output

The distributed file remains a flat JSON array. A proposed record is:

```json
{
  "gallery_name": "vega-lite",
  "example_id": "bar",
  "example_name": "Simple Bar Chart",
  "example_url": "https://vega.github.io/vega-lite/examples/bar.html",
  "spec_url": "https://raw.githubusercontent.com/vega/vega-lite/main/examples/specs/bar.vl.json",
  "spec_format": "vega-lite",
  "thumbnail_url": "https://vega.github.io/vega-lite/examples/bar.svg",
  "categories": ["Single-View Plots", "Bar Charts"],
  "description": "A bar chart encodes quantitative values as rectangular marks.",
  "datasets": [
    {"name": "cars", "role": "primary"}
  ]
}
```

`thumbnail_url` and `description` are always serialized and use JSON `null`
when absent. Uniform row shape is friendlier to Data Package inference and
static JavaScript consumers. `datasets` contains objects because a parallel
`dataset_roles` array would create positional coupling and a join-table file is
unnecessary.

Update the Data Package declaration to:

- `primaryKey = ["gallery_name", "example_id"]`;
- `spec_url.constraints.unique = true`;
- explicit fields for `example_id`, `spec_format`, and `thumbnail_url`;
- `datasets` described as `{name, role}` objects whose `name` references a
  resource in the same package.

### Canonicalization rules

`canonicalize_records()` owns all ordering and normalization:

1. Sort records by the exact tuple `(gallery_name, example_id)`.
2. Emit object keys in the order shown above; never use `sort_keys=True` as a
   substitute for a defined record layout.
3. Preserve category order because galleries may use it as hierarchy. Trim
   surrounding whitespace and remove later duplicates while preserving the
   first occurrence.
4. Normalize title and description line endings to LF. Keep meaningful
   internal whitespace; map an absent/empty description to `null`.
5. Canonicalize each raw data URL through the datapackage name map. Drop
   external URLs. A URL recognized as vega-datasets but absent from the map is
   a hard error.
6. Deduplicate dataset-role pairs and sort them by `(name, role)`.
7. Do not serialize `generatedAt`, current time, fetch time, HTTP metadata, or
   local paths.

`serialize_records()` returns bytes, not text:

```python
payload = json.dumps(records, indent=2, ensure_ascii=False, allow_nan=False)
return (payload + "\n").encode("utf-8")
```

Write with `Path.write_bytes()`. This avoids Python text-mode newline
translation producing CRLF on Windows. A second run with identical fixture
responses must make `git diff --exit-code -- data/gallery-examples.json` pass.

Do not add a changing revision or generation timestamp to every flat row. It
would turn any upstream commit into a 400-record diff. Phase 0 keeps immutable
`sourceRevision` inside the validated adapter result but omits it from the
aggregate. Once real manifests are published, add one small gallery-level
provenance sidecar if a concrete static aggregate consumer needs it; do not
duplicate a SHA in every row.

## Function-level decomposition of PR #776

| Current function or constant | Phase 0 disposition | New home / notes |
| --- | --- | --- |
| `_VEGA_DATASETS_PREFIXES` | Keep | `datasets.py`; exact first-party classification remains important. |
| `normalize_dataset_reference` | Keep and narrow | Rename `canonicalize_dataset_url`; retain direct/path-prefix lookup and hard errors. Remove the redundant kebab-to-snake guess because `build_name_map` already maps real filenames to canonical resource names. |
| `_collect_url_ref` | Replace | `canonicalize_data_references()` maps typed `{url, role}` values after extraction. |
| `_vegalite_lookup_refs` | Keep algorithm | `specs.py`; return `RawDataReference(url, "lookup")`. |
| `extract_vegalite_datasets` | Keep algorithm and tests | Rename `extract_vegalite_data_references`; direct URLs get `primary`, lookup URLs get `lookup`; recursion stays. |
| `_vega_signal_refs` | Keep algorithm | `specs.py`; signal value/options are `primary`. |
| `_vega_lookup_transform_refs` | Keep algorithm | `specs.py`; return `lookup` references. |
| `extract_vega_datasets` | Keep algorithm and tests | Rename `extract_vega_data_references`; make return type role-aware. |
| `_ALTAIR_PATTERNS`, `_INLINE_DATA_PATTERNS`, `_DATA_IMPORT`, `_UNRECOGNIZED_API_PATTERNS` | Keep temporarily | Move unchanged to `adapters/legacy_altair.py`; delete when Altair switches to `manifest`. |
| `extract_altair_datasets` | Keep temporarily | Rename `extract_altair_data_references`; return role-aware source dataset names, then resolve them to real datapackage paths inside the legacy adapter. |
| `build_name_map` | Keep nearly verbatim | `datasets.py`; wrap it in `DatasetCatalog` with both reference-to-name and name-to-path maps, and add collision detection so one input spelling cannot silently map to two resources. |
| `load_sources` | Replace | `config.py::load_config()` validates per-gallery adapter-specific settings. |
| `_TITLE_PATTERN`, `_DESCRIPTION_PATTERN`, `_CATEGORY_PATTERN` | Keep temporarily | Move to `adapters/legacy_altair.py`; they are presentation parsing, not shared manifest logic. |
| `_parse_altair_metadata` | Keep temporarily | Same legacy module; its tests move with it. |
| `_fetch_text` | Keep behavior, inject dependency | `http.py::NiquestsFetcher`; retain timeout/status/empty-body errors, add an in-memory `Fetcher` protocol for offline tests. |
| `fetch_indexes` | Delete | `pipeline.load_manifests()` runs each configured adapter concurrently. Each adapter owns its source requests. |
| `_longest_wins` | Keep temporarily | `legacy_vega_lite.py`; duplicate-slug repair is an adapter concern and disappears on migration. |
| `_build_vegalite_examples` | Keep temporarily, change output | Emit gallery-index examples with stable IDs and no canonical dataset names. |
| `build_example_list` | Delete | Split into three adapter `load()` functions. No shared conditional on gallery name. |
| `enrich_with_datasets` | Replace | `pipeline.backfill_declarative_data()` handles Vega/Vega-Lite; Altair source enrichment stays inside its legacy adapter. Return new values instead of mutating shared dictionaries. |
| `finalize_examples` | Replace | `serialize.py::canonicalize_records()` and `serialize_records()` define stable IDs, fields, ordering, and bytes. |
| `_MIN_EXPECTED_PER_GALLERY`, `assert_expected_galleries` | Delete | Schema `minItems`, configured-gallery presence, adapter semantic checks, and reviewable scheduled diffs replace count guesses. |
| `assert_unique_spec_urls` | Keep and generalize | `pipeline.validate_aggregate()` checks unique `(gallery,id)`, unique `spec_url`, configured-gallery set, and dataset foreign keys. |
| `run_pipeline` | Replace orchestration | `pipeline.aggregate(config, fetcher) -> list[AggregateRecord]`; no file writes. |
| `async_main`, `main` | Keep as thin wrapper | Parse `--check`, optional `--config`, and optional `--output`; call pipeline and serializer. |

### Proposed callable seams

```python
class Fetcher(Protocol):
    async def text(self, url: str) -> str: ...
    async def json(self, url: str) -> object: ...

@dataclass(frozen=True)
class AdapterContext:
    fetcher: Fetcher
    datasets: DatasetCatalog

class GalleryAdapter(Protocol):
    async def load(self, source: GalleryConfig, context: AdapterContext) -> dict[str, object]: ...

def example_id_from_page_url(page_url: str) -> str: ...
def validate_gallery_index(document: object, schema: object) -> GalleryIndex: ...
def validate_manifest_semantics(index: GalleryIndex, config: GalleryConfig) -> None: ...
def build_dataset_catalog(datapackage: object) -> DatasetCatalog: ...
def extract_vega_data_references(spec: object) -> tuple[RawDataReference, ...]: ...
def extract_vegalite_data_references(spec: object) -> tuple[RawDataReference, ...]: ...
async def aggregate(config: Config, fetcher: Fetcher) -> list[AggregateRecord]: ...
def validate_aggregate(records: Sequence[AggregateRecord], config: Config) -> None: ...
def canonicalize_records(records: Iterable[AggregateRecord]) -> list[AggregateRecord]: ...
def serialize_records(records: Sequence[AggregateRecord]) -> bytes: ...
def summarize(before: Sequence[AggregateRecord], after: Sequence[AggregateRecord]) -> ChangeSummary: ...
```

`aggregate()` does not write files, inspect Git, open PRs, or read environment
variables. The CLI handles files; GitHub Actions handles GitHub state.

## Validation strategy

### 1. Schema tests (offline)

- `Draft202012Validator.check_schema()` validates the checked-in schema itself.
- A `FormatChecker` is enabled explicitly; JSON Schema `format` must not be
  assumed to assert URI/date-time validity by default.
- The conforming fixture validates.
- One focused invalid fixture per rule checks error paths: missing required
  core, unknown non-`x-` field, invalid gallery name, moving-ref semantic
  rejection, invalid role, and malformed URL.
- Error output uses JSON paths and reports all errors sorted by path, not only
  the first failure.

### 2. Pure-function tests (offline)

Move the useful PR #776 cases for dataset normalization and declarative spec
extraction. Add stable-ID cases for `.html`, trailing slash, query/fragment,
percent decoding, empty path, non-HTTPS URLs, encoded slash, and invalid slug
characters. Add role assertions to lookup and topological cases.

### 3. Adapter contract tests (offline)

Each legacy adapter receives a `MappingFetcher` whose URL-to-body map contains
small checked-in snapshots. Assert its complete return value validates against
the gallery-index schema. Preserve targeted regression tests for:

- Vega-Lite empty subcategory fallback, duplicate slug/category merge, and
  longest-title/description behavior;
- Vega duplicate index names and title fallback;
- Altair triple-quoted metadata, inline data, unknown dataset, and unrecognized
  API failures.

These tests name the debt and make deletion at adapter migration obvious.

### 4. Pipeline golden test (offline)

Run all three adapters through the real shared pipeline using fixtures and a
small datapackage namespace. Assert exact equality with
`expected-gallery-examples.json` bytes. Repeat with source object keys and
example lists shuffled where order is not semantically meaningful; bytes must
remain identical. Run serialization twice and assert idempotence.

### 5. Committed-artifact test (offline)

Load `data/gallery-examples.json` without network and enforce:

- exact configured gallery set;
- unique `(gallery_name, example_id)`;
- unique `spec_url`;
- `example_id` matches the slug of `example_url`;
- fixed/canonical record ordering;
- fixed dataset-role ordering and no duplicates;
- every dataset name exists in `datapackage.json`;
- Data Package primary key and `spec_url` uniqueness metadata match the code.

Do not re-fetch gallery pages or specs in this test.

### 6. Network behavior (scheduled/manual only)

The regeneration workflow is the integration test. Expected upstream changes
produce a PR. Schema violations, fetch failures, unresolved first-party dataset
URLs, missing galleries, and invariant failures produce a deduplicated issue
and a failed workflow run. Ordinary push/PR CI remains entirely offline.

### Why count floors go away in Phase 0

Legacy scrapers can still partially misparse an upstream presentation file.
An absolute count floor detects only some such cases and turns legitimate
removals into red CI. The scheduled summary instead makes every added/removed
stable key explicit. `examples.minItems = 1` still catches a completely empty
manifest, and configured-gallery presence catches a missing adapter result.

## Scheduled regeneration

`design/gallery-examples-refresh.workflow.yml` is the proposed workflow shape.
Install it as `.github/workflows/refresh-gallery-examples.yml` in the workflow
commit after the generator and summarizer exist.

Important properties:

- weekly at a non-round UTC minute plus `workflow_dispatch`;
- one concurrency group, without canceling a run in the middle of a branch
  update;
- explicit `contents: write`, `pull-requests: write`, and `issues: write` only;
- repository guard so a fork does not unexpectedly create automation PRs;
- one dedicated branch, `automation/gallery-examples-refresh`;
- `--force-with-lease` only on that automation-owned branch;
- one open PR updated in place, with a fresh generated summary;
- one open failure issue updated with later run links rather than issue spam;
- Data Package metadata regenerated only after semantic aggregate drift is
  detected, so its hash/byte count stays aligned without creating metadata-only
  weekly PRs;
- no network regeneration step in the normal test workflow.

The summary tool compares stable keys and emits both machine JSON and Markdown:

- examples added and removed per gallery;
- title changes (reported as renames while retaining the stable ID);
- page/spec URL changes;
- dataset-role pairs added and removed per example;
- datasets that gained their first example;
- datasets that lost their last example.

The PR body should include the summary and workflow-run link. The data diff
remains the source of truth; no bot comment is necessary on every update.

### Failure handling tradeoff

Opening a new issue for every weekly failure is simple but noisy during a
multi-day upstream incident. Silently leaving only a failed workflow is too
easy to miss. The recommendation is one title-keyed open issue: create it on
the first failure, comment with the new run link on repeated failures, and let
a maintainer close it after recovery. A later successful run need not
auto-close the issue because recovery may still require understanding a data
loss or fallback.

## Real tradeoffs and recommendations

### Adapter classes vs async functions

- **Classes** provide a natural interface but encourage stateful clients and a
  hierarchy with only two operations.
- **Async functions in a registry** are smaller and make migration deletion
  obvious.

Recommend async functions typed by one `GalleryAdapter` callable protocol.
Configuration is passed explicitly; no adapter keeps mutable global state.

### One aggregate schema vs relying on Data Package

The manifest needs full JSON Schema because it is a cross-repository contract.
The flat aggregate already has a Data Package schema, plus stronger Python
invariant tests. A second hand-maintained aggregate JSON Schema would duplicate
that metadata without helping federation. Recommend JSON Schema only for the
gallery index in Phase 0.

### Dataset strings vs `{name, role}` objects

- Strings preserve #776 consumer compatibility but cannot carry role.
- Parallel arrays are compact but fragile.
- Objects are self-describing and make future role additions additive.

Recommend objects. This is the moment to make the change because stable IDs and
the primary key already require a consumer-visible schema migration before the
resource has shipped in a release.

### Preserve vs sort categories

Sorting every array maximizes canonicality but may destroy the gallery's
parent-to-child category ordering. Preserve first-seen category order and test
that each adapter emits it deterministically. Sort only unordered derived data
pairs and top-level records.

### Workflow action vs GitHub CLI

A create-pull-request action is concise but adds a privileged third-party
dependency and hides update semantics. `gh` is present on GitHub-hosted runners
and makes the one-branch/one-PR behavior explicit. Recommend `gh` plus ordinary
Git commands. Pin only official setup actions already used by the repository.

### Provenance sidecar now vs later

A sidecar is cleaner than repeating revisions per record, but a legacy scrape
of moving branches would change its SHA after unrelated commits and generate
metadata-only PRs. The static-consumer profile principally targets upstream
published manifests. Recommend carrying provenance through the internal model
but deferring a committed aggregate sidecar until a consumer asks for it or a
real manifest adapter lands.

## Commit-by-commit implementation sequence

The cleanest implementation branch starts from the updated PR #776 branch
rebased onto current `main`. Each commit should pass offline CI.

1. **`test(gallery-examples): add gallery-index schema and fixtures`**
   - Add `schemas/gallery-index.schema.json`, explicit `jsonschema` dependency,
     valid/invalid fixtures, schema meta-validation, format checking, and npm
     package inclusion for the `schemas` directory.
   - No generator or data output changes.

2. **`refactor(gallery-examples): split scraper into importable modules`**
   - Mechanically move current functions into `datasets.py`, `specs.py`,
     `http.py`, and the three `legacy_*` adapters.
   - Keep the old aggregate record shape and expected output byte-identical for
     this commit. Move tests without changing their assertions except imports.

3. **`feat(gallery-examples): add typed adapter contract and manifest reader`**
   - Add config/models/manifest validation, adapter registry, `Fetcher`
     protocol, in-memory test fetcher, and the small `manifest` adapter.
   - Make legacy adapters emit schema-valid internal gallery indexes.
   - Resolve legacy source revisions to commit SHAs. Pipeline output remains in
     compatibility mode to isolate review.

4. **`feat(gallery-examples): use stable gallery example identities`**
   - Add `example_id_from_page_url`, semantic checks, role-aware data
     references, new aggregate row shape, composite primary key, and unique
     `spec_url` constraint.
   - Replace count floors with configured-gallery and uniqueness invariants.
   - Regenerate `data/gallery-examples.json`, `datapackage.json`,
     `datapackage.md`, and `src/urls.ts` together.

5. **`fix(gallery-examples): guarantee canonical cross-platform output`**
   - Add canonicalization/byte serializer, LF/UTF-8 golden tests, shuffled-input
     tests, `allow_nan=False`, and CLI `--check` mode.
   - Make the committed-artifact validation part of normal offline pytest.

6. **`feat(gallery-examples): summarize regeneration drift`**
   - Add `scripts/summarize_gallery_examples.py`, stable-key diff model,
     Markdown/JSON output, and tests for first/last dataset usage.

7. **`ci(gallery-examples): open scheduled regeneration PRs`**
   - Install the reviewed workflow draft under `.github/workflows/`.
   - Exercise it once with `workflow_dispatch` on the fork before proposing it
     upstream. Confirm update-in-place PR behavior and deduplicated failures.

8. **`docs(gallery-examples): document generation and consumer queries`**
   - Update CONTRIBUTING, README, and the generated Data Package documentation.
   - Include copy-paste Python and JavaScript queries for both index directions.

9. **Optional: `feat(examples): add static gallery browser prototype`**
   - Keep this separate from Phase 0 infrastructure so UI feedback cannot block
     the index and workflow.

If retaining the existing PR's commit history is important, commits 1–2 can be
implemented as a follow-up series on top of it. If reviewer clarity is more
important, squash the original scraper history to its current baseline and use
the sequence above. Do not mix the record-shape migration with the mechanical
module split.

## Flagship static browser sketch

Build a zero-framework page under `examples/gallery-browser/` after Phase 0:

```text
+------------------------------------------------------------------+
| Gallery examples                          [Search title/text...]  |
| [All galleries] [Vega] [Vega-Lite] [Altair]                     |
+----------------------+-------------------------------------------+
| Dataset              | 128 matches              Sort: title v    |
| [All datasets     v] | +-----------+ +-----------+ +-----------+ |
|                      | | thumbnail | | thumbnail | | thumbnail | |
| Categories           | | title     | | title     | | title     | |
| [ ] Bar Charts       | | gallery   | | gallery   | | gallery   | |
| [ ] Maps             | | datasets  | | datasets  | | datasets  | |
| [ ] Interactive      | +-----------+ +-----------+ +-----------+ |
+----------------------+-------------------------------------------+
```

Recommended behavior:

- plain HTML/CSS/ES modules; no new build tool or runtime service;
- fetch `data/gallery-examples.json` once and build gallery, dataset, and
  category facets in memory (roughly 400 records is trivial);
- keep filters in query parameters (`gallery`, `dataset`, `category`, `q`) so
  searches are shareable and browser back/forward works;
- treat dataset names as exact canonical keys and search title, description,
  category, and stable ID case-insensitively;
- show gallery, title, categories, and dataset-role badges; link the card to
  `example_url` and provide a secondary source link to `spec_url`;
- lazy-load thumbnails when present and render an accessible text placeholder
  when absent or failed; this page is a live consumer, so hotlinking a
  manifest-provided thumbnail is within the design's live profile;
- use native controls, visible focus, an `aria-live` result count, and no
  filter behavior dependent on pointer hover;
- render in chunks or use a document fragment, but do not add virtualization
  until the index is large enough to justify its accessibility cost.

The initial prototype should consume the committed JSON exactly as shipped. It
must not import Python modules, scrape gallery pages, or learn repository path
conventions. That makes it a useful contract test as well as a demo.

## Phase 0 acceptance checklist

- [ ] Three configured `legacy-scrape` adapters emit the common schema-valid
      gallery-index shape.
- [ ] A `manifest` adapter exists and passes fixture tests, even though no live
      gallery is configured to use it.
- [ ] Stable IDs are derived from page URLs and the Data Package primary key is
      `(gallery_name, example_id)`.
- [ ] Declarative data extraction and datapackage canonicalization preserve the
      useful behavior and test coverage from PR #776.
- [ ] Altair regex/docstring handling is contained in one disposable module.
- [ ] Committed output is byte-identical across repeated Linux/Windows fixture
      runs and contains no wall-clock values.
- [ ] Normal CI performs no network requests.
- [ ] Scheduled/manual regeneration opens or updates one reviewable PR on
      drift; failures open or update one issue.
- [ ] No workflow, test, or adapter writes to another repository.
- [ ] Documentation shows both `dataset -> examples` and
      `(gallery,id) -> datasets` queries.
