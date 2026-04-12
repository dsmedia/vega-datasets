Thanks @joelostblom — really helpful to hear this lines up with #4002.

One upfront clarification: this rewrite dropped technique/feature detection (that got unwieldy in #724), so the current schema has `datasets` and `categories` but not marks, transforms, or the interactivity flags your `_heuristic_links_for_example` keys off of. That means the "Related gallery examples" thumbnail idea works today via shared datasets/categories, but replacing the per-example backlinks in #4002 would need more. A narrow `marks` + `transforms` field derived from the compiled Vega-Lite spec (not re-parsing altair source — that was #724's mistake) is worth exploring as a follow-up if the per-example backlinks end up needing it, but I'd want to size that separately rather than bolt it onto this PR. In the meantime, the jq snippets below show what the thumbnail use case looks like against the file today.

A few one-liners against the shipped file, to make the shape concrete:

```bash
# Most-referenced datasets (top 5)
jq '[.[].datasets[]] | group_by(.) | map({d: .[0], n: length})
    | sort_by(-.n) | .[0:5]' data/gallery-examples.json

# Coverage gap — datasets in the package with zero gallery examples
jq --slurpfile dp datapackage.json \
  '[$dp[0].resources[].name] - [.[].datasets[]] | unique[]' \
  data/gallery-examples.json

# "Related examples" for a given example — same dataset, different entry
jq --arg url "https://altair-viz.github.io/gallery/bump_chart.html" '
  . as $all
  | ($all[] | select(.example_url == $url) | .datasets) as $ds
  | [$all[]
      | select(.example_url != $url
               and (.datasets | any(. as $d | $ds | index($d))))]
  | map({gallery_name, example_name, example_url})' \
  data/gallery-examples.json

# Altair-only view of examples that reference `cars` (the #4002 backlink shape)
jq '[.[] | select(.gallery_name == "altair" and (.datasets | index("cars")))]
    | map({example_name, example_url, categories})' \
  data/gallery-examples.json
```

On compiling altair → Vega-Lite for ingestion: I did look at it, and there's a decisive problem I hadn't thought through until now. `data.cars.url` compiles to `{"data": {"url": "..."}}` fine, but `data.cars()` returns a DataFrame and compiles to `{"data": {"values": [...]}}` — the filename is gone. Both patterns exist in the gallery (e.g. `anscombe_plot.py` uses `data.anscombe()`, `us_population_pyramid_over_time.py` uses `data.population.url`), so compiling would silently drop dataset references for any example using the DataFrame form. The current regex reads source text and catches both, so it's strictly more accurate for this purpose.

On reorganizing altair examples: rather than churning directories, would altair consider shipping a structured `examples.json` like vega-lite's `site/_data/examples.json` and vega's `docs/_data/examples.json`? Altair is the only one of the three without a machine-readable index, which is what forces this PR to list `tests/examples_methods_syntax/` via the GitHub contents API and parse docstrings out of every file. The nice thing is `populate_examples()` in your sphinxext already builds the right shape in memory on every doc build — the ask is basically "also write it to disk." Happy to open that PR on altair if you're open to it; I'd mirror vega-lite's shape exactly.

For wiring against the file: join on `example_url` (stable), fetch from `cdn.jsdelivr.net/npm/vega-datasets@<v>/data/gallery-examples.json` once this ships (resource name `gallery_examples` in `datapackage.json`), schema changes will be additive only.
