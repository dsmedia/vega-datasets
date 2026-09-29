// The two Vega hooks that let the page run a spec as published under its CSP
// (lib/vega-data.ts): CSV and TSV readers that compile no code, and the loader's mapping
// from public data URLs to the site's own data/.
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { csvParse } from 'd3-dsv';
import * as vega from 'vega';
import { describe, expect, test } from 'vitest';
import { PUBLIC_DATA, readers, siteDataUri } from '../src/lib/vega-data';
import { loadCatalog, REPO } from './catalog';

const catalog = loadCatalog();
const text = (file: string) => readFileSync(path.join(REPO, 'data', file), 'utf8');

/** Run `fn` as under the page's CSP (script-src 'self', no 'unsafe-eval'): compiling code throws. */
function withoutEval<T>(fn: () => T): T {
  const original = globalThis.Function;
  globalThis.Function = new Proxy(original, {
    apply() { throw new EvalError('CSP: unsafe-eval'); },
    construct() { throw new EvalError('CSP: unsafe-eval'); },
  });
  try {
    return fn();
  } finally {
    globalThis.Function = original;
  }
}

describe('reading tables under the page CSP', () => {
  const tables = catalog.datasets.filter((d) => d.kind === 'table' && (d.format === 'csv' || d.format === 'tsv'));

  test('the harness catches code compilation (d3 csvParse, which Vega uses, fails)', () => {
    expect(() => withoutEval(() => csvParse(text('seattle-weather.csv')))).toThrow(EvalError);
  });

  test.each(tables.map((d) => [d.name, d] as const))('%s parses through vega.formats without compiling code', (_name, d) => {
    const formats = vega as unknown as { formats(name: string, reader?: unknown): unknown };
    const builtIn = formats.formats(d.format);
    try {
      for (const [name, reader] of Object.entries(readers())) formats.formats(name, reader);
      const rows = withoutEval(() => vega.read(text(d.file), { type: d.format as 'csv' })) as Record<string, unknown>[];
      expect(rows).toHaveLength(d.rows!);
      expect(Object.keys(rows[0]!)).toEqual(d.fields.map((f) => f.name));
    } finally {
      formats.formats(d.format, builtIn);
    }
  });

  test('the "dsv" type keeps its delimiter', () => {
    expect(readers().dsv!('a|b\n1|2', { delimiter: '|' })).toEqual([{ a: '1', b: '2' }]);
    expect(readers().csv!('a,b\n1,\n', {})).toEqual([{ a: '1', b: '' }]);
  });
});

describe('the loader maps public data URLs to the site', () => {
  const site = 'http://localhost:8000/vega-datasets/data/';

  test.each([
    ['https://cdn.jsdelivr.net/npm/vega-datasets@3/data/cars.json', `${site}cars.json`],
    ['https://cdn.jsdelivr.net/npm/vega-datasets@3.2.1/data/us-10m.json', `${site}us-10m.json`],
    ['https://vega.github.io/vega-datasets/data/flights-200k.json', `${site}flights-200k.json`],
    ['https://vega.github.io/vega-datasets/data/sub/dir.csv', `${site}sub/dir.csv`],
  ])('%s', (uri, local) => {
    expect(uri).toMatch(PUBLIC_DATA);
    expect(siteDataUri(uri, site)).toBe(local);
  });

  test.each([
    'https://cdn.jsdelivr.net/npm/other-package@1/data/cars.json',
    'https://vega.github.io/vega-lite/data/cars.json',
    'https://example.org/vega-datasets/data/cars.json',
    'data/cars.json',
  ])('leaves %s alone', (uri) => {
    expect(siteDataUri(uri, site)).toBe(uri);
  });
});
