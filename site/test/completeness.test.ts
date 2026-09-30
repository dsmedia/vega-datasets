// The metadata checklist (lib/completeness.ts): each item on fixtures, the license families,
// datasets without a schema, the order and totals of the status page, where each dataset's
// entry is in the metadata TOML, and the repository edit links go to. The live catalog is
// checked only loosely: the metadata will be filled in, and that must not break a test.
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, expect, test } from 'vitest';
import type { Dataset } from '../src/lib/catalog';
import { ADDITIONS_FILE, byGaps, completeness, documented, entryUrl, hasDescription, resourceLines, sourcesUnlinked, summarize } from '../src/lib/completeness';
import { DEFAULT_SITE_REPO, siteRepo } from '../src/lib/seo';
import { loadCatalog, REPO } from './catalog';
import { described, strip } from './fixtures';

/** The fixture, complete: a title, description, source, known license and every field described. */
function full(): Dataset {
  const d = described();
  return { ...d, sources: [{ title: 'Made up', path: 'https://example.org/' }], fields: d.fields.map((f) => ({ ...f, description: f.description ?? `About ${f.name}.` })) };
}

describe('the checklist', () => {
  test('a fully described dataset has no gaps', () => {
    const s = completeness(full());
    expect(s.has).toEqual({ title: true, description: true, source: true, license: true });
    expect(s.fields).toEqual({ total: 8, undescribed: [] });
    expect(s.gaps).toBe(0);
  });

  test('each dataset-level item is one gap', () => {
    expect(completeness({ ...full(), title: undefined }).has.title).toBe(false);
    expect(completeness({ ...full(), title: '  ' }).gaps).toBe(1);
    expect(completeness({ ...full(), description: '' }).has.description).toBe(false);
    expect(completeness({ ...full(), description: '\n ' }).gaps).toBe(1);
    expect(completeness({ ...full(), sources: [] }).has.source).toBe(false);
    expect(completeness({ ...full(), sources: [] }).gaps).toBe(1);
    const none = { ...full(), title: undefined, description: '', sources: [], licenses: [] };
    expect(completeness(none).gaps).toBe(4);
  });

  test('a source without a link still counts, and is noted', () => {
    const d = { ...full(), sources: [{ title: 'Generated Data' }] };
    expect(completeness(d).has.source).toBe(true);
    expect(sourcesUnlinked(d)).toBe(true);
    expect(sourcesUnlinked(full())).toBe(false);
    expect(sourcesUnlinked({ ...full(), sources: [] })).toBe(false);
  });

  test.each([
    [[], false],
    [[{ name: 'notspecified' }], false],
    [[{ name: 'notspecified' }, { name: 'notspecified', title: 'Unknown' }], false],
    [[{ name: 'notspecified' }, { name: 'CC-BY-4.0' }], true],
    [[{ name: 'CC0-1.0' }], true],
    [[{ name: 'other-pd' }], true],
    [[{ name: 'BSD-3-Clause' }], true],
    [[{ name: 'ODbL-1.0' }], true],
    [[{ name: 'some-other-license' }], true],
  ])('license %j known: %s', (licenses, known) => {
    const s = completeness({ ...full(), licenses });
    expect(s.has.license).toBe(known);
    expect(s.gaps).toBe(known ? 0 : 1);
  });

  test('each field without a description is a gap; whitespace is no description', () => {
    const d = described();
    const s = completeness(d);
    expect(s.fields).toEqual({ total: 8, undescribed: ['id', 'mpg', 'grade', 'side', 'origin', 'when'] });
    // The fixture's source has no link but counts; only the fields are gaps.
    expect(s.gaps).toBe(6);
    const blank = { ...full(), fields: full().fields.map((f, i) => (i === 0 ? { ...f, description: '   ' } : f)) };
    expect(hasDescription(blank.fields[0]!)).toBe(false);
    expect(completeness(blank).fields!.undescribed).toEqual(['id']);
  });

  test('datasets without a schema (maps, images, JSON trees) have no field items', () => {
    const map: Dataset = { ...full(), kind: 'json', format: 'topojson', fields: [], rows: null };
    const image: Dataset = { ...full(), kind: 'file', format: 'png', fields: [], rows: null };
    for (const d of [map, image]) {
      const s = completeness(d);
      expect(s.fields).toBeNull();
      expect(s.gaps).toBe(0);
    }
    expect(completeness({ ...image, title: undefined, licenses: [{ name: 'notspecified' }] }).gaps).toBe(2);
  });

  test('properties that apply only where they fit are documented, never gaps', () => {
    expect(documented(described())).toEqual([
      'Field titles (5 fields)', 'Categories (2 fields)', 'Constraints (4 fields)', 'Missing values', 'Primary key', 'Foreign keys (2)',
    ]);
    const bare = strip(full());
    expect(documented(bare)).toEqual([]);
    expect(completeness(bare).gaps).toBe(1); // the title only: strip() removes it.
    // A field's own markers, without the schema's.
    const own = { ...bare, fields: bare.fields.map((f, i) => (i === 0 ? { ...f, missingValues: ['-'] } : f)) };
    expect(documented(own)).toEqual(['Missing values (1 field)']);
  });
});

describe('the status page', () => {
  const ds = (name: string, gaps: number): Dataset => ({ ...full(), name, fields: full().fields.map((f, i) => (i < gaps ? { ...f, description: null } : f)) });

  test('most gaps first, ties by name', () => {
    const rows = [ds('b', 1), ds('c', 3), ds('a', 1), ds('d', 0)].map((dataset) => ({ dataset, status: completeness(dataset) }));
    expect(byGaps(rows).map((r) => r.dataset.name)).toEqual(['c', 'a', 'b', 'd']);
  });

  test('totals', () => {
    const image: Dataset = { ...full(), name: 'img', kind: 'file', fields: [], licenses: [] };
    expect(summarize([ds('a', 0), ds('b', 8), image])).toEqual({
      datasets: 3,
      complete: 1,
      missing: { title: 0, description: 0, source: 0, license: 1 },
      fields: { total: 16, described: 8 },
      tablesWithoutDescriptions: 1,
    });
  });
});

describe("a dataset's entry in the metadata TOML", () => {
  const TOML = [
    '[package]', // 1
    'name = "x"',
    '',
    '[[resources]] # Path: a.csv', // 4
    'path = "a.csv"',
    '',
    '[[resources.sources]]',
    'path = "https://example.org/a"',
    '',
    '[[resources]]', // 10: no comment, named by its path
    "path = 'b.json'",
    '',
    '[[resources]]', // 13: no comment; its sub-table's path doesn't name it
    '[[resources.licenses]]',
    'path = "https://example.org/license"',
    '',
    '[[resources]]   #   Path: c.png', // 17
    '',
    '[[resources]] # Path: a.csv', // 19: a duplicate keeps the first
  ].join('\r\n');

  test('lines by file: the header comment, else the block\'s own path', () => {
    expect([...resourceLines(TOML)]).toEqual([['a.csv', 4], ['b.json', 10], ['c.png', 17]]);
  });

  test('a link to the line, else to the file', () => {
    const lines = resourceLines(TOML);
    const file = 'https://github.com/o/r/blob/main/_data/datapackage_additions.toml';
    expect(entryUrl(file, lines, 'b.json')).toBe(`${file}#L10`);
    expect(entryUrl(file, lines, 'missing.csv')).toBe(file);
  });

  test('every dataset in the catalog has a line in the real file', () => {
    const lines = resourceLines(readFileSync(path.join(REPO, ADDITIONS_FILE), 'utf8'));
    const text = readFileSync(path.join(REPO, ADDITIONS_FILE), 'utf8').split(/\r?\n/);
    for (const d of loadCatalog().datasets) {
      const n = lines.get(d.file);
      expect(n, d.file).toBeDefined();
      expect(text[n! - 1], d.file).toMatch(/^\[\[resources\]\]/);
    }
  });
});

describe('the repository edit links go to', () => {
  test(`defaults to ${DEFAULT_SITE_REPO}`, () => {
    expect(siteRepo({})).toBe('https://github.com/vega/vega-datasets');
    expect(siteRepo({ SITE_REPO: '' })).toBe('https://github.com/vega/vega-datasets');
    expect(siteRepo({ SITE_REPO: 'not a repo' })).toBe('https://github.com/vega/vega-datasets');
    expect(siteRepo({ SITE_REPO: 'https://evil.example/x' })).toBe('https://github.com/vega/vega-datasets');
  });

  test('SITE_REPO names the repository that built the site', () => {
    expect(siteRepo({ SITE_REPO: 'dsmedia/vega-datasets' })).toBe('https://github.com/dsmedia/vega-datasets');
    expect(siteRepo({ SITE_REPO: ' vega/vega-datasets\n' })).toBe('https://github.com/vega/vega-datasets');
  });
});

test('the live catalog: every dataset is judged, and the totals add up', () => {
  const c = loadCatalog();
  const s = summarize(c.datasets);
  expect(s.datasets).toBe(c.datasets.length);
  const gaps = c.datasets.map((d) => completeness(d).gaps);
  expect(gaps.every((g) => Number.isInteger(g) && g >= 0)).toBe(true);
  expect(s.complete).toBe(gaps.filter((g) => g === 0).length);
  expect(s.fields.total).toBe(c.datasets.reduce((n, d) => n + d.fields.length, 0));
  expect(s.fields.described).toBeLessThanOrEqual(s.fields.total);
});
