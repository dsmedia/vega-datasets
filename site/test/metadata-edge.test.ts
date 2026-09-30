// Edge cases of the metadata rendering, each once a bug (Codex review, round 1): category
// values that are also JavaScript property names, missing-value markers in charts, range
// bounds that must not leak into bars or other measures, date ranges, integer categories,
// the constraints the fields table lists, its missing-values footer, and literal titles.
import { describe, expect, test } from 'vitest';
import type { Dataset, Field } from '../src/lib/catalog';
import { defaultAxes, scatterFields, scatterSpec } from '../src/lib/explore-model';
import { fieldNotes, missingNote } from '../src/lib/field-meta';
import { densityGrid } from '../src/lib/large-data';
import { datasetMetaDescription } from '../src/lib/seo';
import { starterSpec } from '../src/lib/starter';
import { draw, rowsWith } from './draw';
import { described, strip } from './fixtures';

type Spec = Record<string, unknown>;
type Enc = Record<string, Record<string, unknown>>;

const enc = (spec: Spec | null) => (spec as { encoding: Enc }).encoding;
const quant = (min: number, max: number, extra: Partial<Field> = {}): Field => ({
  name: 'v', type: 'number', description: null, profile: { kind: 'quantitative', min, max, mean: (min + max) / 2, missing: 0, bins: [1, 1] }, ...extra,
});
const nominal = (name: string, values: string[], extra: Partial<Field> = {}): Field => ({
  name, type: 'string', description: null, profile: { kind: 'nominal', distinct: values.length, top: values.map((v) => [v, 1]), missing: 0 }, ...extra,
});
/** A bare table of these fields (no dataset-level metadata). */
const table = (fields: Field[], extra: Partial<Dataset> = {}): Dataset => ({ ...strip(described()), fields, ...extra });

describe('category labels', () => {
  test('any value, even a JavaScript property name, gets its label (interpreter-safe)', async () => {
    const values = ['toString', '__proto__', 'constructor', 'x'];
    const labels = ['Method', 'Proto', 'Maker', 'Ex'];
    const kind = nominal('kind', values, { categories: values.map((value, i) => ({ value, label: labels[i] })) });
    const spec = starterSpec(table([kind, quant(1, 4)]))!;
    const view = await draw(spec, values.map((kind, i) => ({ kind, v: i + 1 })));
    try {
      const svg = await view.toSVG();
      for (const label of labels) expect(svg).toContain(`>${label}<`);
    } finally {
      view.finalize();
    }
  });
});

describe('missing-value markers are missing in charts too', () => {
  const hp = quant(100, 200, { name: 'hp', missingValues: ['-99'] });
  const rows = [{ c: 'a', hp: 100 }, { c: 'a', hp: -99 }, { c: 'b', hp: 200 }, { c: 'b', hp: '-99' }];

  test('a mean by category leaves them out', async () => {
    const view = await draw(starterSpec(table([nominal('c', ['a', 'b']), hp]))!, rows);
    try {
      expect(rowsWith(view, 'mean_hp').map((r) => [r.c, r.mean_hp])).toEqual([['a', 100], ['b', 200]]);
    } finally {
      view.finalize();
    }
  });

  test('the schema’s markers apply to fields without their own; a category marker drops its bar', async () => {
    const d = table([nominal('c', ['a', 'b', 'NA']), quant(100, 200, { name: 'hp' })], { missingValues: ['NA'] });
    const view = await draw(starterSpec(d)!, [...rows.slice(0, 1), { c: 'NA', hp: 150 }, rows[2]!]);
    try {
      expect(rowsWith(view, 'mean_hp').map((r) => r.c)).toEqual(['a', 'b']);
    } finally {
      view.finalize();
    }
  });

  test('the Explore scatter plot leaves them out of the picked measures', async () => {
    const d = table([hp, quant(10, 40, { name: 'mpg' })]);
    const sf = scatterFields(d)!;
    const view = await draw(scatterSpec(d, sf, { x: 'hp', y: 'mpg', zoom: false, height: 300 }), [
      { hp: 100, mpg: 10 }, { hp: -99, mpg: 20 }, { hp: 200, mpg: 40 },
    ]);
    try {
      expect(view.scale('x').domain()[0]).toBeGreaterThan(0);
      view.signal('xField', 'mpg').signal('yField', 'hp');
      await view.runAsync();
      expect(view.scale('x').domain()[0]).toBeGreaterThan(0);
      expect(view.scale('y').domain()[0]).toBeGreaterThan(0);
    } finally {
      view.finalize();
    }
  });

  test('the density overview doesn’t bin them', () => {
    const rows = [{ a: '1', b: '2' }, { a: '-99', b: '3' }, { a: -99, b: 4 }, { a: '2', b: '5' }];
    expect(densityGrid(rows, 'a', 'b', { x: 10, y: 10 }).complete).toBe(4);
    const g = densityGrid(rows, 'a', 'b', { x: 10, y: 10 }, { x: ['-99'] });
    expect([g.rows, g.complete]).toEqual([4, 2]);
    expect(g.box.x[0]).toBeGreaterThan(0);
  });

  test('without declared markers the specs carry no filter', () => {
    const bare = table([nominal('c', ['a', 'b']), quant(100, 200, { name: 'hp' })]);
    expect(starterSpec(bare)).not.toHaveProperty('transform');
    const both = table([quant(100, 200, { name: 'hp' }), quant(10, 40, { name: 'mpg' })]);
    const sf = scatterFields(both)!;
    expect(JSON.stringify(scatterSpec(both, sf, { ...defaultAxes(sf), zoom: true, height: 300 }))).not.toContain('indexof');
  });
});

describe('documented ranges', () => {
  test('a positive minimum doesn’t push bars off the plot: bars keep their zero baseline', async () => {
    const hp = quant(46, 230, { name: 'hp', constraints: { minimum: 40, maximum: 500 } });
    const spec = starterSpec(table([nominal('c', ['a', 'b']), hp]))!;
    const view = await draw(spec, [{ c: 'a', hp: 46 }, { c: 'b', hp: 230 }]);
    try {
      expect(view.scale('x').domain()).toEqual([0, 500]);
    } finally {
      view.finalize();
    }
  });

  test('a bound on one measure leaves another measure’s axis as it was', async () => {
    const rows = [{ hp: 46, mpg: 12 }, { hp: 230, mpg: 46.6 }];
    const domains = async (d: Dataset) => {
      const sf = scatterFields(d)!;
      const view = await draw(scatterSpec(d, sf, { x: 'mpg', y: 'hp', zoom: true, height: 300 }), rows);
      try {
        return [view.scale('x').domain(), view.scale('y').domain()];
      } finally {
        view.finalize();
      }
    };
    const mpg = quant(12, 46.6, { name: 'mpg' });
    const [plainX] = await domains(table([quant(46, 230, { name: 'hp' }), mpg]));
    const [x, y] = await domains(table([quant(46, 230, { name: 'hp', constraints: { minimum: 0 } }), mpg]));
    expect(x).toEqual(plainX);
    expect(y![0]).toBe(0);
  });

  test('a one-sided bound leaves a histogram’s bins on the data (a bin extent needs both ends)', () => {
    for (const constraints of [{ minimum: 0 }, { maximum: 500 }]) {
      const x = enc(starterSpec(table([quant(46, 230, { constraints })]))).x!;
      expect(x).toEqual({ field: 'v', type: 'quantitative', bin: { maxbins: 30 } });
    }
  });

  test('date bounds are checked against the data like numbers', () => {
    const when: Field = {
      name: 'when', type: 'date', description: null, constraints: { minimum: '2020-01-01', maximum: '2020-12-31' },
      profile: { kind: 'temporal', min: '2019-06-01T00:00:00Z', max: '2021-02-01T00:00:00Z', missing: 0 },
    };
    expect(fieldNotes(when)).toEqual(['Documented range 2020-01-01 – 2020-12-31 (some values fall outside)']);
    const inside = { ...when, profile: { ...when.profile, min: '2020-01-01T00:00:00Z', max: '2020-12-31T00:00:00Z' } } as Field;
    expect(fieldNotes(inside)).toEqual(['Documented range 2020-01-01 – 2020-12-31']);
  });
});

describe('integer categories', () => {
  const grade: Field = {
    name: 'grade', type: 'integer', description: null,
    categories: [{ value: 1, label: 'Low' }, { value: 2, label: 'Mid' }, { value: 3, label: 'High' }],
    profile: { kind: 'quantitative', min: 1, max: 3, mean: 2, missing: 0, bins: [1, 1, 1] },
  };

  test('group a measure’s bars, labeled', async () => {
    const spec = starterSpec(table([grade, quant(46, 230)]))!;
    expect(enc(spec).y).toMatchObject({ field: 'grade', type: 'nominal' });
    const view = await draw(spec, [{ grade: 1, v: 46 }, { grade: 2, v: 100 }, { grade: 3, v: 230 }]);
    try {
      expect(await view.toSVG()).toContain('>Mid<');
    } finally {
      view.finalize();
    }
  });

  test('alone, are counted', () => {
    expect(enc(starterSpec(table([grade]))).y).toMatchObject({ field: 'grade', type: 'nominal' });
  });
});

describe('fields table text', () => {
  test('an allowed set narrower than the categories is listed too', () => {
    const f = nominal('c', ['a', 'b'], { categories: ['a', 'b'], constraints: { enum: ['a'] } });
    expect(fieldNotes(f)).toEqual(['Values: a, b', 'Allowed values: a']);
    // The same set isn't said twice.
    expect(fieldNotes({ ...f, constraints: { enum: ['b', 'a'] } })).toEqual(['Values: a, b']);
  });

  test('lengths, patterns and exclusive bounds', () => {
    expect(fieldNotes(nominal('c', ['AB'], { constraints: { minLength: 2, maxLength: 5, pattern: '^[A-Z]+$' } }))).toEqual([
      'Length 2 – 5 characters', 'Pattern ^[A-Z]+$',
    ]);
    expect(fieldNotes(nominal('c', ['AB'], { constraints: { minLength: 2 } }))).toEqual(['At least 2 characters']);
    expect(fieldNotes(nominal('c', ['AB'], { constraints: { maxLength: 1 } }))).toEqual(['At most 1 character']);
    expect(fieldNotes(quant(1, 2, { constraints: { exclusiveMinimum: 0, exclusiveMaximum: 10 } }))).toEqual(['Greater than 0', 'Less than 10']);
  });

  test('an empty list of its own says the field has no markers', () => {
    expect(fieldNotes(nominal('c', ['a'], { missingValues: [] }))).toEqual(['No missing-value markers']);
  });

  test('the footer names the schema’s markers only for the fields they apply to', () => {
    const own = nominal('c', ['NA'], { missingValues: [] });
    expect(missingNote(table([own], { missingValues: ['NA'] }))).toBeNull();
    expect(missingNote(table([own, nominal('d', ['x'])], { missingValues: ['NA'] }))).toBe('“NA” counts as missing, except in fields that list their own markers.');
    expect(missingNote(table([nominal('d', ['x'])], { missingValues: ['NA'] }))).toBe('“NA” counts as missing.');
  });
});

test('a title is plain text in the meta description, not Markdown', () => {
  expect(datasetMetaDescription({ ...described(), title: 'CO_2 *measurements*' })).toBe('CO_2 *measurements*. A small table for testing.');
  expect(datasetMetaDescription({ ...described(), description: 'A **bold** start.' })).toBe('Five cars, fully described. A bold start.');
});

test('bars with only a positive minimum documented encode as if undescribed', () => {
  const plain = starterSpec(table([nominal('c', ['a', 'b']), quant(46, 230, { name: 'hp' })]));
  expect(starterSpec(table([nominal('c', ['a', 'b']), quant(46, 230, { name: 'hp', constraints: { minimum: 40 } })]))).toEqual(plain);
});
