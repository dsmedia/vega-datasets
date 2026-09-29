// @vitest-environment jsdom
// Loading Vega on demand (client/embed.ts): the import is shared by every chart on the page,
// but a failed import (a dropped connection) isn't kept, so a Retry imports again.
import { describe, expect, test, vi } from 'vitest';
import { ChartCodeError, vegaLoader } from '../src/client/embed';
import { onceUnlessFailed } from '../src/lib/once';

/** Stand-ins for vega-embed, vega-interpreter and vega: just what setting up reads. */
function fakeModules() {
  const formats = vi.fn();
  const vega = { formats, loader: () => ({ load: async () => '' }) };
  return { formats, modules: [{ default: vi.fn() }, { expressionInterpreter: {} }, vega] as never };
}

describe('loading Vega', () => {
  test('a failed import rejects with ChartCodeError, and the next call imports again', async () => {
    const { formats, modules } = fakeModules();
    const importer = vi.fn()
      .mockRejectedValueOnce(new TypeError('Failed to fetch dynamically imported module'))
      .mockResolvedValue(modules);
    const load = vegaLoader(importer);
    const first = load();
    await expect(first).rejects.toBeInstanceOf(ChartCodeError);
    await expect(first).rejects.toThrow("Couldn't load the chart code.");
    const v = await load();
    expect(importer).toHaveBeenCalledTimes(2);
    expect(typeof v.loader).toBe('function');
    // The CSP-safe readers are registered once the modules are in.
    expect(formats.mock.calls.map(([name]) => name).sort()).toEqual(['csv', 'dsv', 'tsv']);
    // Once in, every later chart shares the same modules.
    expect(await load()).toBe(v);
    expect(importer).toHaveBeenCalledTimes(2);
  });

  test('callers waiting on the same attempt share it', async () => {
    let resolve!: (x: number) => void;
    const loadOnce = vi.fn(() => new Promise<number>((r) => (resolve = r)));
    const load = onceUnlessFailed(loadOnce);
    const [a, b] = [load(), load()];
    resolve(7);
    expect(await a).toBe(7);
    expect(await b).toBe(7);
    expect(loadOnce).toHaveBeenCalledTimes(1);
  });
});
