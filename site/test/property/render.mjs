// Render starter specs from their CSV text in this process's time zone (TZ), and print, per
// spec, its aggregated values in order: explore-invariants.test.ts runs this under several
// zones (a process can't change its own) and requires the same values in each.
// Input on stdin: [{ spec, csv }]. Output: [[values...] | null].
import * as vega from 'vega';
import { compile } from 'vega-lite';

let input = '';
for await (const chunk of process.stdin) input += chunk;
const out = [];
for (const { spec, csv } of JSON.parse(input)) {
  const quiet = { level: () => quiet, error: () => quiet, warn: () => quiet, info: () => quiet, debug: () => quiet };
  const { spec: vg } = compile(spec, { logger: quiet });
  const loader = vega.loader();
  loader.load = async () => csv;
  const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
  await view.runAsync();
  const names = Object.keys(view.getState({ data: vega.truthy, signals: vega.falsy }).data ?? {});
  const values = names
    .flatMap((n) => view.data(n))
    .flatMap((r) => Object.entries(r).filter(([k]) => /^(sum|mean)_/.test(k)).map(([, v]) => (typeof v === 'number' ? Math.round(v * 1e6) / 1e6 : v)));
  out.push(values.length ? values.sort((a, b) => a - b) : null);
  view.finalize();
}
process.stdout.write(JSON.stringify(out));
