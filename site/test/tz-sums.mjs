// Run a starter spec on inline rows in this process's time zone (TZ) and print the y sums, in
// order: explore-rules.test.ts runs it under several zones, since a process can't change its own.
import * as vega from 'vega';
import { compile } from 'vega-lite';

let input = '';
for await (const chunk of process.stdin) input += chunk;
const { spec, rows } = JSON.parse(input);
const { url: _, ...data } = spec.data;
const { spec: vg } = compile({ ...spec, data: { ...data, values: rows } });
const view = new vega.View(vega.parse(vg), { renderer: 'none' });
await view.runAsync();
const names = Object.keys(view.getState({ data: vega.truthy, signals: vega.falsy }).data ?? {});
const sums = names.flatMap((n) => view.data(n)).filter((r) => 'sum_population' in r).map((r) => r.sum_population);
console.log(JSON.stringify([...new Set(sums.map(String))].length === 1 ? sums.slice(0, 3) : sums));
view.finalize();
