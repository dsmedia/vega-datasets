// Render starter specs from their CSV text in this process's time zone (TZ), and print, per
// spec, its bucketed values keyed "series|bucket" (the bucket named by its calendar parts, in
// UTC for a UTC time unit and local time otherwise): explore-invariants.test.ts runs this under
// several zones (a process can't change its own) and requires the same map in each.
// Input on stdin: [{ spec, csv }]. Output: [{ key: value } | null].
import * as vega from 'vega';
import { compile } from 'vega-lite';

let input = '';
for await (const chunk of process.stdin) input += chunk;
const pad = (n) => String(n).padStart(2, '0');
const out = [];
for (const { spec, csv } of JSON.parse(input)) {
  const quiet = { level: () => quiet, error: () => quiet, warn: () => quiet, info: () => quiet, debug: () => quiet };
  const { spec: vg } = compile(spec, { logger: quiet });
  const loader = vega.loader();
  loader.load = async () => csv;
  const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
  await view.runAsync();
  const enc = spec.encoding ?? {};
  const unit = enc.x?.timeUnit ?? '';
  const utc = unit.startsWith('utc');
  const color = enc.color?.type === 'nominal' ? String(enc.color.field).replace(/\\(.)/g, '$1') : null;
  const series = enc.y?.type === 'nominal' || enc.y?.type === 'ordinal' ? String(enc.y.field).replace(/\\(.)/g, '$1') : color;
  const keyed = {};
  const names = Object.keys(view.getState({ data: vega.truthy, signals: vega.falsy }).data ?? {});
  for (const name of names) {
    for (const r of view.data(name)) {
      const valueKey = Object.keys(r).find((k) => /^(sum|mean)_/.test(k));
      const timeKey = Object.keys(r).find((k) => unit && k.startsWith(`${unit}_`));
      if (!valueKey || !timeKey || !(r[timeKey] instanceof Date)) continue;
      const d = r[timeKey];
      const label = utc ? `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}` : `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
      keyed[`${series ? r[series] : ''}|${label}`] = typeof r[valueKey] === 'number' ? Math.round(r[valueKey] * 1e6) / 1e6 : r[valueKey];
    }
  }
  out.push(Object.keys(keyed).length ? Object.fromEntries(Object.entries(keyed).sort()) : null);
  view.finalize();
}
process.stdout.write(JSON.stringify(out));
