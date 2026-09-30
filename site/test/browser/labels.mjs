// Chart labels read: on every dataset's Explore chart, in every mode, at 1360 and 390 px,
// no two axis labels overlap, no legend title sits on its labels, no label is turned on its side when it fits upright, and a
// gradient legend (a color scale) labels more than its two ends (CHART-STANDARDS.md S10).
// Measured on the rendered SVG: the boxes Chrome lays out, not estimates.
// Not part of `npm run site:test` (it needs Chrome and a built site).
//
// Usage, after `npm run site:build`:
//   node site/test/browser/labels.mjs [--port 8128] [--only cars,stocks]
// Environment: PUPPETEER_CORE (a folder whose node_modules has puppeteer-core, if it isn't
// installed here) and CHROME_PATH (the Chrome executable). The script starts the preview
// server (site/scripts/serve.mjs) and stops it when it is done.
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({ options: { port: { type: 'string', default: '8128' }, only: { type: 'string' } } });
const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '..', '..', '..');
// The server's URL: its default port, or the free one it falls back to when that is taken.
let base = `http://localhost:${args.port}/vega-datasets/`;
const chrome = process.env.CHROME_PATH ?? 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const only = args.only ? new Set(args.only.split(',')) : null;

async function loadPuppeteer() {
  const where = process.env.PUPPETEER_CORE;
  if (where) return (await import(pathToFileURL(createRequire(path.join(where, 'index.js')).resolve('puppeteer-core')).href)).default;
  return (await import('puppeteer-core')).default;
}

function serve() {
  const child = spawn(process.execPath, [path.join(repo, 'site', 'scripts', 'serve.mjs'), '--port', args.port], { stdio: ['ignore', 'pipe', 'inherit'] });
  return new Promise((resolve, reject) => {
    child.stdout.on('data', (b) => {
      const url = String(b).match(/Field Guide at (http:\S+)/)?.[1];
      if (!url) return;
      base = url;
      console.log(`Serving at ${url}`);
      resolve(child);
    });
    child.on('exit', (code) => reject(new Error(`The preview server exited (${code})`)));
  });
}

const VIEWS = { wide: { width: 1360, height: 900, deviceScaleFactor: 1 }, phone: { width: 390, height: 844, deviceScaleFactor: 1, isMobile: true, hasTouch: true } };

/** In the page: the label problems of the drawn Explore chart. */
function labelProblems() {
  const svg = document.querySelector('#explore .explore-chart svg.marks');
  // A canvas chart (a large table) has no label elements to measure: counted, and checked in the specs instead.
  if (!svg) return { drawn: false, canvas: !!document.querySelector('#explore .explore-chart canvas.marks'), problems: [] };
  const problems = [];
  const boxes = (texts) => texts.filter((t) => t.textContent && getComputedStyle(t).opacity !== '0' && t.getAttribute('opacity') !== '0').map((t) => ({ t, r: t.getBoundingClientRect() }));
  for (const axis of svg.querySelectorAll('g.role-axis')) {
    const labels = boxes([...axis.querySelectorAll('g.role-axis-label text')]);
    // Overlap, or labels run together: two labels' boxes closer than GAP px along the axis
    // where they share its other direction ("Oct 2013Jan 2014").
    const GAP = 3;
    for (let i = 0; i < labels.length; i++) {
      for (let j = i + 1; j < labels.length; j++) {
        const a = labels[i].r;
        const b = labels[j].r;
        const w = Math.min(a.right, b.right) - Math.max(a.left, b.left);
        const h = Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top);
        if ((w > -GAP && h > 1) || (h > -GAP && w > 1)) problems.push(`labels overlap or run together: "${labels[i].t.textContent}" and "${labels[j].t.textContent}"`);
      }
    }
    // Turned on its side though the labels would fit upright: each label's width at most its share of the axis.
    const angle = (t) => Number((t.getAttribute('transform') ?? '').match(/rotate\((-?[\d.]+)/)?.[1] ?? 0);
    const turned = labels.filter(({ t }) => angle(t) % 180 !== 0);
    if (turned.length) {
      const widths = turned.map(({ t }) => t.getComputedTextLength());
      const centers = turned.map(({ r }) => (r.left + r.right) / 2).sort((x, y) => x - y);
      const gap = Math.min(...centers.slice(1).map((c, i) => c - centers[i]));
      if (Math.max(...widths) + 4 <= (Number.isFinite(gap) ? gap : Infinity)) problems.push(`labels turned though they fit upright ("${turned[0].t.textContent}")`);
    }
  }
  // A legend's title clear of its labels (a vertical gradient's top label sits at its top edge, under the title).
  for (const legend of svg.querySelectorAll('g.role-legend')) {
    const title = boxes([...legend.querySelectorAll('g.role-legend-title text')])[0];
    if (!title) continue;
    for (const { t, r } of boxes([...legend.querySelectorAll('g.role-legend-label text')])) {
      const w = Math.min(title.r.right, r.right) - Math.max(title.r.left, r.left);
      const h = Math.min(title.r.bottom, r.bottom) - Math.max(title.r.top, r.top);
      if (w > 1 && h > -2) problems.push(`legend title "${title.t.textContent}" on its label "${t.textContent}"`);
    }
  }
  // A gradient legend labels more than its ends (a log scale's decades).
  for (const legend of svg.querySelectorAll('g.role-legend')) {
    if (!legend.querySelector('g.role-legend-gradient')) continue;
    const n = boxes([...legend.querySelectorAll('g.role-legend-label text')]).length;
    if (n < 3) problems.push(`a color legend with ${n} labels`);
  }
  return { drawn: true, problems };
}

const catalog = JSON.parse(readFileSync(path.join(repo, 'site', 'generated', 'catalog.json'), 'utf8'));
const puppeteer = await loadPuppeteer();
const server = await serve();
const browser = await puppeteer.launch({ executablePath: chrome, headless: true });
const failures = [];
let checked = 0;
let canvas = 0;
try {
  for (const d of catalog.datasets) {
    if (only && !only.has(d.name)) continue;
    for (const [label, viewport] of Object.entries(VIEWS)) {
      const page = await browser.newPage();
      await page.setViewport(viewport);
      await page.goto(`${base}datasets/${d.name}/`, { waitUntil: 'networkidle0' });
      if (!(await page.$('#explore'))) { await page.close(); continue; }
      // Draw what the page draws by itself, and a table waiting for its button (not the density overview's full draw).
      await page.evaluate(() => { document.querySelector('#explore').scrollIntoView(); document.querySelector('#explore button.draw')?.click(); });
      const drawn = await page.waitForFunction(() => Number(document.querySelector('#explore')?.dataset.draws ?? 0) >= 1, { timeout: 60000 }).then(() => true, () => false);
      if (!drawn) { failures.push(`${d.name} ${label}: no chart drawn`); await page.close(); continue; }
      const modes = await page.evaluate(() => [...document.querySelectorAll('#explore .seg [data-mode]')].map((b) => b.dataset.mode));
      for (const [i, mode] of (modes.length ? modes : ['only']).entries()) {
        if (i > 0) {
          const n = await page.evaluate(() => Number(document.querySelector('#explore').dataset.draws ?? 0));
          await page.click(`#explore .seg [data-mode="${mode}"]`);
          await page.waitForFunction((k) => Number(document.querySelector('#explore').dataset.draws ?? 0) > k, { timeout: 60000 }, n).catch(() => null);
        }
        await new Promise((r) => setTimeout(r, 200));
        const r = await page.evaluate(labelProblems);
        if (!r.drawn) {
          if (r.canvas) canvas++;
          continue;
        }
        checked++;
        for (const p of r.problems) failures.push(`${d.name} ${mode} ${label}: ${p}`);
      }
      await page.close();
    }
  }
} finally {
  await browser.close();
  server.kill();
}
for (const f of failures) console.log(`FAIL  ${f}`);
console.log(`\n${checked} charts checked, ${failures.length} problems (${canvas} canvas charts not measurable here; chart-standards.test.ts checks their specs).`);
process.exitCode = failures.length || checked < 100 ? 1 : 0;
