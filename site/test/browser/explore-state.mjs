// What a reader set on an Explore chart survives a redraw, and a burst of redraws draws once
// more, not once per change. The chart redraws when the screen crosses the phone breakpoint
// (and, for canvas charts and the density overview, when the theme changes); the redraw keeps
// the mode, the picked fields, the zoom, the legend isolation and the focus. Legend entries
// isolate from the keyboard too: focusable, Enter or Space to pick one (Shift adds it),
// Escape to clear, with a visible focus ring.
// Not part of `npm run site:test` (it needs Chrome and a built site).
//
// Usage, after `npm run site:build`:
//   node site/test/browser/explore-state.mjs [--port 8126]
// Environment: PUPPETEER_CORE (a folder whose node_modules has puppeteer-core, if it isn't
// installed here) and CHROME_PATH (the Chrome executable). The script starts the preview
// server (site/scripts/serve.mjs) and stops it when it is done.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({ options: { port: { type: 'string', default: '8126' } } });
const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '..', '..', '..');
const base = `http://localhost:${args.port}/vega-datasets/`;
const chrome = process.env.CHROME_PATH ?? 'C:/Program Files/Google/Chrome/Application/chrome.exe';

async function loadPuppeteer() {
  const where = process.env.PUPPETEER_CORE;
  if (where) return (await import(pathToFileURL(createRequire(path.join(where, 'index.js')).resolve('puppeteer-core')).href)).default;
  return (await import('puppeteer-core')).default;
}

/** Start the preview server and resolve once it listens. */
function serve() {
  const child = spawn(process.execPath, [path.join(repo, 'site', 'scripts', 'serve.mjs'), '--port', args.port], { stdio: ['ignore', 'pipe', 'inherit'] });
  return new Promise((resolve, reject) => {
    child.stdout.on('data', (b) => { if (String(b).includes('Field Guide at')) resolve(child); });
    child.on('exit', (code) => reject(new Error(`The preview server exited (${code})`)));
  });
}

const WIDE = { width: 1360, height: 900, deviceScaleFactor: 1 };
/** A time series of three colored lines (at most four on a phone, CHART-STANDARDS.md S1). */
const SERIES = 'datasets/iowa_electricity/';
const LINES = 3;
// A phone-width window (not an emulated phone: switching touch on reloads the page).
const PHONE = { width: 390, height: 844, deviceScaleFactor: 1 };

const puppeteer = await loadPuppeteer();
const server = await serve();
const browser = await puppeteer.launch({ executablePath: chrome, headless: true });
const results = [];
function check(name, ok, detail) {
  results.push(ok);
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}  ${JSON.stringify(detail)}`);
}

/** How many times the Explore chart has drawn (the page counts them). */
const draws = (page) => page.evaluate(() => Number(document.querySelector('#explore')?.dataset.draws ?? 0));

/** Wait until the chart has drawn `n` times and stayed so for a moment (no redraw left queued). */
async function settled(page, n) {
  await page.waitForFunction((k) => Number(document.querySelector('#explore')?.dataset.draws ?? 0) >= k, { timeout: 20000 }, n);
  let last = await draws(page);
  for (;;) {
    await new Promise((r) => setTimeout(r, 600));
    const now = await draws(page);
    if (now === last) return now;
    last = now;
  }
}

/** A page at `url` and `viewport`, its Explore chart drawn once. */
async function open(url, viewport) {
  const page = await browser.newPage();
  await page.setViewport(viewport);
  await page.goto(base + url, { waitUntil: 'networkidle0' });
  await page.evaluate(() => document.querySelector('#explore')?.scrollIntoView());
  await settled(page, 1);
  return page;
}

/** In the page: the legend's entries (label text, and whether the entry is keyboard-ready). */
function legendEntries() {
  return [...document.querySelectorAll('#explore .explore-chart svg g.role-legend-entry g.role-scope > g')].map((g) => ({
    label: g.querySelector('g.role-legend-label text')?.textContent ?? '',
    focusable: g.getAttribute('tabindex') === '0',
    role: g.getAttribute('role'),
    pressed: g.getAttribute('aria-pressed'),
  }));
}

/** In the page: the lines drawn at full opacity (the isolated series), by count, of all. */
function fullLines() {
  const paths = [...document.querySelectorAll('#explore .explore-chart svg g.mark-line path')];
  const full = paths.filter((p) => Number(p.getAttribute('opacity') ?? p.getAttribute('stroke-opacity') ?? 1) > 0.5);
  return { full: full.length, all: paths.length };
}

/** Click the legend entry labeled `label` (with Shift when `add`), as a mouse would. */
function clickEntry({ label, add }) {
  const g = [...document.querySelectorAll('#explore .explore-chart svg g.role-legend-entry g.role-scope > g')].find((x) => x.querySelector('g.role-legend-label text')?.textContent === label);
  const target = g?.querySelector('g.role-legend-symbol path');
  if (!target) return false;
  const r = target.getBoundingClientRect();
  const init = { bubbles: true, cancelable: true, view: window, clientX: r.x + r.width / 2, clientY: r.y + r.height / 2, shiftKey: !!add };
  for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) target.dispatchEvent(new (type.startsWith('pointer') ? PointerEvent : MouseEvent)(type, init));
  return true;
}

try {
  // A time series with a legend (iowa_electricity: three sources, lines on a phone too):
  // isolate one, cross the breakpoint both ways.
  {
    const page = await open(SERIES, WIDE);
    const [first] = await page.evaluate(legendEntries);
    await page.evaluate(clickEntry, { label: first.label });
    await new Promise((r) => setTimeout(r, 300));
    const before = await page.evaluate(fullLines);
    check('time series: a click on a legend entry isolates its line', before.full === 1 && before.all === LINES, before);
    let n = await draws(page);
    await page.setViewport(PHONE);
    n = await settled(page, n + 1);
    const phone = await page.evaluate(fullLines);
    check('time series: the isolation survives the redraw for a phone', phone.full === 1 && phone.all === LINES, phone);
    await page.setViewport(WIDE);
    n = await settled(page, n + 1);
    const back = await page.evaluate(fullLines);
    check('time series: and the redraw back', back.full === 1 && back.all === LINES, back);
    await page.close();
  }

  // A burst of breakpoint crossings: the chart ends drawn for the screen it ended on. (That a
  // burst during a draw draws once more, not once each, is test/serial.test.ts: in a browser
  // the timing of a draw against the crossings isn't under the test's control.)
  {
    const page = await open(SERIES, WIDE);
    const n = await draws(page);
    await page.setViewport(PHONE);
    await page.setViewport(WIDE);
    await page.setViewport(PHONE);
    await settled(page, n + 1);
    const lines = await page.evaluate(fullLines);
    const legendTop = await page.evaluate(() => {
      const svg = document.querySelector('#explore .explore-chart svg');
      const legend = svg?.querySelector('g.role-legend');
      const axis = svg?.querySelector('g.role-axis');
      return legend && axis ? legend.getBoundingClientRect().top < axis.getBoundingClientRect().top : null;
    });
    check('time series: after quick crossings, the last draw is for the phone it ended on (legend on top)', lines.all === LINES && legendTop === true, { lines, legendTop });
    await page.close();
  }

  // Keyboard isolation on the time series: Tab to an entry, Enter, Shift+Space, Escape.
  {
    const page = await open(SERIES, WIDE);
    const entries = await page.evaluate(legendEntries);
    check('time series: every legend entry is focusable, a toggle button', entries.length === LINES && entries.every((e) => e.focusable && e.role === 'button' && e.pressed === 'false'), entries);
    await page.evaluate(() => (document.querySelector('#explore .explore-chart svg g.role-legend-entry g.role-scope > g[tabindex]'))?.focus());
    const ring = await page.evaluate(() => {
      const el = document.activeElement;
      const s = el ? getComputedStyle(el) : null;
      return { label: el?.querySelector('g.role-legend-label text')?.textContent, outline: s ? `${s.outlineStyle} ${s.outlineWidth}` : null };
    });
    check('time series: a focused entry shows a focus ring', ring.outline !== null && !ring.outline.startsWith('none') && ring.outline !== 'auto 0px', ring);
    await page.keyboard.press('Enter');
    await new Promise((r) => setTimeout(r, 300));
    const one = await page.evaluate(fullLines);
    const pressed = await page.evaluate(legendEntries);
    check('time series: Enter isolates the focused series', one.full === 1 && pressed.filter((e) => e.pressed === 'true').length === 1, { one, pressed: pressed.map((e) => e.pressed) });
    await page.keyboard.press('Tab');
    await page.keyboard.down('Shift');
    await page.keyboard.press('Space');
    await page.keyboard.up('Shift');
    await new Promise((r) => setTimeout(r, 300));
    const two = await page.evaluate(fullLines);
    check('time series: Shift+Space on the next entry adds it', two.full === 2, two);
    const focusedLabel = await page.evaluate(() => document.activeElement?.querySelector('g.role-legend-label text')?.textContent ?? null);
    let n = await draws(page);
    await page.setViewport(PHONE);
    n = await settled(page, n + 1);
    const refocused = await page.evaluate(() => document.activeElement?.querySelector('g.role-legend-label text')?.textContent ?? null);
    check('time series: the focused entry has focus again after a redraw', refocused !== null && refocused === focusedLabel, { focusedLabel, refocused });
    await page.keyboard.press('Escape');
    await new Promise((r) => setTimeout(r, 300));
    const none = await page.evaluate(fullLines);
    check('time series: Escape clears the isolation', none.full === LINES, none);
    await page.close();
  }

  // A scatter plot (cars): pick fields, zoom, isolate a color, focus a picker; cross to a phone and back.
  {
    const page = await open('datasets/cars/', WIDE);
    await page.evaluate(() => {
      const x = document.querySelectorAll('#explore .binds select')[0];
      x.value = 'Horsepower';
      x.dispatchEvent(new Event('change', { bubbles: true }));
    });
    await new Promise((r) => setTimeout(r, 400));
    const plot = await page.evaluate(() => {
      const r = document.querySelector('#explore .explore-chart svg g.mark-symbol')?.getBoundingClientRect();
      return r ? { x: r.x + r.width / 2, y: r.y + r.height / 2 } : null;
    });
    // Each axis's range, from its labels (which labels show depends on overlap, not the domain).
    const ticks = () => page.evaluate(() => [...document.querySelectorAll('#explore .explore-chart svg g.role-axis')].map((a) => {
      const v = [...a.querySelectorAll('g.role-axis-label text')].map((t) => Number(t.textContent.replace(/,/g, ''))).filter(Number.isFinite);
      return v.length ? [Math.min(...v), Math.max(...v)] : null;
    }).filter(Boolean));
    const unzoomed = await ticks();
    await page.mouse.move(plot.x, plot.y);
    await page.mouse.wheel({ deltaY: -400 });
    await new Promise((r) => setTimeout(r, 400));
    const zoomed = await ticks();
    check('cars: the wheel zoomed in', JSON.stringify(zoomed) !== JSON.stringify(unzoomed), { unzoomed, zoomed });
    await page.evaluate(clickEntry, { label: 'Japan' });
    await new Promise((r) => setTimeout(r, 300));
    const faded = () => page.evaluate(() => {
      const pts = [...document.querySelectorAll('#explore .explore-chart svg g.mark-symbol path')];
      return pts.filter((p) => Number(p.getAttribute('opacity') ?? p.getAttribute('fill-opacity') ?? 1) < 0.5).length;
    });
    const fadedBefore = await faded();
    await page.evaluate(() => document.querySelectorAll('#explore .binds select')[0].focus());
    let n = await draws(page);
    await page.setViewport(PHONE);
    n = await settled(page, n + 1);
    const onPhone = await page.evaluate(() => ({ x: document.querySelectorAll('#explore .binds select')[0]?.value, mode: document.querySelector('#explore .seg [aria-pressed="true"]')?.dataset.mode, focus: document.activeElement === document.querySelectorAll('#explore .binds select')[0] }));
    check('cars: the redraw for a phone keeps the mode, the field and the focus', onPhone.x === 'Horsepower' && onPhone.mode === 'scatter' && onPhone.focus, onPhone);
    check('cars: and the isolation', fadedBefore > 0 && (await faded()) === fadedBefore, { fadedBefore });
    await page.setViewport(WIDE);
    n = await settled(page, n + 1);
    const back = await ticks();
    check('cars: back on a wide screen, the zoom is as it was', JSON.stringify(back) === JSON.stringify(zoomed), { zoomed, back });
    await page.close();
  }

  // Keyboard isolation on the scatter plot (cars, colored by origin), and its focus ring.
  {
    const page = await open('datasets/cars/', WIDE);
    const entries = await page.evaluate(legendEntries);
    check('scatter: every legend entry is focusable, a toggle button', entries.length === 3 && entries.every((e) => e.focusable && e.role === 'button'), entries);
    await page.evaluate(() => document.querySelector('#explore .binds select')?.focus());
    // Tab from the pickers reaches the legend (the chart's actions menu may come first).
    let label = null;
    for (let i = 0; i < 8 && label === null; i++) {
      await page.keyboard.press('Tab');
      label = await page.evaluate(() => document.activeElement?.closest?.('g.role-legend-entry') ? document.activeElement.querySelector('g.role-legend-label text')?.textContent ?? null : null);
    }
    if (process.env.SHOTS) await (await page.$('#explore')).screenshot({ path: path.join(process.env.SHOTS, 'scatter-legend-focus.png') });
    await page.keyboard.press('Enter');
    await new Promise((r) => setTimeout(r, 300));
    const faded = await page.evaluate(() => [...document.querySelectorAll('#explore .explore-chart svg g.mark-symbol path')].filter((p) => Number(p.getAttribute('opacity') ?? 1) < 0.5).length);
    const pressed = await page.evaluate(legendEntries);
    check('scatter: Tab reaches the legend, Enter isolates the focused origin', label !== null && faded > 0 && pressed.filter((e) => e.pressed === 'true').map((e) => e.label).join() === label, { label, faded, pressed: pressed.map((e) => e.pressed) });
    await page.keyboard.press('Escape');
    await new Promise((r) => setTimeout(r, 300));
    const after = await page.evaluate(() => [...document.querySelectorAll('#explore .explore-chart svg g.mark-symbol path')].filter((p) => Number(p.getAttribute('opacity') ?? 1) < 0.5).length);
    check('scatter: Escape clears it', after === 0, { after });
    await page.close();
  }

  // A Draw button pressed before the chart code has arrived still draws (a quick tap on a
  // phone: seattle_weather_hourly_normals waits for its button there). It was lost before.
  {
    const page = await browser.newPage();
    await page.setViewport({ width: 390, height: 844, deviceScaleFactor: 1, isMobile: true, hasTouch: true });
    await page.goto(`${base}datasets/seattle_weather_hourly_normals/`, { waitUntil: 'networkidle0' });
    const early = await page.evaluate(() => {
      document.querySelector('#explore').scrollIntoView();
      const b = document.querySelector('#explore button.draw');
      b?.click();
      return !!b;
    });
    const drew = await page.waitForFunction(() => document.querySelector('#explore .explore-chart svg, #explore .explore-chart canvas'), { timeout: 20000 }).then(() => true, () => false);
    check('a Draw pressed before the chart code arrives still draws', early && drew, { early, drew });
    await page.close();
  }
} finally {
  await browser.close();
  server.kill();
}
const passed = results.filter(Boolean).length;
console.log(`\n${passed} of ${results.length} checks passed.`);
process.exitCode = passed === results.length ? 0 : 1;
