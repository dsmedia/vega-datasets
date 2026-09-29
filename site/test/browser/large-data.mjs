// Large data in a real browser: which data files a dataset page requests before any click,
// on a phone and on a desktop, and whether the chart runs the spec the page shows.
// Not part of `npm run site:test` (it needs Chrome and a built site).
//
// Usage, after `npm run site:build`:
//   node site/test/browser/large-data.mjs [--port 8123]
// Environment: PUPPETEER_CORE (a folder whose node_modules has puppeteer-core, if it isn't
// installed here) and CHROME_PATH (the Chrome executable). The script starts the preview
// server (site/scripts/serve.mjs) and stops it when it is done.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({ options: { port: { type: 'string', default: '8123' } } });
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];
function check(name, ok, detail) {
  results.push({ name, ok });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail === undefined ? '' : `  ${JSON.stringify(detail)}`}`);
}

const PHONE = { viewport: { width: 390, height: 844, deviceScaleFactor: 3, isMobile: true, hasTouch: true }, userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1' };
const DESKTOP = { viewport: { width: 1360, height: 900, deviceScaleFactor: 1, isMobile: false, hasTouch: false } };

/**
 * Open a dataset page, bring Explore (and, if asked, In Motion) into view, wait for the
 * page to settle, and report the data files requested so far and what Explore shows.
 */
async function openPage(browser, name, device, { sections = ['#explore'], settle = 2500, block = null } = {}) {
  const ctx = await browser.createBrowserContext();
  const page = await ctx.newPage();
  if (device.userAgent) await page.setUserAgent(device.userAgent);
  await page.setViewport(device.viewport);
  const requests = [];
  // `block.on` aborts matching requests (a failed download) until the check turns it off.
  if (block) {
    await page.setRequestInterception(true);
    page.on('request', (r) => (block.on && block.pattern.test(r.url()) ? r.abort('failed') : r.continue()));
  }
  page.on('request', (r) => { if (r.url().includes('/vega-datasets/data/')) requests.push(r.url().slice(base.length)); });
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));
  await page.goto(`${base}datasets/${name}/`, { waitUntil: 'networkidle0', timeout: 120_000 });
  for (const s of sections) {
    await page.evaluate((sel) => document.querySelector(sel)?.scrollIntoView(), s);
    await sleep(settle);
  }
  const state = await page.evaluate(() => {
    const host = document.querySelector('#explore .explore-chart');
    return {
      pointerCoarse: matchMedia('(pointer: coarse)').matches,
      pointerFine: matchMedia('(pointer: fine)').matches,
      cores: navigator.hardwareConcurrency,
      memory: navigator.deviceMemory ?? null,
      preview: Boolean(host?.querySelector('img.chart-preview')),
      drawButton: host?.querySelector('button.draw')?.textContent ?? document.querySelector('#explore [data-draw-all]')?.textContent ?? null,
      canvas: Boolean(host?.querySelector('.vega-embed canvas')),
      svg: Boolean(host?.querySelector('.vega-embed svg.marks')),
      caption: document.querySelector('#explore .chart-caption .hint')?.textContent ?? null,
    };
  });
  return { ctx, page, requests, errors, state };
}

const server = await serve();
const puppeteer = await loadPuppeteer();
const browser = await puppeteer.launch({ executablePath: chrome, headless: true });
try {
  // A phone (touch, coarse pointer): nothing heavy loads before a click.
  for (const name of ['flights_200k_json', 'flights_20k', 'zipcodes', 'us_10m']) {
    const { ctx, requests, errors, state } = await openPage(browser, name, PHONE);
    check(`phone ${name}: no data file before a click`, requests.length === 0 && state.drawButton !== null && errors.length === 0, { requests, errors, ...state });
    await ctx.close();
  }

  // A desktop: flights_20k draws its points by itself; the heavy maps still wait for a click.
  {
    const { ctx, page, requests, errors, state } = await openPage(browser, 'flights_20k', DESKTOP, { settle: 6000 });
    await page.waitForFunction(() => /of 20,000 rows/.test(document.querySelector('#explore .chart-caption .hint')?.textContent ?? ''), { timeout: 60_000 }).catch(() => {});
    const caption = await page.$eval('#explore .chart-caption .hint', (e) => e.textContent);
    check('desktop flights_20k: points drawn automatically, on canvas', state.canvas && state.drawButton === null && /of 20,000 rows/.test(caption) && errors.length === 0, { requests, errors, ...state, caption });
    await ctx.close();
  }
  for (const name of ['zipcodes', 'us_10m']) {
    const { ctx, page, requests, errors, state } = await openPage(browser, name, DESKTOP);
    const before = [...requests];
    check(`desktop ${name}: preview image, no data file before a click`, state.preview && before.length === 0 && errors.length === 0, { requests: before, errors, ...state });
    await page.click('#explore button.draw');
    await page.waitForSelector('#explore .vega-embed canvas, #explore .vega-embed svg.marks', { timeout: 120_000 });
    await sleep(1000);
    check(`desktop ${name}: "Draw the Live Map" loads the file once`, requests.length === 1 && errors.length === 0, { requests, errors });
    await ctx.close();
  }
  {
    const { ctx, requests, errors, state } = await openPage(browser, 'flights_200k_json', DESKTOP);
    check('desktop flights_200k_json: density overview, no data file before a click', requests.length === 0 && state.svg && errors.length === 0, { requests, errors, ...state });
    await ctx.close();
  }

  // The page runs the spec it shows: "View Source" has the public URL and no inlined rows.
  {
    const { ctx, page, requests, errors } = await openPage(browser, 'cars', DESKTOP, { settle: 4000 });
    await page.waitForSelector('#explore .vega-embed details > summary', { timeout: 60_000 });
    await page.click('#explore .vega-embed details > summary');
    const opened = new Promise((resolve) => ctx.once('targetcreated', resolve));
    await page.evaluate(() => [...document.querySelectorAll('#explore .vega-embed .vega-actions a')].find((a) => /View Source/.test(a.textContent ?? ''))?.click());
    const target = await Promise.race([opened, sleep(10_000).then(() => null)]);
    const popup = target ? await target.page() : null;
    await sleep(500);
    const source = popup ? await popup.evaluate(() => document.body.innerText) : '';
    const hasUrl = source.includes('"url": "https://cdn.jsdelivr.net/npm/vega-datasets@3/data/cars.json"');
    // Inlined rows: any `values` array other than the axis-title layers' single empty datum.
    const inlined = [];
    const walk = (node) => {
      if (Array.isArray(node)) node.forEach(walk);
      else if (node && typeof node === 'object') {
        for (const [k, v] of Object.entries(node)) {
          if (k === 'values' && Array.isArray(v) && v.some((row) => Object.keys(row ?? {}).length)) inlined.push(v.length);
          walk(v);
        }
      }
    };
    try { walk(JSON.parse(source)); } catch { inlined.push('unparsed'); }
    check('cars: "View Source" shows the public URL and no inlined rows', hasUrl && inlined.length === 0 && errors.length === 0, { length: source.length, hasUrl, inlined, requests, errors });
    await ctx.close();
  }

  // A failed download says so, with a way to try again, and counts no rows.
  {
    const block = { on: true, pattern: /\/data\/cars\.json$/ };
    const { ctx, page, errors } = await openPage(browser, 'cars', DESKTOP, { settle: 3000, block });
    const failed = await page.evaluate(() => ({
      message: document.querySelector('#explore .explore-chart')?.textContent?.trim() ?? '',
      retry: Boolean(document.querySelector('#explore .explore-chart button[data-retry]')),
      caption: document.querySelector('#explore .chart-caption .hint')?.textContent ?? '',
    }));
    check('cars, download fails: an error with Retry, and no row count', failed.message.startsWith("Couldn't load cars.json.") && failed.retry && !/of 406 rows/.test(failed.caption), { ...failed, errors });
    block.on = false;
    await page.click('#explore button[data-retry]').catch(() => {});
    await page.waitForFunction(() => /of 406 rows/.test(document.querySelector('#explore .chart-caption .hint')?.textContent ?? ''), { timeout: 30_000 }).catch(() => {});
    const caption = await page.$eval('#explore .chart-caption .hint', (e) => e.textContent);
    check('cars, Retry after a failed download draws the chart', /398 of 406 rows/.test(caption) && Boolean(await page.$('#explore .vega-embed svg.marks')), { caption });
    await ctx.close();
  }

  // gapminder has two charts on one file: the page fetches it once.
  {
    const { ctx, page, requests, errors } = await openPage(browser, 'gapminder', DESKTOP, { sections: ['#explore', '#motion'], settle: 4000 });
    await page.waitForSelector('#motion .motion-chart svg', { timeout: 60_000 }).catch(() => {});
    const count = requests.filter((r) => r.endsWith('gapminder.json')).length;
    check('gapminder: Explore and In Motion share one fetch of gapminder.json', count === 1 && errors.length === 0, { requests, errors });
    await ctx.close();
  }
} finally {
  await browser.close();
  server.kill();
}
const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length} of ${results.length} checks passed.`);
process.exitCode = failed.length ? 1 : 0;
