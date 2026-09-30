// Switching a snippet tab doesn't move the page. The panels differed in height (the home
// page's Vega-Lite tab was 241 px on a phone, its URL tab 61 px), so each switch shifted
// everything below the box by up to 180 px. Checked on the home page and a dataset page,
// on a phone and a desktop: after each switch, the content below the box stays put.
// Not part of `npm run site:test` (it needs Chrome and a built site).
//
// Usage, after `npm run site:build`:
//   node site/test/browser/snippet-tabs.mjs [--port 8127]
// Environment: PUPPETEER_CORE (a folder whose node_modules has puppeteer-core, if it isn't
// installed here) and CHROME_PATH (the Chrome executable). The script starts the preview
// server (site/scripts/serve.mjs) and stops it when it is done.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({ options: { port: { type: 'string', default: '8127' } } });
const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '..', '..', '..');
// The server's URL: its default port, or the free one it falls back to when that is taken.
let base = `http://localhost:${args.port}/vega-datasets/`;
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

const VIEWPORTS = {
  phone: { width: 390, height: 844, deviceScaleFactor: 2, isMobile: true, hasTouch: true },
  desktop: { width: 1360, height: 900 },
};
const PAGES = [['home', ''], ['cars', 'datasets/cars/']];

/** In the page: the box's height and the top of the page's last element, in document coordinates. */
function measure(i) {
  const box = document.querySelectorAll('[data-snippets]')[i];
  const below = document.querySelector('footer') ?? document.body.lastElementChild;
  return { box: Math.round(box.getBoundingClientRect().height), below: Math.round(below.getBoundingClientRect().top + scrollY) };
}

const puppeteer = await loadPuppeteer();
const server = await serve();
const browser = await puppeteer.launch({ executablePath: chrome, headless: true });
const results = [];
function check(name, ok, detail) {
  results.push(ok);
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}  ${JSON.stringify(detail)}`);
}

try {
  for (const [vp, viewport] of Object.entries(VIEWPORTS)) {
    for (const [label, url] of PAGES) {
      const page = await browser.newPage();
      await page.setViewport(viewport);
      await page.goto(base + url, { waitUntil: 'networkidle0' });
      const sets = await page.$$('[data-snippets]');
      for (const [i, set] of sets.entries()) {
        const start = await page.evaluate(measure, i);
        const seen = [];
        for (const tab of await set.$$('[role=tab]')) {
          await tab.click();
          await new Promise((res) => setTimeout(res, 100));
          const m = await page.evaluate(measure, i);
          seen.push({ tab: await tab.evaluate((t) => t.textContent.trim()), ...m });
        }
        const moved = Math.max(...seen.map((s) => Math.abs(s.below - start.below)));
        check(`${vp} ${label} box ${i}: switching tabs moves nothing below`, moved === 0, { moved, heights: seen.map((s) => `${s.tab} ${s.box}`) });
      }
      await page.close();
    }
  }
} finally {
  await browser.close();
  server.kill();
}
const passed = results.filter(Boolean).length;
console.log(`\n${passed} of ${results.length} checks passed.`);
process.exitCode = passed === results.length ? 0 : 1;
