// Every outbound link the Field Guide generates must resolve: gallery pages, example
// sources, Vega Editor example routes, and each dataset's file URL (which the starter
// charts load). Needs the network; run with `npm run site:check-links`.
import { existsSync } from 'node:fs';
import path from 'node:path';
import { expect, test } from 'vitest';
import { starterSpec } from '../../src/starter';
import { REPO, loadCatalog } from '../catalog';

/**
 * Files not yet on npm link to GitHub Pages, which this same build deploys; checking
 * the live site would fail until the deploy it is blocking. Check the checkout instead.
 */
const PAGES_DATA = 'https://vega.github.io/vega-datasets/data/';

const ATTEMPTS = 4;
const CONCURRENCY = 12;

/** Final HTTP status after redirects, retrying transient failures (jsDelivr can 403 under bursts). */
async function status(url: string): Promise<number> {
  if (url.startsWith(PAGES_DATA)) {
    return existsSync(path.join(REPO, 'data', url.slice(PAGES_DATA.length))) ? 200 : 404;
  }
  let last = 0;
  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    try {
      last = (await fetch(url, { method: 'HEAD', redirect: 'follow' })).status;
      if (last === 200 || last === 404) return last;
    } catch {
      last = -1;
    }
    await new Promise((r) => setTimeout(r, 1000 * 2 ** attempt));
  }
  return last;
}

test('every link target resolves', async () => {
  const catalog = loadCatalog();
  const links = new Map<string, string>();
  for (const e of catalog.examples) {
    links.set(e.url, `gallery page of ${e.id}`);
    links.set(e.source, `source of ${e.id}`);
    if (e.editor) {
      // The Editor is a single-page app; its example routes load these spec files.
      const [gallery, slug] = e.editor.split('#/examples/')[1]!.split('/');
      const ext = gallery === 'vega' ? 'vg' : 'vl';
      links.set(`https://vega.github.io/editor/spec/${gallery}/${slug}.${ext}.json`, `Editor route of ${e.id}`);
    }
  }
  for (const d of catalog.datasets) {
    links.set(d.url, `file of ${d.name}`);
    const data = (starterSpec(d) as { data?: { url?: string } } | null)?.data?.url;
    if (data) links.set(data, `starter data of ${d.name}`);
  }

  const queue = [...links];
  const broken: string[] = [];
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    for (let next = queue.shift(); next; next = queue.shift()) {
      const [url, what] = next;
      const code = await status(url);
      if (code !== 200) broken.push(`${code} ${what}: ${url}`);
    }
  }));
  expect(broken.sort()).toEqual([]);
  expect(links.size).toBeGreaterThan(1000);
});
