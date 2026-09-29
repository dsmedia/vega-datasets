// Preview the built site the way GitHub Pages serves it: site/dist at /vega-datasets/, on
// top of the repository root, so /vega-datasets/data/*, /vega-datasets/datapackage.json and
// the rest resolve as in production. Directories redirect to their trailing-slash URL and
// missing pages get the site's 404 page, as on Pages.
// Usage: npm run site:serve [-- --port 8000]
import { createReadStream, statSync } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const BASE = '/vega-datasets/';
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const dist = path.join(repo, 'site', 'dist');
const roots = [dist, repo];
const { values } = parseArgs({ options: { port: { type: 'string', default: '8000' } } });

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.xml': 'application/xml; charset=utf-8',
  '.csv': 'text/csv; charset=utf-8',
  '.tsv': 'text/tab-separated-values; charset=utf-8',
  '.md': 'text/markdown; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.webp': 'image/webp',
  '.woff2': 'font/woff2',
};

const kind = (file) => {
  try {
    const s = statSync(file);
    return s.isFile() ? 'file' : s.isDirectory() ? 'dir' : null;
  } catch {
    return null;
  }
};

/** The file for a request path, or a redirect for a directory without its slash. */
function resolve(pathname) {
  const rel = pathname.slice(BASE.length);
  for (const root of roots) {
    const file = path.join(root, rel);
    if (file !== root && !file.startsWith(root + path.sep)) return null;
    const k = kind(file);
    if (k === 'file') return { file };
    if (k === 'dir' && kind(path.join(file, 'index.html')) === 'file') {
      return pathname.endsWith('/') ? { file: path.join(file, 'index.html') } : { redirect: `${pathname}/` };
    }
  }
  return null;
}

function send(res, status, file) {
  res.writeHead(status, { 'Content-Type': TYPES[path.extname(file)] ?? 'application/octet-stream' });
  createReadStream(file).pipe(res);
}

createServer((req, res) => {
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url ?? '/', 'http://localhost').pathname);
  } catch {
    res.writeHead(400).end('Bad request');
    return;
  }
  if (!pathname.startsWith(BASE)) {
    res.writeHead(302, { Location: BASE }).end();
    return;
  }
  const found = resolve(pathname);
  if (found?.redirect) res.writeHead(301, { Location: found.redirect }).end();
  else if (found?.file) send(res, 200, found.file);
  else send(res, 404, path.join(dist, '404.html'));
}).listen(Number(values.port), () => {
  console.log(`Field Guide at http://localhost:${values.port}${BASE}`);
});
