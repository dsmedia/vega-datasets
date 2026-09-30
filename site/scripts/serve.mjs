// Preview the built site the way GitHub Pages serves it: site/dist at /vega-datasets/, on
// top of the repository root, so /vega-datasets/data/*, /vega-datasets/datapackage.json and
// the rest resolve as in production. As on Pages, text is gzipped, directories redirect to
// their trailing-slash URL, and missing pages get the site's 404 page (so a local
// Lighthouse run measures what visitors download).
// Usage: npm run site:serve [-- --port 8000] (a port in use falls back to a free one, printed)
import { createReadStream, statSync } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { createGzip } from 'node:zlib';

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

function send(req, res, status, file) {
  const type = TYPES[path.extname(file)] ?? 'application/octet-stream';
  const gzip = /\bgzip\b/.test(req.headers['accept-encoding'] ?? '') && /text|json|javascript|xml|svg/.test(type);
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'max-age=600', ...(gzip ? { 'Content-Encoding': 'gzip' } : {}) });
  const stream = createReadStream(file);
  (gzip ? stream.pipe(createGzip()) : stream).pipe(res);
}

const server = createServer((req, res) => {
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url ?? '/', 'http://localhost').pathname);
  } catch {
    res.writeHead(400).end('Bad request');
    return;
  }
  if (!pathname.startsWith(BASE)) {
    // The site lives under /vega-datasets/; the host's root (robots.txt and the rest) isn't ours.
    if (pathname === '/' || pathname === BASE.slice(0, -1)) res.writeHead(302, { Location: BASE }).end();
    else res.writeHead(404).end('Not found');
    return;
  }
  const found = resolve(pathname);
  if (found?.redirect) res.writeHead(301, { Location: found.redirect }).end();
  else if (found?.file) send(req, res, 200, found.file);
  else send(req, res, 404, path.join(dist, '404.html'));
});
// A port another run holds (a second worktree's checks) falls back to a free one; the line
// below says which, and the browser checks read their URL from it.
let fallback = false;
server.on('error', (err) => {
  if (err.code !== 'EADDRINUSE' || fallback) throw err;
  fallback = true;
  console.error(`Port ${values.port} is taken: serving on a free port instead.`);
  server.listen(0);
});
server.listen(Number(values.port), () => {
  console.log(`Field Guide at http://localhost:${server.address().port}${BASE}`);
});
