// Preview the built site the way GitHub Pages serves it: site/dist on top of the
// repository root, so /data/*, /datapackage.json and the rest resolve as in production.
// Usage: npm run site:serve [-- --port 8000]
import { createReadStream, statSync } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const roots = [path.join(repo, 'site', 'dist'), repo];
const { values } = parseArgs({ options: { port: { type: 'string', default: '8000' } } });

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.csv': 'text/csv; charset=utf-8',
  '.tsv': 'text/tab-separated-values; charset=utf-8',
  '.md': 'text/markdown; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.webp': 'image/webp',
  '.woff2': 'font/woff2',
};

function resolve(urlPath) {
  const rel = decodeURIComponent(urlPath.split('?')[0]).replace(/\/$/, '/index.html');
  for (const root of roots) {
    const file = path.join(root, rel);
    if (!file.startsWith(root + path.sep)) return null;
    try {
      if (statSync(file).isFile()) return file;
    } catch {
      // not in this root
    }
  }
  return null;
}

createServer((req, res) => {
  const file = resolve(req.url ?? '/');
  if (!file) {
    res.writeHead(404).end('Not found');
    return;
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] ?? 'application/octet-stream' });
  createReadStream(file).pipe(res);
}).listen(Number(values.port), () => {
  console.log(`Field Guide at http://localhost:${values.port}/`);
});
