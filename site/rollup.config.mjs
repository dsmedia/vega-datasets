// Bundles the Field Guide into site/dist. Run `npm run site:build`, which first
// writes catalog.json and the thumbnails (scripts/build_site_catalog.py).
// SITE_NOINDEX=1 keeps the build out of search results (site.yml sets it outside vega/vega-datasets).
import { copyFileSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';
import nodeResolve from '@rollup/plugin-node-resolve';
import terser from '@rollup/plugin-terser';
import typescript from '@rollup/plugin-typescript';

import { pageShell } from './scripts/page-shell.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const dist = path.join(here, 'dist');

/** Copy the page shell, stylesheet and images next to the bundle (after clearing old chunks). */
function staticFiles() {
  return {
    name: 'static-files',
    buildStart() {
      rmSync(path.join(dist, 'assets'), { recursive: true, force: true });
    },
    writeBundle() {
      const shell = readFileSync(path.join(here, 'static', 'index.html'), 'utf8');
      writeFileSync(path.join(dist, 'index.html'), pageShell(shell, { noindex: process.env.SITE_NOINDEX === '1' }));
      for (const file of ['site.css', 'theme-init.js', 'idl-logo.png']) {
        copyFileSync(path.join(here, 'static', file), path.join(dist, 'assets', file));
      }
    },
  };
}

export default {
  input: path.join(here, 'src', 'main.ts'),
  output: {
    dir: path.join(dist, 'assets'),
    format: 'esm',
    entryFileNames: '[name].js',
    chunkFileNames: '[name]-[hash].js',
    sourcemap: true,
  },
  plugins: [
    nodeResolve({ browser: true }),
    commonjs(),
    json(),
    typescript({ tsconfig: path.join(here, 'tsconfig.json'), noEmitOnError: true }),
    // ASCII-only output: non-ASCII string literals are written as \u escapes.
    terser({ format: { ascii_only: true } }),
    staticFiles(),
  ],
  onwarn(warning, warn) {
    // d3 and vega have known, harmless circular imports.
    if (warning.code === 'CIRCULAR_DEPENDENCY' && /node_modules/.test(warning.ids?.join() ?? '')) return;
    warn(warning);
  },
};
