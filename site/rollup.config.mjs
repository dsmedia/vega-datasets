// Bundles the Field Guide into site/dist. Run `npm run site:build`, which first
// writes catalog.json and the thumbnails (scripts/build_site_catalog.py).
import { copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';
import nodeResolve from '@rollup/plugin-node-resolve';
import terser from '@rollup/plugin-terser';
import typescript from '@rollup/plugin-typescript';

const here = path.dirname(fileURLToPath(import.meta.url));
const dist = path.join(here, 'dist');
const require = createRequire(import.meta.url);

/** Self-hosted fonts (@fontsource, OFL): the weights and styles the stylesheet uses. */
const FONTS = {
  '@fontsource/spectral': ['500', '600', '400-italic'],
  '@fontsource/public-sans': ['400', '500', '600'],
  '@fontsource/jetbrains-mono': ['400', '500'],
};

/**
 * Write assets/fonts.css (woff2 only; every browser the charts run in reads it)
 * and copy the font files it references.
 */
function writeFonts() {
  const fontsDir = path.join(dist, 'assets', 'fonts');
  mkdirSync(fontsDir, { recursive: true });
  const css = [];
  for (const [pkg, variants] of Object.entries(FONTS)) {
    const root = path.dirname(require.resolve(`${pkg}/package.json`));
    for (const variant of variants) {
      const text = readFileSync(path.join(root, `${variant}.css`), 'utf8')
        .replace(/, url\(\.\/files\/[^)]+\.woff\) format\('woff'\)/g, '')
        .replace(/url\(\.\/files\/([^)]+\.woff2)\)/g, (_, file) => {
          copyFileSync(path.join(root, 'files', file), path.join(fontsDir, file));
          return `url(fonts/${file})`;
        });
      css.push(text);
    }
  }
  writeFileSync(path.join(dist, 'assets', 'fonts.css'), css.join('\n'));
}

/** Copy the page shell and stylesheet next to the bundle (after clearing old chunks). */
function staticFiles() {
  return {
    name: 'static-files',
    buildStart() {
      rmSync(path.join(dist, 'assets'), { recursive: true, force: true });
    },
    writeBundle() {
      copyFileSync(path.join(here, 'static', 'index.html'), path.join(dist, 'index.html'));
      copyFileSync(path.join(here, 'static', 'site.css'), path.join(dist, 'assets', 'site.css'));
      writeFonts();
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
