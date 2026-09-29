import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

// `unit` runs offline against site/generated/catalog.json and the built pages in site/dist
// (build both first with `npm run site:build`).
// `links` checks every outbound link the site generates, so it needs the network.
export default defineConfig({
  root: fileURLToPath(new URL('.', import.meta.url)),
  test: {
    projects: [
      { extends: true, test: { name: 'unit', include: ['test/*.test.ts'] } },
      { extends: true, test: { name: 'links', include: ['test/network/*.test.ts'], testTimeout: 600_000 } },
    ],
  },
});
