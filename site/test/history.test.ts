// The address bar and history: moving to another page adds an entry, so Back returns to the
// page before (every dataset is a page); tidying the opening address replaces it.
import { expect, test } from 'vitest';
import { hashUpdate } from '../src/dom';

const at = (hash: string) => {
  const href = `https://vega.github.io/vega-datasets/${hash}`;
  return { pathname: '/vega-datasets/', search: '', hash: hash === '#' ? '' : hash, href };
};

test('a step to another dataset, or home, adds a history entry', () => {
  expect(hashUpdate(at('#cars'), 'co2_concentration', true)).toEqual({ method: 'pushState', url: '#co2_concentration' });
  expect(hashUpdate(at('#cars'), '', true)).toEqual({ method: 'pushState', url: '/vega-datasets/' });
});

test('an address that is already right is left alone (no duplicate entries)', () => {
  expect(hashUpdate(at(''), '', true)).toBeNull();
  expect(hashUpdate(at('#cars'), 'cars', true)).toBeNull();
});

test('tidying the address replaces the entry: an empty "#", or an unknown dataset', () => {
  expect(hashUpdate(at('#'), '', false)).toEqual({ method: 'replaceState', url: '/vega-datasets/' });
  expect(hashUpdate(at('#no-such-dataset'), '', false)).toEqual({ method: 'replaceState', url: '/vega-datasets/' });
});
