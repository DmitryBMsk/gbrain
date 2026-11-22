import { describe, test, expect } from 'bun:test';
import { runSuggestion } from '../src/core/ops/lumenix-typesafe.ts';

describe('TypeSafe MCP operation', () => {
  test('returns advisory result without changing names', async () => {
    const result = await runSuggestion({ request: 'Read PDF', roster: [{ name: 'pdf' }], sensitive: false }, async () => JSON.stringify({ status: 'suggested', suggestions: ['pdf'] }));
    expect(result.suggestions).toEqual(['pdf']);
  });
  test('rejects oversized requests before spawning', async () => {
    let called = false;
    const result = await runSuggestion({ request: 'x'.repeat(4001), roster: [], sensitive: false }, async () => { called = true; return ''; });
    expect(called).toBe(false);
    expect(result.status).toBe('unavailable');
  });
  test('provider failure does not leak stderr', async () => {
    const result = await runSuggestion({ request: 'Read PDF', roster: [], sensitive: false }, async () => { throw new Error('token secret'); });
    expect(JSON.stringify(result)).not.toContain('secret');
    expect(result.status).toBe('unavailable');
  });
  test('unknown output skill is rejected', async () => {
    const result = await runSuggestion({ request: 'Read PDF', roster: [], sensitive: false }, async () => JSON.stringify({ status: 'suggested', suggestions: ['invented'] }));
    expect(result.status).toBe('unavailable');
  });
  test('sensitive request skips subprocess entirely', async () => {
    let called = false;
    const result = await runSuggestion({ request: 'Private', roster: [] }, async () => { called = true; return ''; });
    expect(called).toBe(false);
    expect(result.status).toBe('skipped_sensitive');
  });
});
