// Lumenix fork: suggest_skill sends only caller-supplied input to the external selector and returns at most one
// skill from the caller's own roster; every other outcome degrades to unavailable without throwing.
import { describe, test, expect } from 'bun:test';
import { runSuggestion } from '../src/core/ops/lumenix-typesafe.ts';

const ROSTER = [{ name: 'alpha-skill', description: 'Alpha.' }, { name: 'beta-skill', description: 'Beta.' }];
const ok = { request: 'plan a trip', roster: ROSTER, sensitive: false };
const reply = (body: unknown) => async () => JSON.stringify(body);

describe('suggest_skill (runSuggestion)', () => {
  test('dry run and sensitive input never reach the selector', async () => {
    const never = async () => { throw new Error('selector called'); };
    expect(await runSuggestion({ ...ok, dry_run: true }, never)).toEqual({ status: 'dry_run', suggestions: [], calls: 0 });
    expect(await runSuggestion({ ...ok, sensitive: true }, never)).toEqual({ status: 'skipped_sensitive', suggestions: [], calls: 0 });
    expect(await runSuggestion({ request: 'x', roster: ROSTER }, never)).toEqual({ status: 'skipped_sensitive', suggestions: [], calls: 0 });
  });

  test('invalid input is unavailable before any call', async () => {
    const never = async () => { throw new Error('selector called'); };
    for (const p of [{ ...ok, request: '' }, { ...ok, request: 'x'.repeat(4001) }, { ...ok, roster: 'nope' }, { ...ok, roster: Array(201).fill(ROSTER[0]) }]) {
      expect((await runSuggestion(p, never)).status).toBe('unavailable');
    }
  });

  test('only a roster skill, at most one, passes through; anything else is unavailable', async () => {
    expect(await runSuggestion(ok, reply({ status: 'suggested', suggestions: ['beta-skill'] }))).toEqual({ status: 'suggested', suggestions: ['beta-skill'] });
    expect((await runSuggestion(ok, reply({ status: 'no_match', suggestions: [] }))).status).toBe('no_match');
    for (const bad of [
      { status: 'suggested', suggestions: ['not-in-roster'] },
      { status: 'suggested', suggestions: ['alpha-skill', 'beta-skill'] },
      { status: 'weird', suggestions: [] },
      { status: 'suggested', suggestions: 'alpha-skill' },
    ]) expect((await runSuggestion(ok, reply(bad))).status).toBe('unavailable');
    expect((await runSuggestion(ok, async () => 'not json')).status).toBe('unavailable');
    expect((await runSuggestion(ok, async () => { throw new Error('docker down'); })).status).toBe('unavailable');
  });
});
