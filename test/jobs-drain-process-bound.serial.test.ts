/**
 * `gbrain jobs drain` process bound with an UNCOOPERATIVE handler.
 *
 * A handler that ignores its abort signal cannot be cancelled safely in
 * process. The drain must (1) return after wall clock + grace, (2) report the
 * domain stop as UNCONFIRMED (fail closed, exit 1) rather than green, and
 * (3) the process must still end — via the CLI's non-daemon exit contract
 * (flushThenExit), which the fixture reproduces. The control run proves the
 * live timer really keeps the event loop alive without that exit.
 *
 * Serial: spawns Bun processes.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const FIXTURE = join(import.meta.dir, 'fixtures', 'jobs-drain-uncooperative.ts');
let home: string;

beforeAll(() => { home = mkdtempSync(join(tmpdir(), 'gbrain-drain-bound-')); });
afterAll(() => { rmSync(home, { recursive: true, force: true }); });

async function spawnFixture(extraEnv: Record<string, string>, killAfterMs: number) {
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(process.env)) if (v !== undefined) env[k] = v;
  delete env.DATABASE_URL;
  delete env.GBRAIN_DATABASE_URL;
  Object.assign(env, { HOME: home, GBRAIN_HOME: home, GBRAIN_SKIP_STARTUP_HOOKS: '1' }, extraEnv);
  const t0 = Date.now();
  const proc = Bun.spawn([process.execPath, '--no-env-file', FIXTURE], { env, stdout: 'pipe', stderr: 'pipe' });
  let killed = false;
  const killer = setTimeout(() => { killed = true; proc.kill(9); }, killAfterMs);
  const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  clearTimeout(killer);
  const drainStarted = stderr.includes('[fixture] drain starting');
  return { stdout, stderr, code, killed, elapsed: Date.now() - t0, drainStarted };
}

describe('uncooperative handler: fail closed, bounded process', () => {
  test('drain returns after wall clock + grace, reports unconfirmed, exits 1 through the CLI exit contract', async () => {
    const r = await spawnFixture({}, 60_000);
    console.log(`[bound] fixture elapsed_ms=${r.elapsed} (includes PGLite boot)`);
    expect(r.killed).toBe(false);
    expect(r.code).toBe(1);
    expect(r.stderr).toContain('[fixture] drain returned');
    const report = JSON.parse(r.stdout.trim().split('\n').pop()!);
    expect(report.outcome).toBe('time_limit');
    expect(report.domain_stop_confirmed).toBe(false);
    expect(report.in_flight_unconfirmed).toBe(1);
    expect(report.budget.final).toBe(false);
    expect(r.stdout + r.stderr).not.toContain('drain-fixture-example');
  }, 90_000);

  test('control: without the exit contract the live timer keeps the process alive past any bound', async () => {
    const r = await spawnFixture({ FIXTURE_NO_FORCE_EXIT: '1' }, 20_000);
    expect(r.drainStarted).toBe(true);
    expect(r.stderr).toContain('[fixture] drain returned');
    expect(r.killed).toBe(true);
  }, 60_000);
});
