/**
 * Process-bound fixture for `gbrain jobs drain` (test/jobs-drain-process-bound.serial.test.ts).
 * Runs the real CLI entry (runJobsDrainCli) against an in-memory PGLite brain
 * with a synthetic handler that IGNORES its abort signal and keeps the event
 * loop alive with a live interval — then ends exactly like src/cli.ts does for
 * every non-daemon command: flushThenExit(currentExitCode()).
 * FIXTURE_NO_FORCE_EXIT=1 skips that exit (control: the live timer alone keeps
 * the process running). Synthetic data only; no provider, no network.
 */
import { PGLiteEngine } from '../../src/core/pglite-engine.ts';
import { MinionQueue } from '../../src/core/minions/queue.ts';
import { runJobsDrainCli } from '../../src/commands/jobs-drain.ts';
import { currentExitCode, flushThenExit } from '../../src/core/cli-force-exit.ts';

const engine = new PGLiteEngine();
await engine.connect({});
await engine.initSchema();
// The stub handler never calls a model; without this the keyless fixture is (correctly) blocked by chat_unavailable.
await engine.setConfig('facts.extraction_enabled', 'false');
await new MinionQueue(engine).add('facts-absorb', { slug: 'meetings/drain-fixture-example', sourceId: 'default' }, {});
process.stderr.write('[fixture] drain starting\n');
await runJobsDrainCli(
  ['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--max-runtime-min', '0.01', '--yes', '--json'],
  { probe: async () => engine, full: async () => engine, owned: false },
  {
    pollMs: 50, workerPollMs: 25, graceMs: 500,
    registerHandlers: async (worker) => {
      worker.register('facts-absorb', () => new Promise(() => {
        setInterval(() => { /* uncooperative: ignores job.signal, keeps the loop alive */ }, 25);
      }));
    },
  },
);
process.stderr.write('[fixture] drain returned\n');
if (process.env.FIXTURE_NO_FORCE_EXIT !== '1') flushThenExit(currentExitCode());
