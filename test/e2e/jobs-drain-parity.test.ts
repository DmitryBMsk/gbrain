// PostgreSQL arm of the shared `gbrain jobs drain` suite: claim/lease fencing,
// SKIP LOCKED concurrency, JSONB authority and the protocol trigger run on real
// PostgreSQL in a fresh isolated database. Refuses (never skips) without a
// safe DATABASE_URL.
import { registerPostgresTests } from '../helpers/test-backends.ts';

await registerPostgresTests(() => import('../jobs-drain.test.ts'));
