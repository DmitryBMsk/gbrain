/**
 * `gbrain jobs drain <name>` — bounded, one-shot drain of ONE queued job type.
 *
 * Why it exists: durable producers enqueue work for a long-lived consumer. When
 * no consumer runs, the only existing ways to execute that work (`jobs work`,
 * the supervisor, autopilot, `jobs submit --follow`) register EVERY builtin
 * handler and run queue-global maintenance, so they also execute or sweep
 * unrelated jobs. This command is the narrow alternative.
 *
 * Boundaries (each pinned by test/jobs-drain.test.ts + the CLI serial suites):
 *   - Allowlisted names only (DRAINABLE_JOB_NAMES) on the shared default queue.
 *   - Exactly one registered handler: the real builtin one, taken from a
 *     throwaway registry (same pattern as `jobs run-child`), so gateway refresh,
 *     provider-halt cooldown, execution/submission authority and the handler's
 *     own source/revision/visibility/forget fences are unchanged. The claim
 *     query is name-scoped, so no other job can be claimed.
 *   - `queueMaintenance: false`: no global delayed promotion and no
 *     stall/timeout/TTL sweeps. Retries of THIS name are served by one scoped
 *     promotion per run (`MinionQueue.promoteDueDelayed`): only DUE delayed rows
 *     of this (queue, name), at most --max-jobs (independent of rows already
 *     waiting, so an older retry cannot be starved by fresh work), and only
 *     after every preview gate passed. Attempts remain separately <= --max-jobs.
 *   - Preview by default (read-only counts); `--yes` executes. The CLI preview
 *     connects probe-only: no migrations, no gateway/model-discovery refresh.
 *   - Extraction intended but chat unavailable (metadata-only check: configured
 *     model + credentials, never a provider call) blocks before any claim; if it
 *     becomes unavailable mid-run the claim is deferred without an attempt and
 *     the run stops (`chat_unavailable`) — never consumed as a calm keyless skip.
 *   - `--model <provider:model>` pins the extraction model for THIS run: the
 *     preview gates that model (availability, pricing, and an operator
 *     `pricing.overrides` entry that would replace its canonical dated rate →
 *     `pricing_override_conflict`), and the builtin handler receives it as a
 *     registration option that wins over a queued payload model. The durable
 *     payload, the engine config and every other handler are untouched.
 *     Without it, per-job payload models and the configured default apply.
 *   - Pricing snapshot (pinned runs): `pricing.overrides` is read strictly — a
 *     read failure or an unusable document/pinned entry blocks
 *     (`pricing_overrides_unreadable` / `pricing_overrides_invalid`) instead of
 *     counting as "no override". Execution re-reads it once after the preview
 *     and before any promotion or claim, re-applies every pricing gate, and
 *     prices the whole run with exactly that snapshot (the tracker never
 *     re-reads config). Unpinned runs keep the fail-open loader.
 *   - Bounds: `--max-jobs` handler executions (attempts, not successes), a
 *     wall-clock `--max-runtime-min` (default 10), an independent empty-queue
 *     observer, and `--max-usd` enforced by a BudgetTracker scoped around the
 *     worker (the gateway reserves chat/embed/rerank calls against it BEFORE
 *     the provider call; an unpriced model fails closed).
 *   - `--job-ids` (1-4 ids): one selector threaded into the count query, the
 *     scoped promotion and the worker's claim (`MinionWorkerOpts.jobIds` →
 *     `MinionQueue.claim`), so nothing outside the selection — including rows
 *     enqueued mid-run — is promoted, claimed or kept alive by the observer.
 *   - `--max-model-attempts N`: supported only with an explicit native Google
 *     `--model google:…` (parser rejects otherwise; preview blocks programmatic
 *     callers with `model_attempts_unsupported_model`). An invocation preflight
 *     (`withAIInvocationPreflight`, run before any parent guard, which it never
 *     replaces) refuses chat calls of any other provider family
 *     (`model_provider_mismatch`) and admits at most N chat invocations — an
 *     upper bound on SDK chat calls, not confirmed requests; with the guard active
 *     the gateway turns SDK retries off, so each transport call is one counted
 *     attempt. Admitting the Nth stops claiming (`model_attempts_exhausted`);
 *     a later attempt is refused before the provider and its claim deferred.
 *   - Both are opt-in: without them the queries, retries and JSON documents
 *     are the legacy ones; `job_ids` / `max_model_attempts` / `model_attempts`
 *     / `models` (confirmed model ids, else null) appear only when requested.
 *   - Settlement is tracked per claim (handler promise + the worker's durable
 *     `jobSettled` write), independent of `worker.start()`, which may resolve
 *     after its own shutdown grace while a handler still runs.
 *   - Output: counts and reason codes only — never payloads, slugs or messages.
 *
 * Honest limits: the USD cap is a tracked-estimate ceiling (reservation uses
 * estimated input + max output tokens; settlement uses provider-reported
 * usage), not a guarantee about the provider's invoice. A handler that ignores
 * its abort is reported as unconfirmed; its claim is released, and the CLI
 * process exit is the bound on its remaining work.
 */
import { existsSync } from 'node:fs';
import type { BrainEngine } from '../core/engine.ts';
import type { PricingOverrides } from '../core/budget/budget-tracker.ts';
import type { AIInvocation } from '../core/ai/invocation-guard.ts';
import { JobDeferredError } from '../core/minions/errors.ts';
import type { MinionHandler } from '../core/minions/types.ts';
import { MinionWorker, type JobSettledOutcome } from '../core/minions/worker.ts';
import { RateLeaseUnavailableError } from '../core/minions/rate-leases.ts';

export const DRAINABLE_JOB_NAMES: ReadonlySet<string> = new Set(['facts-absorb']);
export const DRAIN_QUEUE = 'default';
const MAX_JOBS_LIMIT = 50;
const MAX_USD_LIMIT = 1000;
const DEFAULT_RUNTIME_MIN = 10;
const MAX_RUNTIME_MIN = 60;
const MAX_JOB_IDS = 4;
const MAX_MODEL_ATTEMPTS_LIMIT = 16;

export const DRAIN_HELP = `gbrain jobs drain — bounded one-shot drain of one queued job type

USAGE
  gbrain jobs drain facts-absorb --max-jobs N --max-usd X
                    [--max-runtime-min M] [--model provider:model]
                    [--job-ids ID[,ID...]] [--max-model-attempts N]
                    [--queue default] [--yes] [--json]

Without --yes this is a read-only preview: counts (waiting, delayed_due,
delayed_future, active, active_expired), blockers, extraction/chat/embedding/
pricing status. With extraction enabled, an unusable chat or embedding
configuration blocks (embedding_disabled=true is an explicit opt-out). Nothing is claimed, promoted or authorised.

OPTIONS
  --max-jobs N         Required. Handler executions to attempt (1-50). Counts
                       attempts, not successes.
  --max-usd X          Required. Cost ceiling for tracked model calls (> 0).
                       An unpriced extraction/embedding model refuses to run.
                       This bounds gbrain's tracked estimate, not the
                       provider's invoice.
  --max-runtime-min M  Wall-clock bound in minutes (default ${DEFAULT_RUNTIME_MIN}, max ${MAX_RUNTIME_MIN}).
                       Running work is aborted at the bound; its claim is
                       released without burning an attempt.
  --model P:M          Extraction model for this run only (e.g.
                       google:gemini-3.8-flash). Wins over the configured
                       default and any queued per-job model; the preview
                       gates it. A pricing.overrides entry that would replace
                       its canonical rate blocks (pricing_override_conflict),
                       as does an unreadable or invalid pricing.overrides
                       (pricing_overrides_unreadable / _invalid). The run
                       prices with the snapshot re-checked before any claim.
                       Writes no config and no job payload.
  --job-ids ID,...     Only these jobs (1-${MAX_JOB_IDS} distinct ids of this name): counts,
                       promotion and claims are scoped to them; every other
                       row, including rows enqueued during the run, is left
                       untouched. Nothing claimable ends the run at once.
  --max-model-attempts N
                       Ceiling on chat provider attempts for this run (1-${MAX_MODEL_ATTEMPTS_LIMIT}).
                       Supported only with an explicit native Google
                       --model google:<model>; other or missing pins are
                       rejected. Any chat call of another provider family is
                       refused (blocked: model_provider_mismatch). Counts
                       admitted invocations (retries and failed attempts
                       included): an upper bound, not confirmed or billed
                       requests. SDK-internal retries are off. Admitting
                       the last attempt stops claiming (that call settles
                       normally); a later retry
                       never reaches the provider and its job is handed
                       back without burning an attempt.
                       With --job-ids or this flag, preview and report also
                       name the model ids they can confirm (models.chat =
                       the --model pin, models.embedding = the gateway
                       model the brain also stores); otherwise null.
  --queue default      Only the shared default queue is supported.
  --yes                Execute. Registers only this job's builtin handler and
                       runs no queue-wide maintenance. Once, before claiming,
                       promotes DUE delayed retries of this job only, at most
                       N (independent of rows already waiting; claims then
                       follow priority, then age, so older retries are not
                       starved). Promoted rows not reached stay waiting.
  --json               Machine-readable report (counts and reason codes only).

Supported names: ${[...DRAINABLE_JOB_NAMES].join(', ')}

EXIT CODES
  0 preview, or execution ended empty / delayed_only / max_jobs
  1 blocked, time_limit, budget_exhausted, provider_halt, chat_unavailable,
    model_attempts_exhausted, interrupted, error
  2 invalid arguments (rejected before connecting to the brain)
`;

export class DrainUsageError extends Error {
  constructor(message: string) { super(message); this.name = 'DrainUsageError'; }
}

export interface DrainOptions {
  name: string;
  queue: string;
  maxJobs: number;
  maxUsd: number;
  maxRuntimeMs: number;
  /** Explicit extraction model for this run (`--model`); absent = configured/per-job models. */
  model?: string;
  /** `--job-ids`: claim, promote and observe only these rows; absent = every row of this name. */
  jobIds?: number[];
  /** `--max-model-attempts`: per-run ceiling on chat provider attempts; absent = no ceiling. */
  maxModelAttempts?: number;
  execute: boolean;
  json: boolean;
}

export type DrainOutcome =
  | 'empty' | 'delayed_only' | 'max_jobs' | 'time_limit' | 'budget_exhausted'
  | 'provider_halt' | 'chat_unavailable' | 'interrupted' | 'blocked' | 'error'
  /** Only with --max-model-attempts. */
  | 'model_attempts_exhausted';

/** delayed = delayed_due + delayed_future; active_expired ⊆ active (lease past, awaiting a stall sweep). */
export interface DrainCounts {
  waiting: number; delayed: number; delayed_due: number; delayed_future: number; active: number; active_expired: number;
}

export interface DrainPreview {
  schema_version: 1;
  mode: 'preview';
  name: string;
  queue: string;
  /** The `--model` this preview gated; null = configured default and per-job models. */
  model_override: string | null;
  counts: DrainCounts;
  oldest_waiting_age_seconds: number | null;
  extraction_enabled: boolean;
  /** Metadata-only: configured extraction model + credentials; never a provider call. */
  chat: 'available' | 'unavailable' | 'not_required';
  /** Metadata-only (`diagnoseEmbedding`). `disabled` = explicit `embedding_disabled=true`
   *  policy (facts stored without vectors); `unavailable` blocks when extraction is enabled. */
  embedding: 'available' | 'unavailable' | 'disabled' | 'not_required';
  pricing: { chat: 'priced' | 'unpriced' | 'not_required'; embedding: 'priced' | 'unpriced' | 'unavailable' | 'not_required' };
  blockers: string[];
  would_promote_at_most: number;
  would_attempt_at_most: number;
  /** Present only with `--job-ids`: the selection every count above is scoped to. */
  job_ids?: number[];
  /** Present only with `--max-model-attempts`. */
  max_model_attempts?: number;
  /** Present only with `--job-ids` / `--max-model-attempts`. */
  models?: DrainModels;
}

/**
 * Model identifiers an opt-in run can confirm from metadata (no provider call).
 * `chat`: the pinned `--model` when extraction is enabled, else null (queued
 * payload models may win). `embedding`: the gateway embedding model when the
 * brain's stored `embedding_model` names the same model, else null.
 */
export interface DrainModels { chat: string | null; embedding: string | null }

export interface DrainReport {
  schema_version: 1;
  mode: 'execute';
  name: string;
  queue: string;
  /** The `--model` every handler execution of this run used; null = configured/per-job models. */
  model_override: string | null;
  outcome: DrainOutcome;
  blockers: string[];
  /** False = the handler completes jobs as `extraction_disabled` skips (explicit policy). */
  extraction_enabled: boolean;
  /** Due delayed rows of this (queue, name) moved back to waiting by this run. */
  promoted: number;
  /** Handler executions started (the --max-jobs budget). */
  attempted: number;
  /** Handler promise resolved — NOT a completion; see `durable`. */
  handler_returned: number;
  handler_threw: number;
  /** Token-fenced terminal writes the worker actually committed (or lost). */
  durable: Record<JobSettledOutcome, number>;
  /** Claims handed back un-run after a stop decision (also counted in durable.deferred). */
  released_unrun: number;
  /** True when every claim this run took has a settled handler AND a durable
   *  terminal write before return. False = some handler ignored the release
   *  abort; its claim was token-released, but it may still act until this
   *  process exits (the CLI force-exits after reporting). */
  domain_stop_confirmed: boolean;
  in_flight_unconfirmed: number;
  skipped: Record<string, number>;
  failures: Record<string, number>;
  /** Facts the handler reported inserting (a domain effect, independent of durable completion). */
  facts_inserted: number;
  remaining: DrainCounts;
  /** `final` is false when unsettled work could still record spend after return. */
  budget: { max_usd: number; spent_usd: number; exhausted: boolean; final: boolean };
  elapsed_ms: number;
  max_runtime_ms: number;
  /** Present only with `--job-ids`: the only rows this run could promote or claim. */
  job_ids?: number[];
  /** Present only with `--max-model-attempts`. `attempted` = chat invocations admitted by
   *  the drain preflight, which runs before any parent guard: a conservative UPPER BOUND on
   *  SDK chat invocations, not a count of confirmed HTTP or billed requests (a parent
   *  refusal or a transport failure after admission still counts). `exhausted` = ceiling reached. */
  model_attempts?: { attempted: number; max: number; exhausted: boolean };
  /** Present only with `--job-ids` / `--max-model-attempts`: as judged by the execution gates. */
  models?: DrainModels;
}

/** Test seams. Production callers pass nothing. */
export interface DrainDeps {
  registerHandlers?: (worker: MinionWorker, engine: BrainEngine) => Promise<void>;
  /** Empty-queue observer cadence (default 1000ms). */
  pollMs?: number;
  /** Worker claim poll cadence (default 250ms). */
  workerPollMs?: number;
  /** Wait for aborted work after the wall clock before declaring it unconfirmed (default 30000ms). */
  graceMs?: number;
  budgetAuditPath?: string;
  /** Worker's own post-stop in-flight wait (default 30000ms); scaled down only in tests. */
  workerShutdownGraceMs?: number;
  onStarted?: (settled: Promise<void>) => void;
}

// ---------------------------------------------------------------------------
// Argument contract — engine-free
// ---------------------------------------------------------------------------

const VALUE_FLAGS = new Set(['--max-jobs', '--max-usd', '--max-runtime-min', '--model', '--queue', '--job-ids', '--max-model-attempts']);
const BOOL_FLAGS = new Set(['--yes', '--json']);
const DECIMAL = /^\d+(?:\.\d+)?$/;
const JOB_IDS = /^[1-9]\d*(?:,[1-9]\d*)*$/;
/** Supported --max-model-attempts scope in this release: an explicit native Google pin
 *  (other provider families have gateway fallbacks the attempt accounting does not cover). */
const capSupported = (model: string | undefined) => model !== undefined && model.startsWith('google:');
/** Provider-qualified model id; the preview decides availability and pricing. */
const MODEL_ID = /^[a-z][a-z0-9-]{0,31}:[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$/;

/** Parse `['drain', <name>, ...flags]`. Throws DrainUsageError; never touches a brain. */
export function parseDrainArgs(args: readonly string[]): DrainOptions {
  if (args[0] !== 'drain') throw new DrainUsageError('expected the drain subcommand');
  const values = new Map<string, string>();
  const bools = new Set<string>();
  const positional: string[] = [];
  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith('--')) { positional.push(arg); continue; }
    const eq = arg.indexOf('=');
    const flag = eq === -1 ? arg : arg.slice(0, eq);
    if (values.has(flag) || bools.has(flag)) throw new DrainUsageError(`${flag} given more than once`);
    if (BOOL_FLAGS.has(flag)) {
      if (eq !== -1) throw new DrainUsageError(`${flag} takes no value`);
      bools.add(flag);
    } else if (VALUE_FLAGS.has(flag)) {
      const value = eq === -1 ? args[++i] : arg.slice(eq + 1);
      if (value === undefined || value.startsWith('--')) throw new DrainUsageError(`${flag} requires a value`);
      values.set(flag, value);
    } else {
      throw new DrainUsageError(`unsupported flag ${flag}`);
    }
  }
  if (positional.length !== 1) throw new DrainUsageError('exactly one job name is required (e.g. facts-absorb)');
  const name = positional[0];
  if (!DRAINABLE_JOB_NAMES.has(name)) {
    throw new DrainUsageError(`'${name}' is not drainable; supported: ${[...DRAINABLE_JOB_NAMES].join(', ')}`);
  }
  const queue = values.get('--queue') ?? DRAIN_QUEUE;
  if (queue !== DRAIN_QUEUE) throw new DrainUsageError(`--queue must be ${DRAIN_QUEUE}`);

  const jobsRaw = values.get('--max-jobs');
  if (jobsRaw === undefined) throw new DrainUsageError('--max-jobs is required');
  const maxJobs = /^\d+$/.test(jobsRaw) ? Number(jobsRaw) : NaN;
  if (!Number.isInteger(maxJobs) || maxJobs < 1 || maxJobs > MAX_JOBS_LIMIT) {
    throw new DrainUsageError(`--max-jobs must be an integer from 1 to ${MAX_JOBS_LIMIT}`);
  }
  const usdRaw = values.get('--max-usd');
  if (usdRaw === undefined) throw new DrainUsageError('--max-usd is required');
  const maxUsd = DECIMAL.test(usdRaw) ? Number(usdRaw) : NaN;
  if (!Number.isFinite(maxUsd) || maxUsd <= 0 || maxUsd > MAX_USD_LIMIT) {
    throw new DrainUsageError(`--max-usd must be a number greater than 0 and at most ${MAX_USD_LIMIT}`);
  }
  const minRaw = values.get('--max-runtime-min') ?? String(DEFAULT_RUNTIME_MIN);
  const minutes = DECIMAL.test(minRaw) ? Number(minRaw) : NaN;
  if (!Number.isFinite(minutes) || minutes <= 0 || minutes > MAX_RUNTIME_MIN) {
    throw new DrainUsageError(`--max-runtime-min must be greater than 0 and at most ${MAX_RUNTIME_MIN}`);
  }
  const model = values.get('--model');
  if (model !== undefined && !MODEL_ID.test(model)) {
    throw new DrainUsageError('--model must be a provider-qualified model id (provider:model)');
  }
  const idsRaw = values.get('--job-ids');
  let jobIds: number[] | undefined;
  if (idsRaw !== undefined) {
    jobIds = JOB_IDS.test(idsRaw) ? idsRaw.split(',').map(Number) : [];
    if (jobIds.length < 1 || jobIds.length > MAX_JOB_IDS || !jobIds.every(Number.isSafeInteger) || new Set(jobIds).size !== jobIds.length) {
      throw new DrainUsageError(`--job-ids must be 1 to ${MAX_JOB_IDS} distinct positive job ids, comma-separated`);
    }
  }
  const attemptsRaw = values.get('--max-model-attempts');
  let maxModelAttempts: number | undefined;
  if (attemptsRaw !== undefined) {
    maxModelAttempts = /^\d+$/.test(attemptsRaw) ? Number(attemptsRaw) : NaN;
    if (!Number.isInteger(maxModelAttempts) || maxModelAttempts < 1 || maxModelAttempts > MAX_MODEL_ATTEMPTS_LIMIT) {
      throw new DrainUsageError(`--max-model-attempts must be an integer from 1 to ${MAX_MODEL_ATTEMPTS_LIMIT}`);
    }
    if (!capSupported(model)) throw new DrainUsageError('--max-model-attempts requires --model google:<model> (native Google only)');
  }
  return {
    name, queue, maxJobs, maxUsd, maxRuntimeMs: Math.round(minutes * 60_000),
    ...(model !== undefined ? { model } : {}),
    ...(jobIds !== undefined ? { jobIds } : {}),
    ...(maxModelAttempts !== undefined ? { maxModelAttempts } : {}),
    execute: bools.has('--yes'), json: bools.has('--json'),
  };
}

// ---------------------------------------------------------------------------
// Read-only inspection
// ---------------------------------------------------------------------------

async function readCounts(engine: BrainEngine, o: Pick<DrainOptions, 'name' | 'queue' | 'jobIds'>): Promise<DrainCounts & { oldest: number | null }> {
  // --job-ids narrows every count (and so the idle observer) to the selection.
  const [row] = await engine.executeRaw<Record<string, number | string | null>>(
    `SELECT count(*) FILTER (WHERE status = 'waiting')::int AS waiting,
            count(*) FILTER (WHERE status = 'delayed')::int AS delayed,
            count(*) FILTER (WHERE status = 'delayed' AND delay_until <= now())::int AS delayed_due,
            count(*) FILTER (WHERE status = 'active')::int AS active,
            count(*) FILTER (WHERE status = 'active' AND lock_until < now())::int AS active_expired,
            EXTRACT(EPOCH FROM (now() - min(created_at) FILTER (WHERE status = 'waiting')))::int AS oldest
       FROM minion_jobs WHERE queue = $1 AND name = $2${o.jobIds !== undefined ? ' AND id = ANY($3::bigint[])' : ''}`,
    [o.queue, o.name, ...(o.jobIds !== undefined ? [o.jobIds] : [])],
  );
  const n = (k: string) => Number(row?.[k] ?? 0);
  return {
    waiting: n('waiting'), delayed: n('delayed'), delayed_due: n('delayed_due'),
    delayed_future: n('delayed') - n('delayed_due'), active: n('active'), active_expired: n('active_expired'),
    oldest: row?.oldest == null ? null : Number(row.oldest),
  };
}
const countsOf = ({ oldest: _oldest, ...c }: DrainCounts & { oldest: number | null }): DrainCounts => c;
// Promotion is bounded by --max-jobs INDEPENDENTLY of rows already waiting, so
// due retries always re-enter the claim order (priority ASC, created_at ASC).
// Bounding it by (maxJobs - waiting) starved an older retry forever under a
// steady fresh batch of >= maxJobs equal-priority jobs per run. The metadata
// bound (rows promoted) and the execution bound (handler attempts) are
// separate: a run may promote up to maxJobs AND attempt up to maxJobs, so
// promoted rows it does not reach stay waiting for the next run.
const promotionAllowance = (o: DrainOptions, c: DrainCounts) => Math.min(o.maxJobs, c.delayed_due);

/**
 * The `pricing.overrides` snapshot a drain prices with, and the blocker it
 * raises. Pinned (`--model`): the strict reader — an unreadable or invalid
 * document cannot prove absence (`pricing_overrides_unreadable` /
 * `pricing_overrides_invalid`), and an override that would replace the pinned
 * model's canonical dated rate blocks (`pricing_override_conflict`); the
 * stored document is never rewritten. Unpinned: the legacy fail-open loader.
 */
async function pricingSnapshot(engine: BrainEngine, o: DrainOptions): Promise<{ overrides: PricingOverrides | undefined; blocker: string | null }> {
  const { loadPricingOverrides, loadPricingOverridesForModel, pricingOverrideConflicts } = await import('../core/budget/budget-tracker.ts');
  if (o.model === undefined) return { overrides: await loadPricingOverrides(engine), blocker: null };
  const read = await loadPricingOverridesForModel(engine, o.model);
  if (read.status !== 'ok') return { overrides: undefined, blocker: `pricing_overrides_${read.status}` };
  return { overrides: read.overrides, blocker: pricingOverrideConflicts(o.model, read.overrides) ? 'pricing_override_conflict' : null };
}

interface PricingGates {
  overrides: PricingOverrides | undefined;
  extractionEnabled: boolean;
  chat: DrainPreview['chat'];
  embedding: DrainPreview['embedding'];
  pricing: DrainPreview['pricing'];
  blockers: string[];
  models?: DrainModels;
}

/**
 * Every gate that depends on the extraction configuration or on
 * `pricing.overrides`, evaluated against ONE snapshot read here: chat and
 * embedding availability, chat and embedding priceability under that
 * snapshot, and the pinned-model snapshot gates. Metadata only — no request
 * reaches a provider. The preview renders it; execution calls it again and
 * prices with exactly the snapshot it judged.
 */
async function pricingGates(engine: BrainEngine, o: DrainOptions): Promise<PricingGates> {
  const { isFactsExtractionEnabled, getFactsExtractionModel } = await import('../core/facts/extract.ts');
  const extractionEnabled = await isFactsExtractionEnabled(engine);
  const g: PricingGates = {
    overrides: undefined, extractionEnabled, chat: 'not_required', embedding: 'not_required',
    pricing: { chat: 'not_required', embedding: 'not_required' }, blockers: [],
  };
  // Taken even with extraction disabled: extraction can be re-enabled mid-run,
  // and the run prices with this snapshot (a pinned run gates it regardless).
  const priced = await pricingSnapshot(engine, o);
  g.overrides = priced.overrides;
  if (o.jobIds !== undefined || o.maxModelAttempts !== undefined) g.models = { chat: null, embedding: null };
  if (extractionEnabled) {
    const { isModelPriceable } = await import('../core/budget/budget-tracker.ts');
    const { isAvailable, getEmbeddingModel, diagnoseEmbedding } = await import('../core/ai/gateway.ts');
    const { loadConfig } = await import('../core/config.ts');
    const extractionModel = o.model ?? await getFactsExtractionModel(engine);
    // The same predicate the handler's execution-time gate uses.
    try { g.chat = isAvailable('chat', extractionModel) ? 'available' : 'unavailable'; } catch { g.chat = 'unavailable'; }
    g.pricing.chat = isModelPriceable(extractionModel, 'chat', g.overrides) ? 'priced' : 'unpriced';
    // Explicit opt-out (file or DB plane, as the managed facts path reads it) is
    // a policy, not a failure. Otherwise the configured embedding must be usable
    // (gateway configured, recipe + credentials present).
    const embeddingOff = loadConfig()?.embedding_disabled === true || (await engine.getConfig('embedding_disabled')) === 'true';
    if (embeddingOff) g.embedding = 'disabled';
    else { try { g.embedding = diagnoseEmbedding().ok ? 'available' : 'unavailable'; } catch { g.embedding = 'unavailable'; } }
    const embeddingModel = g.embedding === 'available' ? getEmbeddingModel() : null;
    g.pricing.embedding = g.embedding === 'disabled' ? 'not_required' : embeddingModel === null ? 'unavailable'
      : isModelPriceable(embeddingModel, 'embed', g.overrides) ? 'priced' : 'unpriced';
    // Opt-in identity, metadata only: the chat model only when pinned (queued
    // payload models may win otherwise); the embedding only when the brain's
    // stored `embedding_model` names the same model as the gateway.
    if (g.models) {
      g.models.chat = o.model ?? null;
      const stored = (await engine.getConfig('embedding_model'))?.trim() || null;
      g.models.embedding = embeddingModel !== null && stored === embeddingModel ? embeddingModel : null;
    }
    if (g.chat === 'unavailable') g.blockers.push('chat_unavailable');
    if (g.embedding === 'unavailable') g.blockers.push('embedding_unavailable');
    if (g.pricing.chat === 'unpriced') g.blockers.push('unpriced_chat_model');
    if (g.pricing.embedding === 'unpriced') g.blockers.push('unpriced_embedding_model');
  }
  if (priced.blocker) g.blockers.push(priced.blocker);
  return g;
}

/** Read-only: counts, blockers and pricing. No claims, promotion, authorisation or provider calls. */
export async function previewDrain(engine: BrainEngine, o: DrainOptions): Promise<DrainPreview> {
  const blockers: string[] = [];
  const { hasPendingMigrations } = await import('../core/migrate.ts');
  if (await hasPendingMigrations(engine)) blockers.push('pending_migrations');
  const { assertNoUnreviewedJobs } = await import('../core/minions/submission-authority.ts');
  const { OperationError } = await import('../core/ops/contract.ts');
  try { await assertNoUnreviewedJobs(engine); }
  catch (error) {
    if (error instanceof OperationError && error.code === 'permission_denied') blockers.push('unreviewed_jobs');
    else throw error;
  }
  const { autopilotPausedMarkerPath } = await import('../core/autopilot-paths.ts');
  if (existsSync(autopilotPausedMarkerPath())) blockers.push('paused');
  // Programmatic callers skip parseDrainArgs: the supported cap scope is enforced here too.
  if (o.maxModelAttempts !== undefined && !capSupported(o.model)) blockers.push('model_attempts_unsupported_model');

  const g = await pricingGates(engine, o);
  blockers.push(...g.blockers);
  const c = await readCounts(engine, o);
  const promote = blockers.length ? 0 : promotionAllowance(o, c);
  return {
    schema_version: 1, mode: 'preview', name: o.name, queue: o.queue,
    model_override: o.model ?? null,
    counts: countsOf(c),
    oldest_waiting_age_seconds: c.oldest,
    extraction_enabled: g.extractionEnabled,
    chat: g.chat, embedding: g.embedding, pricing: g.pricing, blockers,
    would_promote_at_most: promote,
    would_attempt_at_most: blockers.length ? 0 : Math.min(o.maxJobs, c.waiting + promote),
    ...(o.jobIds !== undefined ? { job_ids: [...o.jobIds] } : {}),
    ...(o.maxModelAttempts !== undefined ? { max_model_attempts: o.maxModelAttempts } : {}),
    ...(g.models ? { models: g.models } : {}),
  };
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

const SAFE_CODE = /^[a-z][a-z0-9_:.-]{0,63}$/;
const SAFE_NAME = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
const bump = (m: Record<string, number>, k: string) => { m[k] = (m[k] ?? 0) + 1; };

async function defaultRegisterHandlers(worker: MinionWorker, engine: BrainEngine, model: string | undefined): Promise<void> {
  const { registerBuiltinHandlers } = await import('./jobs.ts');
  await registerBuiltinHandlers(worker, engine, { quiet: true, ...(model !== undefined ? { factsAbsorbModel: model } : {}) });
}

export async function runDrain(engine: BrainEngine, o: DrainOptions & { execute: true }, deps?: DrainDeps): Promise<DrainReport>;
export async function runDrain(engine: BrainEngine, o: DrainOptions, deps?: DrainDeps): Promise<DrainPreview | DrainReport>;
export async function runDrain(engine: BrainEngine, o: DrainOptions, deps: DrainDeps = {}): Promise<DrainPreview | DrainReport> {
  const startedAt = Date.now();
  const pre = await previewDrain(engine, o);
  if (!o.execute) return pre;

  const r: DrainReport = {
    schema_version: 1, mode: 'execute', name: o.name, queue: o.queue, model_override: o.model ?? null,
    outcome: 'empty', blockers: pre.blockers,
    extraction_enabled: pre.extraction_enabled, promoted: 0,
    attempted: 0, handler_returned: 0, handler_threw: 0,
    durable: { completed: 0, retry_scheduled: 0, dead: 0, released: 0, deferred: 0, dropped: 0, left_for_stall: 0 },
    released_unrun: 0, domain_stop_confirmed: true, in_flight_unconfirmed: 0,
    skipped: {}, failures: {}, facts_inserted: 0, remaining: pre.counts,
    budget: { max_usd: o.maxUsd, spent_usd: 0, exhausted: false, final: true },
    elapsed_ms: 0, max_runtime_ms: o.maxRuntimeMs,
    ...(o.jobIds !== undefined ? { job_ids: [...o.jobIds] } : {}),
    ...(o.maxModelAttempts !== undefined ? { model_attempts: { attempted: 0, max: o.maxModelAttempts, exhausted: false } } : {}),
    ...(pre.models ? { models: pre.models } : {}),
  };
  const finish = async (outcome: DrainOutcome): Promise<DrainReport> => {
    r.outcome = outcome;
    r.remaining = countsOf(await readCounts(engine, o));
    r.elapsed_ms = Date.now() - startedAt;
    return r;
  };
  if (pre.blockers.length) return finish('blocked');

  // Execution prices with ONE override snapshot: read fresh after the preview
  // and before any promotion or claim, with EVERY snapshot-dependent gate
  // (chat/embedding availability and priceability, pinned-model gates)
  // evaluated again against it, then handed unchanged to the tracker, which
  // never re-reads config. A change since the preview is judged here; a later
  // change cannot reach this run.
  const gates = await pricingGates(engine, o);
  if (gates.models) r.models = gates.models;
  if (gates.blockers.length) {
    r.blockers = gates.blockers;
    return finish('blocked');
  }

  const { MinionQueue } = await import('../core/minions/queue.ts');
  const queue = new MinionQueue(engine);
  // Every gate passed: serve this name's own due retries, once, bounded.
  const allowance = promotionAllowance(o, pre.counts);
  if (allowance > 0) {
    r.promoted = (await queue.promoteDueDelayed({
      queue: o.queue, name: o.name, limit: allowance, ...(o.jobIds !== undefined ? { jobIds: o.jobIds } : {}),
    })).length;
  }
  if (pre.counts.waiting + r.promoted === 0) return finish(pre.counts.delayed > 0 ? 'delayed_only' : 'empty');

  const { BudgetTracker } = await import('../core/budget/budget-tracker.ts');
  const { withBudgetTracker } = await import('../core/ai/gateway.ts');
  const { factsAbsorbUnavailable } = await import('./jobs.ts');
  const { FactsExtractionError } = await import('../core/facts/extract.ts');
  const { withAIInvocationPreflight } = await import('../core/ai/invocation-guard.ts');
  const tracker = new BudgetTracker({
    label: `jobs-drain:${o.name}`, maxCostUsd: o.maxUsd, pricingOverrides: gates.overrides,
    // Hidden SDK retries would bill attempts outside the one reservation --max-usd admitted.
    providerRetries: 0,
    ...(deps.budgetAuditPath ? { auditPath: deps.budgetAuditPath } : {}),
  });

  // Take the real builtin handler (with its wrappers) from a throwaway registry.
  // An explicit --model reaches it as a registration option, never via job.data.
  const registry = new MinionWorker(engine, { queue: o.queue, concurrency: 1, healthCheckInterval: 0 });
  await (deps.registerHandlers ?? ((w, e) => defaultRegisterHandlers(w, e, o.model)))(registry, engine);
  const inner: MinionHandler | undefined = registry.getHandler(o.name);
  if (!inner) throw new Error(`no builtin handler registered for ${o.name}`);

  const worker = new MinionWorker(engine, {
    queue: o.queue, concurrency: 1, pollInterval: deps.workerPollMs ?? 250,
    healthCheckInterval: 0, maxRssMb: 0, queueMaintenance: false,
    ...(o.jobIds !== undefined ? { jobIds: o.jobIds } : {}),
    ...(deps.workerShutdownGraceMs !== undefined ? { shutdownGraceMs: deps.workerShutdownGraceMs } : {}),
  });

  // Settlement is tracked per claim, independent of worker.start(): a claim is
  // settled once its handler promise has settled AND the worker reported the
  // durable terminal write for that exact claim token.
  const claims = new Map<string, number>();       // `${id}:${token}` -> job id
  const handlerSettled = new Set<string>();
  const durableSettled = new Set<string>();
  let handlersRunning = 0;
  worker.on('jobSettled', (s) => { r.durable[s.outcome]++; durableSettled.add(`${s.id}:${s.lockToken}`); });
  const unsettled = () => [...claims.keys()].filter(k => !handlerSettled.has(k) || !durableSettled.has(k));

  let stopReason: DrainOutcome | null = null;
  // stop() only ends claiming; running work finishes normally (max_jobs, empty, budget, halt).
  const requestStop = (reason: DrainOutcome) => { stopReason ??= reason; worker.stop(); };
  tracker.onExhausted(() => { r.budget.exhausted = true; requestStop('budget_exhausted'); });
  const deferUnavailable = () => {
    requestStop('chat_unavailable');
    return new RateLeaseUnavailableError('jobs-drain:chat-unavailable', 1, 1, 60_000);
  };
  // --max-model-attempts: every chat provider attempt of this run (extractor
  // retries, error attempts, gateway fallbacks) is admitted here BEFORE its
  // transport call; the guarded gateway sends each with SDK retries off.
  // Admitting the last one stops claiming at once (that call still settles);
  // any later attempt is refused and its claim is handed back un-burned.
  const attemptRefusals = new Set<unknown>();
  const refuse = (message: string) => {
    const refusal = new Error(`jobs drain: ${message}; no provider call was made`);
    attemptRefusals.add(refusal);
    return refusal;
  };
  const admitModelAttempt = async (call: AIInvocation): Promise<void> => {
    const a = r.model_attempts;
    if (!a || call.kind !== 'chat') return;
    // Negative guard: a capped run is pinned to native Google; a chat invocation
    // of any other provider family is refused (not counted) and stops the run.
    if (!capSupported(call.model)) {
      if (!r.blockers.includes('model_provider_mismatch')) r.blockers = [...r.blockers, 'model_provider_mismatch'];
      requestStop('blocked');
      throw refuse('chat invocation outside the pinned native Google family');
    }
    if (a.attempted >= a.max) throw refuse('--max-model-attempts reached');
    if (++a.attempted >= a.max) {
      a.exhausted = true;
      requestStop('model_attempts_exhausted');
    }
  };
  const refusedAttempt = (error: unknown) => attemptRefusals.has(error)
    || (error instanceof FactsExtractionError && attemptRefusals.has(error.cause));

  worker.register(o.name, async (job) => {
    const token = worker.inFlightClaims.find(c => c.id === job.id)?.lockToken ?? 'unknown';
    const key = `${job.id}:${token}`;
    claims.set(key, job.id);
    handlersRunning++;
    try {
      // A claim that lands after a stop decision is handed back un-run: the
      // worker's lease-full path releases it without burning an attempt.
      if (stopReason !== null || r.attempted >= o.maxJobs) {
        r.released_unrun++;
        throw new RateLeaseUnavailableError('jobs-drain:bounded', 1, 1, 1_000);
      }
      r.attempted++;
      try {
        const result = await inner(job);
        // A keyless handler calmly "completes" work it could not do; for this
        // dedicated consumer that would lose the extraction. Defer instead.
        if (result && typeof result === 'object' && factsAbsorbUnavailable(result as never)) throw deferUnavailable();
        r.handler_returned++;
        if (result && typeof result === 'object') {
          const out = result as Record<string, unknown>;
          const code = typeof out.skipped === 'string' ? out.skipped : typeof out.skipped_reason === 'string' ? out.skipped_reason : null;
          if (code) bump(r.skipped, SAFE_CODE.test(code) ? code : 'other');
          if (typeof out.inserted === 'number') r.facts_inserted += out.inserted;
        }
        return result;
      } catch (error) {
        r.handler_threw++;
        if (error instanceof FactsExtractionError && error.reason === 'chat_unavailable') throw deferUnavailable();
        if (error instanceof JobDeferredError && error.reason === 'no_key') throw deferUnavailable();
        if (refusedAttempt(error)) throw new RateLeaseUnavailableError('jobs-drain:model-attempts', 1, 1, 60_000);
        if (error instanceof RateLeaseUnavailableError) {
          if (error.key.startsWith('global-llm-halt')) requestStop('provider_halt');
        } else {
          const name = error instanceof Error ? error.name : 'Error';
          bump(r.failures, SAFE_NAME.test(name) ? name : 'Error');
        }
        throw error;
      }
    } finally {
      handlersRunning--;
      handlerSettled.add(key);
      // The final permitted attempt stops claiming only AFTER its handler has
      // settled (concurrency 1: nothing else can be claimed meanwhile), so
      // max_jobs never cuts the last handler short via the worker's grace.
      if (r.attempted >= o.maxJobs) requestStop('max_jobs');
    }
  });
  const names = worker.registeredNames;
  if (names.length !== 1 || names[0] !== o.name) throw new Error('jobs drain invariant: exactly one handler must be registered');

  // Independent empty-queue observation: ends the run when this name has no
  // waiting rows and nothing is running — including when another consumer
  // emptied the queue before this worker claimed anything.
  let observing = false;
  const idle = () => handlersRunning === 0 && worker.inFlightClaims.length === 0;
  const observer = setInterval(() => {
    if (observing || stopReason !== null || !idle()) return;
    observing = true;
    readCounts(engine, o).then((c) => {
      if (c.waiting === 0 && idle()) requestStop(c.delayed > 0 ? 'delayed_only' : 'empty');
    }).catch(() => { /* observation is best-effort; the wall clock still bounds the run */ })
      .finally(() => { observing = false; });
  }, deps.pollMs ?? 1_000);

  let startSettled = false;
  let startError: unknown = null;
  const started = withBudgetTracker(tracker, () => (r.model_attempts
    ? withAIInvocationPreflight(admitModelAttempt, () => worker.start())
    : worker.start()));
  const done = started.then(() => { startSettled = true; }, (error) => { startSettled = true; startError = error; });
  deps.onStarted?.(done);
  const deadlineAt = startedAt + o.maxRuntimeMs;
  // Completion = the worker loop ended AND every claim it took is settled.
  const complete = () => startSettled && unsettled().length === 0;
  const waitUntil = async (pred: () => boolean, until: number) => {
    while (!pred() && Date.now() < until) await new Promise(res => setTimeout(res, 20));
    return pred();
  };
  try {
    if (!(await waitUntil(complete, deadlineAt))) {
      // Wall clock: stop claiming AND abort running work through the job's
      // own signal; the worker releases the claim without burning an attempt
      // (and never completes it, even if the handler returns anyway).
      if (stopReason === null || unsettled().length > 0) stopReason = 'time_limit';
      worker.releaseInFlight();
      await waitUntil(complete, Date.now() + (deps.graceMs ?? 30_000));
    }
  } finally {
    clearInterval(observer);
  }

  // Still unsettled after the grace: a handler ignored the abort (or was
  // force-evicted while running). Token-release its claim so the row is not
  // stranded, and report the domain stop as UNCONFIRMED — it may still act
  // until this process exits.
  const open = unsettled();
  if (open.length > 0) {
    r.domain_stop_confirmed = false;
    r.budget.final = false;
    for (const key of open) {
      r.in_flight_unconfirmed++;
      const [id, ...rest] = key.split(':');
      await queue.releaseLeaseFullJob(Number(id), rest.join(':'), 'jobs drain: handler did not settle after release abort; claim released', 60_000);
    }
  }
  r.budget.spent_usd = Math.round(tracker.totalSpent * 1e6) / 1e6;

  if (startError !== null) {
    const { OperationError } = await import('../core/ops/contract.ts');
    if (startError instanceof OperationError && startError.code === 'permission_denied') {
      r.blockers = [...r.blockers, 'unreviewed_jobs'];
      return finish('blocked');
    }
    bump(r.failures, startError instanceof Error && SAFE_NAME.test(startError.name) ? startError.name : 'Error');
    return finish('error');
  }
  if (open.length > 0) return finish('time_limit');
  return finish(stopReason ?? 'interrupted');
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const CLEAN_OUTCOMES: ReadonlySet<DrainOutcome> = new Set(['empty', 'delayed_only', 'max_jobs']);

function renderHuman(v: DrainPreview | DrainReport): string {
  const lines = [`gbrain jobs drain ${v.name} (${v.mode})`];
  if (v.mode === 'preview') {
    lines.push(`  waiting=${v.counts.waiting} delayed_due=${v.counts.delayed_due} delayed_future=${v.counts.delayed_future} active=${v.counts.active} active_expired=${v.counts.active_expired} oldest_waiting=${v.oldest_waiting_age_seconds ?? '-'}s`);
    lines.push(`  extraction=${v.extraction_enabled ? 'enabled' : 'DISABLED'} model=${v.model_override ?? 'configured'} chat=${v.chat} embedding=${v.embedding} pricing: chat=${v.pricing.chat} embedding=${v.pricing.embedding}`);
    lines.push(`  would promote at most ${v.would_promote_at_most}, attempt at most ${v.would_attempt_at_most}`);
    lines.push(v.blockers.length ? `  blocked: ${v.blockers.join(', ')}` : '  Re-run with --yes to execute.');
  } else {
    lines.push(`  outcome=${v.outcome} promoted=${v.promoted} attempted=${v.attempted} handler_returned=${v.handler_returned} handler_threw=${v.handler_threw} extraction=${v.extraction_enabled ? 'enabled' : 'DISABLED'} model=${v.model_override ?? 'configured'}`);
    lines.push(`  durable: ${Object.entries(v.durable).map(([k, n]) => `${k}=${n}`).join(' ')}`);
    if (!v.domain_stop_confirmed) lines.push(`  WARNING: ${v.in_flight_unconfirmed} handler(s) ignored the release abort; claim released, domain stop unconfirmed until process exit`);
    lines.push(`  remaining: waiting=${v.remaining.waiting} delayed_due=${v.remaining.delayed_due} delayed_future=${v.remaining.delayed_future} active=${v.remaining.active}`);
    lines.push(`  budget: spent~$${v.budget.spent_usd} of $${v.budget.max_usd}${v.budget.exhausted ? ' (exhausted)' : ''}${v.budget.final ? '' : ' (not final)'}`);
    if (v.model_attempts) lines.push(`  model attempts admitted (upper bound, not confirmed requests): ${v.model_attempts.attempted} of ${v.model_attempts.max}${v.model_attempts.exhausted ? ' (exhausted)' : ''}`);
    if (v.blockers.length) lines.push(`  blocked: ${v.blockers.join(', ')}`);
  }
  if (v.job_ids) lines.push(`  selected job ids: ${v.job_ids.join(',')}`);
  if (v.models) lines.push(`  models: chat=${v.models.chat ?? 'unconfirmed'} embedding=${v.models.embedding ?? 'unconfirmed'}`);
  return lines.join('\n') + '\n';
}

/**
 * CLI entry. Arguments are validated before either connector runs. Preview
 * uses the probe-only connection (no migrations, no gateway refresh); only an
 * unblocked `--yes` run reconnects with the ordinary worker boot.
 */
export async function runJobsDrainCli(
  args: string[],
  connect: { probe: () => Promise<BrainEngine>; full: () => Promise<BrainEngine>; owned?: boolean },
  deps: DrainDeps = {},
): Promise<void> {
  const { setCliExitVerdict, writeStdoutFinal } = await import('../core/cli-force-exit.ts');
  if (args.includes('--help') || args.includes('-h')) { await writeStdoutFinal(DRAIN_HELP); return; }
  let o: DrainOptions;
  try { o = parseDrainArgs(args); }
  catch (error) {
    if (!(error instanceof DrainUsageError)) throw error;
    console.error(`gbrain jobs drain: ${error.message}`);
    console.error('Run: gbrain jobs drain --help');
    setCliExitVerdict(2);
    return;
  }
  const owned = connect.owned !== false;
  const emit = (v: DrainPreview | DrainReport) => writeStdoutFinal(o.json ? `${JSON.stringify(v)}\n` : renderHuman(v));
  let engine: BrainEngine | undefined = await connect.probe();
  try {
    const preview = await previewDrain(engine, o);
    if (!o.execute || preview.blockers.length > 0) {
      if (o.execute) {
        const blocked = await runDrain(engine, o, deps);
        await emit(blocked);
        setCliExitVerdict(1);
      } else {
        await emit(preview);
      }
      return;
    }
    if (owned) { await engine.disconnect(); engine = undefined; engine = await connect.full(); }
    // Worker lifecycle lines go to stderr so --json stdout stays one document.
    const log = console.log;
    if (o.json) console.log = (...a: unknown[]) => console.error(...a);
    let report: DrainPreview | DrainReport;
    try { report = await runDrain(engine, o, deps); } finally { console.log = log; }
    await emit(report);
    if (report.mode === 'execute' && !CLEAN_OUTCOMES.has(report.outcome)) setCliExitVerdict(1);
  } finally {
    if (owned && engine) await engine.disconnect();
  }
}
