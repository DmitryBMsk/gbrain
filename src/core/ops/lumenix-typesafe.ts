// Installed as src/core/ops/lumenix-typesafe.ts in the hosted gbrain checkout.
import type { Operation } from './contract.ts';
import { execFile } from '../spawn.ts';

type Result = { status: string; suggestions: string[]; [key: string]: unknown };
type Runner = (input: string) => Promise<string>;
const unavailable = (): Result => ({ status: 'unavailable', suggestions: [], reason: 'selector_unavailable' });
let active = 0;

const runContainer: Runner = (input) => new Promise((resolve, reject) => {
  const child = execFile('/usr/bin/docker', [
    'exec', '-i', 'openclaw', 'python3',
    '/home/node/.openclaw/workspace/skills/typesafe-ai/scripts/typesafe-skill-suggestion.py',
  ], { timeout: 14000, maxBuffer: 100000, encoding: 'utf8' }, (err, stdout) => {
    if (err) reject(new Error('selector unavailable'));
    else resolve(stdout);
  });
  child.stdin?.on('error', () => {});
  child.stdin?.end(input);
});

export async function runSuggestion(p: Record<string, unknown>, runner: Runner = runContainer): Promise<Result> {
  if (p.dry_run === true) return { status: 'dry_run', suggestions: [], calls: 0 };
  if (p.sensitive !== false) return { status: 'skipped_sensitive', suggestions: [], calls: 0 };
  if (typeof p.request !== 'string' || !p.request.trim() || p.request.length > 4000 ||
      !Array.isArray(p.roster) || p.roster.length > 200) return unavailable();
  const input = JSON.stringify({ request: p.request, roster: p.roster, sensitive: false });
  if (Buffer.byteLength(input) > 300000 || active >= 2) return unavailable();
  active++;
  try {
    const result = JSON.parse(await runner(input));
    const names = new Set(p.roster.map((item: any) => item?.name));
    if (!['suggested', 'no_match', 'unavailable', 'skipped_sensitive', 'dry_run'].includes(result.status) ||
        !Array.isArray(result.suggestions) || result.suggestions.length > 1 ||
        result.suggestions.some((name: unknown) => typeof name !== 'string' || !names.has(name))) return unavailable();
    return result;
  } catch {
    return unavailable();
  } finally {
    active--;
  }
}

export const typesafeOperations: Operation[] = [{
  name: 'suggest_skill',
  description: 'TypeSafe skill suggestion (non-binding). Before a substantive task, send a sanitized task summary and the skills actually available to this agent. Returns at most one skill to read, or no_match/unavailable. Sends only supplied input to TypeSafe; never reads brain content or executes a skill. Explicit user choices and mandatory workflows take priority. Set sensitive=false only for sanitized input; otherwise skip external selection.',
  area: 'skills',
  scope: 'read',
  outputRedaction: 'no_stored_text', // returns only skill names from the caller's own roster
  params: {
    request: { type: 'string', required: true, description: 'Sanitized task summary, at most 4000 characters. No secrets, private documents or full conversation.' },
    roster: { type: 'array', items: { type: 'object' }, required: true, description: 'At most 200 available skills, each with required name and description (up to 500 chars). Optional excerpt (up to 700 chars) defaults to description. Use only this client\'s available catalog. Input errors return reason=invalid_input and calls=0; this is not an external API failure.' },
    sensitive: { type: 'boolean', required: true, description: 'False only after removing private data from request and roster. True skips external evaluation.' },
  },
  mutating: false,
  idempotent: true,
  annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true },
  handler: async (ctx, p) => runSuggestion({ ...p, dry_run: ctx.dryRun }),
}];
