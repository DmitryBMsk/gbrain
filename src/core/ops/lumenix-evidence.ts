// Installed as src/core/ops/lumenix-evidence.ts next to evidence-operation.ts.
// Imports resolve only inside the hosted gbrain checkout.
import type { Operation, OperationContext } from './contract.ts';
import { searchOperations } from './search.ts';
import { fenceTranscriptText, unquoteTranscriptText } from '../search/transcript-quote.ts';
import { createEvidenceOperation, runContainer } from './evidence-operation.ts';

export async function searchBrain(ctx: OperationContext, question: string): Promise<unknown> {
  const search = searchOperations.find((op) => op.name === 'search');
  if (!search) return null;
  const raw = await search.handler(ctx, { query: question, limit: 4, snippet_chars: 400 });
  // 0.60 fences remote rows' title/chunk_text; the judge gets the plain text it got on 0.58 and treats it as data.
  const rows = Array.isArray(raw) ? raw : (raw as { results?: unknown })?.results;
  if (Array.isArray(rows)) {
    for (const row of rows as Array<Record<string, unknown>>) {
      if (typeof row?.slug !== 'string') continue;
      for (const key of ['title', 'chunk_text']) {
        if (typeof row[key] === 'string') row[key] = unquoteTranscriptText(row[key] as string, row.slug);
      }
    }
  }
  return raw;
}

/**
 * The judge echoes gbrain titles and snippets verbatim (ref `gbrain:<slug>`). 0.60 fences page text for remote
 * readers, so fence those fields again on the way out; any caller that is not strictly local counts as remote.
 */
export function fenceGbrainEvidence<T>(result: T, remote: boolean): T {
  const evidence = remote && result && typeof result === 'object' ? (result as { evidence?: unknown }).evidence : null;
  if (!Array.isArray(evidence)) return result;
  for (const item of evidence as Array<Record<string, unknown>>) {
    const ref = typeof item?.ref === 'string' ? item.ref : '';
    if (item?.backend !== 'gbrain' || !ref.startsWith('gbrain:')) continue;
    for (const key of ['title', 'highlight']) {
      if (typeof item[key] === 'string' && item[key]) item[key] = fenceTranscriptText(item[key] as string, ref.slice('gbrain:'.length));
    }
  }
  return result;
}

// The op registry hands every handler a full OperationContext; BrainSearch only declares the dryRun it reads.
const packEvidence = createEvidenceOperation((ctx, q) => searchBrain(ctx as OperationContext, q), runContainer) as Operation;

export const evidenceOperations: Operation[] = [
  { ...packEvidence, handler: async (ctx, p) => fenceGbrainEvidence(await packEvidence.handler(ctx, p), ctx.remote !== false) },
];
