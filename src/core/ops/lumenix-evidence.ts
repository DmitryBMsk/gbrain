// Installed as src/core/ops/lumenix-evidence.ts next to evidence-operation.ts.
// Imports resolve only inside the hosted gbrain checkout.
import type { Operation, OperationContext } from './contract.ts';
import { searchOperations } from './search.ts';
import { unquoteTranscriptText } from '../search/transcript-quote.ts';
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

export const evidenceOperations: Operation[] = [
  // The op registry hands every handler a full OperationContext; BrainSearch only declares the dryRun it reads.
  createEvidenceOperation((ctx, q) => searchBrain(ctx as OperationContext, q), runContainer) as Operation,
];
