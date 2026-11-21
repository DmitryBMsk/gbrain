// Installed as src/core/ops/lumenix-evidence.ts next to evidence-operation.ts.
// Imports resolve only inside the hosted gbrain checkout.
import type { Operation, OperationContext } from './contract.ts';
import { searchOperations } from './search.ts';
import { createEvidenceOperation, runContainer } from './evidence-operation.ts';

async function searchBrain(ctx: OperationContext, question: string): Promise<unknown> {
  const search = searchOperations.find((op) => op.name === 'search');
  if (!search) return null;
  return search.handler(ctx, { query: question, limit: 4, snippet_chars: 400 });
}

export const evidenceOperations: Operation[] = [
  createEvidenceOperation(searchBrain, runContainer) as Operation,
];
