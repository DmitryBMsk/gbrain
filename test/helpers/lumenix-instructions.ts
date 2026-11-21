// Lumenix fork: the suggest_skill / pack_evidence clauses (src/mcp/instructions.ts) sit outside upstream's
// recorded instruction budgets, so those tests pin upstream's own text and add exactly what our clauses add.
import { buildMcpInstructions, type InstructionContent } from '../../src/mcp/instructions.ts';

const LUMENIX_OPS = new Set(['suggest_skill', 'pack_evidence']);
type Callable = (name: string) => boolean;
const ALL: Callable = () => true;

/** Upstream's rendering: the same callable set without our ops (byte-identical to buildMcpInstructions() for ALL). */
export function upstreamInstructions(callable: Callable = ALL, content?: InstructionContent): string {
  return buildMcpInstructions({ ...content, tools: { callable: n => callable(n) && !LUMENIX_OPS.has(n) } });
}

/** Bytes our clauses add to this callable set's rendering (the text is ASCII, so also characters). */
export function lumenixClauseBytes(callable: Callable = ALL): number {
  return Buffer.byteLength(buildMcpInstructions({ tools: { callable } })) - Buffer.byteLength(upstreamInstructions(callable));
}
