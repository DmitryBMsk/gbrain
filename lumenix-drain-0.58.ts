import { loadConfig, toEngineConfig } from "./src/core/config.ts";
import { configureGateway } from "./src/core/ai/gateway.ts";
import { buildGatewayConfig } from "./src/core/ai/build-gateway-config.ts";
import { createEngine } from "./src/core/engine-factory.ts";
import { connectWithRetry } from "./src/core/db.ts";
import { rebuildPendingPageProjections } from "./src/core/page-state/projections.ts";
const cfg = loadConfig(); if (!cfg) throw new Error("no config");
configureGateway(buildGatewayConfig(cfg));
const engine = await createEngine(toEngineConfig(cfg));
await connectWithRetry(engine, toEngineConfig(cfg), { noRetry: true });
let total = 0;
for (;;) { const r = await rebuildPendingPageProjections(engine, 100); total += r.rebuilt;
  if (r.rebuilt === 0 && r.superseded === 0) break; }
console.log(`DONE rebuilt=${total}`); process.exit(0);
