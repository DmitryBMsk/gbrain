import type { Page } from '@playwright/test';
import { test, expect } from './fixtures';
import { openJobs, healthRegion } from './jobs-watch-helpers';

// Jobs Watch health states. The empty case runs against the real embedded
// server + PGLite; the remaining states route fixture responses into the same
// compiled page. The seeded stuck-backlog journey is jobs-watch-wedged.pw.ts.
// Synthetic data only.

const baseSnapshot = {
  ts_ms: Date.parse('2026-01-15T12:00:00Z'),
  by_type: [{ name: 'synthetic-backlog', total: 3, completed: 2, failed: 0, dead: 0 }],
  queue_health: { waiting: 1, active: 1, stalled: 0 },
  lease_pressure_1h: 0,
  top_errors: [],
  budget_owners: [],
};

const healthyHealth = {
  schema_version: 1,
  observed_at: '2026-01-15T12:00:00.000Z',
  queue: 'default',
  wedged: false,
  wedge_threshold_minutes: 15,
  private_queue: false,
  waiting: 1,
  active_healthy: 1,
  oldest_waiting_minutes: 3,
  last_completed_at: '2026-01-15T11:58:00.000Z',
  minutes_since_completion: 2,
  worker: {
    status: 'running',
    detected_via: 'db_lock',
    probes: { supervisor_pidfile: 'not_detected', supervisor_db_lock: 'detected', active_lease: 'detected' },
  },
};

const wedgedProbeFailed = {
  ...healthyHealth,
  wedged: true,
  waiting: 4,
  active_healthy: 0,
  oldest_waiting_minutes: 1234,
  last_completed_at: null,
  minutes_since_completion: null,
  worker: {
    status: 'unknown',
    detected_via: null,
    probes: { supervisor_pidfile: 'not_detected', supervisor_db_lock: 'failed', active_lease: 'not_detected' },
  },
};

async function routeWatch(page: Page, body: unknown, status = 200) {
  await page.route('**/admin/api/jobs/watch', route =>
    route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) }));
}

test.describe('empty and fixture-routed states', () => {
  test('empty real queue shows no alert and an explicit idle state', async ({ page, brain }) => {
    await openJobs(page, brain);
    const region = healthRegion(page);
    await expect(region).toContainText('no waiting jobs');
    await expect(region).toContainText('worker: not detected');
    await expect(region).not.toContainText('missing');
    await expect(region).toContainText('last completion: no completion record retained');
    await expect(page.getByRole('alert')).toHaveCount(0);
  });

  test('healthy progress: worker running, recent completion, no alert', async ({ page, brain }) => {
    await routeWatch(page, { ...baseSnapshot, health: healthyHealth });
    await openJobs(page, brain);
    const region = healthRegion(page);
    await expect(region).toContainText('worker: running (supervisor lock)');
    await expect(region).toContainText('active now: 1');
    await expect(region).toContainText('last completion: 2 min ago');
    await expect(region).toContainText('processing');
    await expect(page.getByRole('alert')).toHaveCount(0);
  });

  test('legacy server without a health block is shown as unknown, never healthy', async ({ page, brain }) => {
    await routeWatch(page, baseSnapshot);
    await openJobs(page, brain);
    const region = healthRegion(page);
    await expect(region).toContainText('Health unknown');
    await expect(region).toContainText('worker: unknown');
    await expect(region).not.toContainText('processing');
    await expect(region).not.toContainText('worker: running');
  });

  test('malformed health fields are treated as unknown', async ({ page, brain }) => {
    await routeWatch(page, { ...baseSnapshot, health: { ...healthyHealth, wedged: 'no', worker: undefined } });
    await openJobs(page, brain);
    await expect(healthRegion(page)).toContainText('Health unknown');
    await expect(healthRegion(page)).not.toContainText('worker: running');
  });

  test('a running claim without matching probe evidence, or a legacy "missing" status, is not trusted', async ({ page, brain }) => {
    const unbacked = { ...healthyHealth, worker: { ...healthyHealth.worker,
      probes: { supervisor_pidfile: 'not_detected', supervisor_db_lock: 'not_detected', active_lease: 'not_detected' } } };
    await routeWatch(page, { ...baseSnapshot, health: unbacked });
    await openJobs(page, brain);
    await expect(healthRegion(page)).toContainText('Health unknown');
    await expect(healthRegion(page)).not.toContainText('worker: running');

    await page.unroute('**/admin/api/jobs/watch');
    await routeWatch(page, { ...baseSnapshot, health: { ...healthyHealth, worker: { status: 'missing', detected_via: null } } });
    await page.reload();
    await expect(healthRegion(page)).toContainText('Health unknown');
    await expect(healthRegion(page)).not.toContainText('missing');
  });

  test('a failed detection probe reads unknown while the backend wedge alert still shows', async ({ page, brain }) => {
    await routeWatch(page, { ...baseSnapshot, health: wedgedProbeFailed });
    await openJobs(page, brain);
    await expect(page.getByRole('alert')).toContainText('Waiting jobs are not being processed');
    await expect(page.getByRole('alert')).toContainText('Worker: unknown (detection failed: supervisor lock)');
    const region = healthRegion(page);
    await expect(region).toContainText('worker: unknown (detection failed: supervisor lock)');
    await expect(region).toContainText('supervisor lock: failed');
    await expect(region).not.toContainText('not detected (an idle');
  });

  test('server-side health failure is shown as unavailable while other panels stay', async ({ page, brain }) => {
    await routeWatch(page, { ...baseSnapshot, health: { error: 'unavailable' } });
    await openJobs(page, brain);
    await expect(healthRegion(page)).toContainText('Health unavailable');
    await expect(healthRegion(page)).toContainText('worker: unknown');
    await expect(page.getByText('synthetic-backlog')).toBeVisible();
  });

  test('fetch failure keeps the existing error view', async ({ page, brain }) => {
    await routeWatch(page, { error: 'synthetic failure' }, 500);
    await openJobs(page, brain);
    await expect(page.getByRole('heading', { name: 'Jobs Watch — error' })).toBeVisible();
    await expect(page.getByText('synthetic failure')).toBeVisible();
    await expect(healthRegion(page)).toHaveCount(0);
  });
});
