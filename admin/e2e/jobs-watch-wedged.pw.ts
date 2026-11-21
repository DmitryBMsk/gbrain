import { test, expect } from './fixtures';
import { openJobs, healthRegion, PAYLOAD_MARKER } from './jobs-watch-helpers';

// Real embedded server + PGLite seeded (server.ts) with four waiting jobs
// created 1234 minutes ago: no active row, no completion, no worker evidence.
test.use({ brainSeed: 'jobs-watch-wedged' });

test('waiting work older than the threshold with stalled=0 and no detected worker is flagged', async ({ page, brain }) => {
  await openJobs(page, brain);
  const alert = page.getByRole('alert');
  await expect(alert).toContainText('Waiting jobs are not being processed');
  await expect(alert).toContainText('4 waiting in queue default');
  await expect(alert).toContainText(/oldest 123[345] min/);
  await expect(alert).toContainText('threshold 15 min');
  // The legacy queue panel still reads stalled=0 — the alert is what tells the truth.
  await expect(page.getByText(/stalled=\s*0/)).toBeVisible();
  await expect(alert).toContainText('Worker: not detected');
  await expect(alert).not.toContainText('missing');
  const region = healthRegion(page);
  await expect(region).toContainText('worker: not detected');
  await expect(region).toContainText('does not prove none is running');
  await expect(region).toContainText('supervisor pidfile: not detected · supervisor lock: not detected · active job lease: not detected');
  await expect(region).toContainText(/oldest waiting: 123[345] min/);
  await expect(region).toContainText('last completion: no completion record retained');
  await expect(region).toContainText('active now: 0');
  await expect(region).toContainText('scope: queue default');
  await expect(page.locator('body')).not.toContainText(PAYLOAD_MARKER);
});

test('the admin API carries the backend health block, without payloads, only to the owner', async ({ page, brain }) => {
  const anonymous = await fetch(`${brain.url}/admin/api/jobs/watch`);
  expect(anonymous.status).toBe(401);
  expect(await anonymous.text()).not.toContain('wedged');

  await page.goto(await brain.loginLink());
  const response = await page.request.get(`${brain.url}/admin/api/jobs/watch`);
  expect(response.ok()).toBe(true);
  const body = await response.json();
  expect(body.queue_health).toEqual({ waiting: 4, active: 0, stalled: 0 });
  expect(body.health).toMatchObject({ schema_version: 1, queue: 'default', wedged: true, wedge_threshold_minutes: 15,
    waiting: 4, active_healthy: 0, last_completed_at: null, worker: { status: 'not_detected', detected_via: null,
      probes: { supervisor_pidfile: 'not_detected', supervisor_db_lock: 'not_detected', active_lease: 'not_detected' } } });
  expect(JSON.stringify(body)).not.toContain(PAYLOAD_MARKER);
});
