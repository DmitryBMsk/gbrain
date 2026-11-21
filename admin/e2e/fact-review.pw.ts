import type { Page, Request } from '@playwright/test';
import { test, expect } from './fixtures';
import { FR } from './fact-review-seed';

test.use({ brainSeed: 'fact-review' });

const REVIEW = '/admin/api/facts-absorb/review';
const PRIVATE_TEXT = [FR.activeFact, FR.supersededFact, FR.expiredFact, FR.changedFact, FR.exactBody, FR.changedBody];
const NEVER_SHOWN = [FR.foreignFact, FR.unlistedFact, FR.sessionMarker, FR.authorityMarker, FR.waitingPayload, FR.otherPayload];
const ALL_IDS = `${FR.exactJob},${FR.changedJob},${FR.waitingJob},${FR.otherJob}`;

async function openReview(page: Page, brain: { url: string; loginLink: () => Promise<string> }) {
  await page.goto(await brain.loginLink());
  await page.goto(`${brain.url}/admin/#fact-review`);
  await expect(page.getByRole('heading', { name: 'Fact Review', exact: true })).toBeVisible();
}

function reviewRequests(page: Page): Request[] {
  const seen: Request[] = [];
  page.on('request', request => { if (new URL(request.url()).pathname === REVIEW) seen.push(request); });
  return seen;
}

test('anonymous and ordinary bearer callers are refused, even with bearers that work on /mcp', async ({ brain }) => {
  const url = `${brain.url}${REVIEW}?job_ids=${FR.exactJob}`;
  const anonymous = await fetch(url);
  expect(anonymous.status).toBe(401);
  const anonymousBody = await anonymous.text();
  for (const text of PRIVATE_TEXT) expect(anonymousBody).not.toContain(text);

  for (const token of [FR.oauthAdminToken, FR.legacyToken]) {
    // Positive control: the bearer is a live, admin-capable MCP credential.
    const mcp = await fetch(`${brain.url}/mcp`, { method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) });
    expect(mcp.status).toBe(200);
    const refused = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    expect(refused.status).toBe(401);
    const body = await refused.text();
    for (const text of PRIVATE_TEXT) expect(body).not.toContain(text);
  }
});

test('owner loads only on request and sees receipt facts beside the extraction-time source, with changed and refused jobs marked', async ({ page, brain }) => {
  const logs: string[] = [];
  page.on('console', message => logs.push(message.text()));
  const requests = reviewRequests(page);
  await openReview(page, brain);
  await page.waitForTimeout(500);
  expect(requests).toHaveLength(0); // no automatic fetch

  await page.getByLabel('Completed job IDs', { exact: true }).fill(`${FR.exactJob}, ${FR.changedJob}, ${FR.waitingJob}, ${FR.otherJob}`);
  const response = page.waitForResponse(r => new URL(r.url()).pathname === REVIEW);
  await page.getByRole('button', { name: 'Load', exact: true }).click();
  expect((await response).headers()['cache-control']).toBe('private, no-store');
  expect(requests.map(r => new URL(r.url()).searchParams.get('job_ids'))).toEqual([ALL_IDS]);

  const exact = page.getByRole('region', { name: `Job ${FR.exactJob}`, exact: true });
  const facts = exact.getByRole('list', { name: 'Facts' }).getByRole('listitem');
  await expect(facts).toHaveCount(3);
  await expect(facts.nth(0)).toContainText(FR.activeFact); // markup is shown as text
  await expect(facts.nth(0)).toContainText('active');
  await expect(facts.nth(1)).toContainText(FR.supersededFact);
  await expect(facts.nth(1)).toContainText(/superseded by #\d+/);
  await expect(facts.nth(2)).toContainText(FR.expiredFact);
  await expect(facts.nth(2)).toContainText('expired');
  await expect(exact).toContainText('Receipt: 4 fact IDs (inserted 3, duplicate 1, superseded 0)');
  await expect(exact.getByText(/^Receipt IDs not found in source default: #\d+$/)).toBeVisible();
  const exactSource = exact.getByRole('region', { name: 'Source text', exact: true });
  await expect(exactSource.getByRole('status')).toHaveText('Current page, still at the extraction revision: the exact text the extraction read');
  await expect(exactSource).toContainText('Persistence request input is identical to this text.');
  await expect(exactSource.getByRole('region', { name: 'Request input (provenance only)' })).toHaveCount(0);
  await expect(exactSource.getByRole('heading', { name: new RegExp(`^${FR.exactTitle}`) })).toBeVisible();
  await expect(exactSource).toContainText(FR.exactBody);

  const changed = page.getByRole('region', { name: `Job ${FR.changedJob}`, exact: true });
  await expect(changed.getByRole('listitem')).toContainText(FR.changedFact);
  const changedSource = changed.getByRole('region', { name: 'Source text', exact: true });
  await expect(changedSource.getByRole('status')).toHaveText('Current page, CHANGED since extraction: this is NOT the text the facts were extracted from');
  await expect(changedSource).toContainText(FR.changedBody);
  await expect(changedSource).toContainText('persistence request — (none recorded)');

  await expect(page.getByRole('region', { name: `Job ${FR.waitingJob}`, exact: true })).toHaveText(`Job ${FR.waitingJob}Not completed (status: waiting); nothing is shown.`);
  await expect(page.getByRole('region', { name: `Job ${FR.otherJob}`, exact: true })).toHaveText(`Job ${FR.otherJob}Not a facts-absorb job on the default queue; nothing is shown.`);

  const text = await page.locator('body').innerText();
  for (const marker of NEVER_SHOWN) expect(text).not.toContain(marker);
  await expect(page.locator('img[src="x"]')).toHaveCount(0);
  expect(await page.evaluate(() => (window as unknown as { __factReviewXss?: number }).__factReviewXss)).toBeUndefined();

  // Nothing private is persisted client-side or logged.
  const stored = await page.evaluate(() => JSON.stringify([{ ...localStorage }, { ...sessionStorage }, document.cookie]));
  for (const value of [...PRIVATE_TEXT, ...NEVER_SHOWN]) {
    expect(stored).not.toContain(value);
    for (const line of logs) expect(line).not.toContain(value);
  }
  expect(requests).toHaveLength(1);
});

test('the text the extraction read stays primary; divergent request input is provenance only, and unverified without revision text', async ({ page, brain }) => {
  await openReview(page, brain);
  await page.getByLabel('Completed job IDs', { exact: true }).fill(`${FR.divergentJob}, ${FR.unverifiedJob}`);
  await page.getByRole('button', { name: 'Load', exact: true }).click();

  const divergent = page.getByRole('region', { name: `Job ${FR.divergentJob}`, exact: true }).getByRole('region', { name: 'Source text', exact: true });
  await expect(divergent.getByRole('status')).toHaveText('Current page, still at the extraction revision: the exact text the extraction read');
  await expect(divergent).toContainText('Persistence request input DIFFERS from the text the extraction read; it is shown below for provenance only and is not the extraction source.');
  const provenance = divergent.getByRole('region', { name: 'Request input (provenance only)', exact: true });
  await expect(provenance).toContainText('provenance only, NOT the extraction source');
  await expect(provenance.locator('pre')).toHaveText(FR.divergentInput);
  // The primary block (first <pre>, outside the provenance region) carries the revision-only tail.
  await expect(divergent.locator('pre').first()).toContainText(FR.divergentPublishedTail);
  await expect(provenance).not.toContainText(FR.divergentPublishedTail);

  const unverified = page.getByRole('region', { name: `Job ${FR.unverifiedJob}`, exact: true }).getByRole('region', { name: 'Source text', exact: true });
  await expect(unverified.getByRole('status')).toHaveText('UNVERIFIED writer input: the page text at the extraction revision is no longer retained, so this persistence request input may differ from what the extraction read');
  await expect(unverified).toContainText('Not compared: no revision-fenced page text is retained.');
  await expect(unverified.locator('pre')).toHaveText(FR.unverifiedInput);
  await expect(unverified).not.toContainText(FR.unverifiedCurrent);
});

test('malformed IDs are refused before any request; the server refuses repeated or extra parameters', async ({ page, brain }) => {
  const requests = reviewRequests(page);
  await openReview(page, brain);
  for (const bad of [`${FR.exactJob};${FR.changedJob}`, `${FR.exactJob},${FR.exactJob}`, '1,2,3,4,5,6,7,8,9', '0', '']) {
    await page.getByLabel('Completed job IDs', { exact: true }).fill(bad);
    await page.getByRole('button', { name: 'Load', exact: true }).click();
    await expect(page.getByRole('alert')).toHaveText('Enter 1–8 unique positive job IDs separated by commas.');
  }
  expect(requests).toHaveLength(0);

  for (const query of [`job_ids=${FR.exactJob}&job_ids=${FR.changedJob}`, `job_ids=${FR.exactJob}&slug=x`, `job_ids[a]=${FR.exactJob}`, `job_ids=${FR.exactJob}%3BSELECT`]) {
    const refused = await page.request.get(`${brain.url}${REVIEW}?${query}`);
    expect(refused.status()).toBe(400);
    expect(refused.headers()['cache-control']).toBe('private, no-store');
    expect(await refused.json()).toEqual({ error: 'invalid_job_ids' });
  }
  const accepted = await page.request.get(`${brain.url}${REVIEW}?job_ids=${FR.waitingJob}`);
  expect(accepted.status()).toBe(200);
  expect((await accepted.json()).jobs).toEqual([{ job_id: FR.waitingJob, status: 'not_completed', job_status: 'waiting' }]);
});
