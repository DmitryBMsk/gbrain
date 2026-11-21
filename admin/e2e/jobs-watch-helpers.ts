import type { Page } from '@playwright/test';
import { expect } from './fixtures';

export const PAYLOAD_MARKER = 'synthetic-payload-marker-0b7e';

export async function openJobs(page: Page, brain: { url: string; loginLink: () => Promise<string> }): Promise<void> {
  await page.goto(await brain.loginLink());
  await page.goto(`${brain.url}/admin/#jobs`);
  await expect(page.getByRole('heading', { name: /^Jobs Watch/ })).toBeVisible();
}

export function healthRegion(page: Page) {
  return page.getByRole('region', { name: 'Worker health' });
}
