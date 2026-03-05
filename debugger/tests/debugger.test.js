import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';

test.describe('y-cinder Debugger', () => {

  test.beforeAll(async () => {
    // Wait for the emulator to be ready
    await new Promise(r => setTimeout(r, 10000));
    try {
        execSync('node populate.mjs');
    } catch (e) {
        console.error("Failed to populate db:", e);
    }
  });

  test('should load the debugger and display the environment panel', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('h1').filter({ hasText: 'y-cinder Debugger' })).toBeVisible();
    await expect(page.getByText('Use Local Emulator')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Load Document' })).toBeVisible();
  });

  test('should allow toggling emulator mode', async ({ page }) => {
    await page.goto('/');
    const emulatorCheckbox = page.getByLabel('Use Local Emulator');

    await expect(emulatorCheckbox).toBeChecked();

    await emulatorCheckbox.uncheck();
    await expect(page.getByText('API Key')).toBeVisible();
    await expect(page.getByText('App ID')).toBeVisible();
    await expect(page.getByText('Auth Domain')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Sign In with Google' })).toBeVisible();

    await emulatorCheckbox.check();
    await expect(page.getByText('API Key')).not.toBeVisible();
  });

  test('should load document data, display panels, and allow update selection', async ({ page }) => {
    await page.goto('/');

    // Click Load Document
    await page.getByRole('button', { name: 'Load Document' }).click();

    // Wait for data to load
    await expect(page.getByRole('heading', { name: 'Base Document' })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('heading', { name: /^History/ })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('heading', { name: /^Updates/ })).toBeVisible({ timeout: 15000 });

    // Data populated by populate.mjs
    await expect(page.getByText('gs://test/snap.bin').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('client-B').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('client-A').first()).toBeVisible({ timeout: 15000 });

    // Check initial state (should be Deselect All as updates are selected by default)
    const deselectAllBtn = page.getByRole('button', { name: 'Deselect All' });
    await expect(deselectAllBtn).toBeVisible({ timeout: 15000 });

    const checkboxes = await page.locator('input[type="checkbox"]').all();

    // Verify updates checkboxes exist
    expect(checkboxes.length).toBeGreaterThan(1);

    // Test Deselect All
    await deselectAllBtn.click();

    // Checkboxes should be unchecked after "Deselect All" (except the first one, which is Emulator check)
    await expect(checkboxes[1]).not.toBeChecked();

    // Select All
    const selectAllBtn = page.getByRole('button', { name: 'Select All' });
    await expect(selectAllBtn).toBeVisible({ timeout: 15000 });
    await selectAllBtn.click();

    // Checkboxes should be checked again
    await expect(checkboxes[1]).toBeChecked();
  });
});
