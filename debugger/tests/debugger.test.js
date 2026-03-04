import { test, expect } from '@playwright/test';

test.describe('y-cinder Debugger', () => {

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
});
