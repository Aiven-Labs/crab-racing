const { test, expect } = require('@playwright/test');
const { pointAtServer, useShortTrack, waitForCanvas, driveToFinish } = require('./helpers');

// Regression coverage for the "menu does nothing when clicked" class of bug
// (a stray merge-conflict marker in game.html once broke every click handler
// by throwing a SyntaxError at script-parse time).
test('solo race: menu launches, race can be won, score is submitted', async ({ page }) => {
  const pageErrors = [];
  page.on('pageerror', err => pageErrors.push(err));

  await page.goto('/game.html');
  await pointAtServer(page);
  await useShortTrack(page);

  await expect(page.locator('#lobby')).not.toHaveClass(/hidden/);
  await page.click('#btn-solo');

  await expect(page.locator('#lobby')).toHaveClass(/hidden/);
  await waitForCanvas(page);

  const leaderboardPost = page.waitForResponse(
    res => res.url().includes('/leaderboard') && res.request().method() === 'POST'
  );
  await driveToFinish(page);
  const res = await leaderboardPost;
  expect(res.ok()).toBeTruthy();

  expect(pageErrors, `unexpected page errors: ${pageErrors.map(e => e.message).join('; ')}`).toHaveLength(0);
});
