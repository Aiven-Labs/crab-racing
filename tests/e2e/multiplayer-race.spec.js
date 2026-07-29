const { test, expect } = require('@playwright/test');
const { pointAtServer, useShortTrack, waitForCanvas, driveToFinish } = require('./helpers');

test('multiplayer: two players join a room, race, and return to lobby', async ({ browser }) => {
  const roomId = `e2e-${test.info().workerIndex}-${Date.now()}`;

  const hostCtx = await browser.newContext();
  const guestCtx = await browser.newContext();
  const host = await hostCtx.newPage();
  const guest = await guestCtx.newPage();

  try {
    await host.goto('/game.html');
    await pointAtServer(host);
    await useShortTrack(host); // room settings are fixed by the host on join
    await host.fill('#player-name', 'Host');
    await host.fill('#room-code', roomId);
    await host.click('#btn-multi');
    await expect(host.locator('#lobby-status')).not.toContainText('Connecting', { timeout: 10_000 });

    await guest.goto('/game.html');
    await pointAtServer(guest);
    await guest.fill('#player-name', 'Guest');
    await guest.fill('#room-code', roomId);
    await guest.click('#btn-multi');
    await expect(guest.locator('#lobby-status')).not.toContainText('Connecting', { timeout: 10_000 });

    // Only the host (room creator) sees Start Match, and only once 2+ players
    // are present.
    await expect(host.locator('#btn-start')).not.toHaveClass(/hidden/, { timeout: 10_000 });
    await host.click('#btn-start');

    // 3-2-1 countdown, then both clients get 'go' and the lobby overlay hides.
    await expect(host.locator('#lobby')).toHaveClass(/hidden/, { timeout: 10_000 });
    await expect(guest.locator('#lobby')).toHaveClass(/hidden/, { timeout: 10_000 });
    await waitForCanvas(host);
    await waitForCanvas(guest);

    // Race ends the instant every player finishes (no need to wait out the DNF
    // window) — drive both crabs home.
    await Promise.all([driveToFinish(host), driveToFinish(guest)]);

    // Race over → the results panel offers "Back to Lobby" (in-room overlay,
    // keeps the WS connection so the host can start another race).
    await host.click('#r-lobby');
    await guest.click('#r-lobby');
    await expect(host.locator('#lobby')).not.toHaveClass(/hidden/, { timeout: 10_000 });
    await expect(guest.locator('#lobby')).not.toHaveClass(/hidden/, { timeout: 10_000 });
  } finally {
    await hostCtx.close();
    await guestCtx.close();
  }
});
