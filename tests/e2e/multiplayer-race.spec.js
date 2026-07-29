const { test, expect } = require('@playwright/test');
const { pointAtServer, useShortTrack, waitForCanvas, driveToFinish } = require('./helpers');

test('multiplayer: two players join a room, race, and return to lobby', async ({ browser }) => {
  const roomId = `e2e-${test.info().workerIndex}-${Date.now()}`;

  const hostCtx = await browser.newContext();
  const guestCtx = await browser.newContext();
  const host = await hostCtx.newPage();
  const guest = await guestCtx.newPage();

  try {
    await host.goto('/');
    await pointAtServer(host);
    await useShortTrack(host); // room settings are fixed by the host on join
    await host.fill('#player-name', 'Host');
    await host.fill('#room-code', roomId);
    await host.click('#btn-multi');
    await expect(host.locator('#lobby-status')).toContainText(/Lobby/i, { timeout: 10_000 });

    await guest.goto('/');
    await pointAtServer(guest);
    await guest.fill('#player-name', 'Guest');
    await guest.fill('#room-code', roomId);
    await guest.click('#btn-multi');
    await expect(guest.locator('#lobby-status')).toContainText(/Lobby/i, { timeout: 10_000 });

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

    // Race over → server resets the room and both clients see the lobby again.
    await expect(host.locator('#lobby')).not.toHaveClass(/hidden/, { timeout: 15_000 });
    await expect(guest.locator('#lobby')).not.toHaveClass(/hidden/, { timeout: 15_000 });
  } finally {
    await hostCtx.close();
    await guestCtx.close();
  }
});
