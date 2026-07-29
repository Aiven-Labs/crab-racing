const WS_HTTP_URL = require('../../playwright.config.js').WS_HTTP_URL;

// nginx (the static host at :8080) doesn't proxy WebSocket/HTTP to the Node
// server, so every page under test must be told the server's real origin —
// see docker-compose.yml's header comment.
async function pointAtServer(page) {
  await page.fill('#ws-url', WS_HTTP_URL.replace('http://', 'ws://'));
}

// Shrinks the track so a race finishes in a handful of key presses instead of
// dozens — keeps the e2e run fast and avoids relying on obstacle RNG.
async function useShortTrack(page) {
  await page.fill('#cfg-lanes', '1');
  await page.fill('#cfg-length', '10');
}

async function waitForCanvas(page) {
  await page.waitForSelector('canvas', { state: 'attached' });
}

// The race outcome is drawn on a Phaser <canvas> — invisible to DOM queries —
// so we read Scene state directly instead of scraping rendered text.
async function isWon(page) {
  return page.evaluate(() => {
    const scene = window._phaserGame?.scene?.getScene('Race');
    return !!scene?.won;
  });
}

// Repeatedly presses forward, honoring the client's ~250ms per-move debounce
// (see RaceScene.move), until the crab crosses the finish line or we give up.
async function driveToFinish(page, { maxPresses = 60 } = {}) {
  for (let i = 0; i < maxPresses; i++) {
    if (await isWon(page)) return;
    await page.keyboard.press('ArrowRight');
    await page.waitForTimeout(300);
  }
  if (!(await isWon(page))) throw new Error(`Race did not finish after ${maxPresses} presses`);
}

module.exports = { pointAtServer, useShortTrack, waitForCanvas, isWon, driveToFinish };
