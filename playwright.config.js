// @ts-check
const { defineConfig } = require('@playwright/test');

// The docker-compose nginx container serves the static game at :8080; the
// Node/WS server lives on :3000 behind no proxy, so tests must point the
// lobby's ws-url field at it directly (see docker-compose.yml's header comment).
const WEB_URL = 'http://localhost:8080';
const WS_HTTP_URL = 'http://localhost:3000';

module.exports = defineConfig({
  testDir: './tests/e2e',
  timeout: 30_000,
  fullyParallel: false, // shares one docker-compose stack; avoid room-id / port races
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: WEB_URL,
    // Reuse the Chrome already installed on the machine instead of downloading
    // a separate Playwright Chromium build.
    channel: 'chrome',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: {
    command: 'docker compose up --build',
    // Gate readiness on the Node/WS server (not nginx) — it's the one that
    // waits on Kafka + Valkey at startup, so it's the slow half of the stack.
    url: `${WS_HTTP_URL}/rooms`,
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});

module.exports.WS_HTTP_URL = WS_HTTP_URL;
