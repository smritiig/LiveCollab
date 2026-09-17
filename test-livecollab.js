const { chromium } = require('playwright');

const NUM_USERS = 15; // adjust up/down to find the breaking point
const BASE_URL = 'http://localhost:5173';

async function createRoom(browser, username) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(`${BASE_URL}/`);
  await page.fill('.landing-input', username);
  await page.click('.landing-primary-button');
  await page.waitForURL('**/room/**');
  const roomId = new URL(page.url()).pathname.split('/room/')[1];
  return { page, context, roomId };
}

async function joinRoom(browser, username, roomId) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(`${BASE_URL}/`);
  await page.fill('.landing-input', username);
  const inputs = await page.locator('.landing-input').all();
  await inputs[1].fill(roomId);
  await page.click('.landing-secondary-button');
  await page.waitForURL('**/room/**');
  return { page, context };
}

(async () => {
  const browser = await chromium.launch({ headless: true }); // headless for speed at scale

  console.log(`Creating room and joining ${NUM_USERS} users...`);

  const host = await createRoom(browser, 'User0');
  console.log('Room created:', host.roomId);

  const joinStart = Date.now();
  const joiners = [];
  for (let i = 1; i < NUM_USERS; i++) {
    joiners.push(joinRoom(browser, `User${i}`, host.roomId));
  }
  const joinedUsers = await Promise.all(joiners);
  const joinElapsed = Date.now() - joinStart;
  console.log(`${NUM_USERS - 1} additional users joined in ${joinElapsed}ms`);

  const allPages = [host.page, ...joinedUsers.map(u => u.page)];

  // let all sockets settle
  await new Promise(r => setTimeout(r, 1500));

  // Host sends an edit, measure how long until it propagates to all others
  const testMessage = `Load test message ${Date.now()}`;
  const sendStart = Date.now();
  await host.page.fill('textarea', testMessage);

  // Poll each follower page until it sees the update or times out
  const results = await Promise.all(
    allPages.slice(1).map(async (page, idx) => {
      const timeoutMs = 5000;
      const pollInterval = 50;
      let waited = 0;
      while (waited < timeoutMs) {
        const content = await page.inputValue('textarea');
        if (content === testMessage) {
          return { user: idx + 1, latencyMs: Date.now() - sendStart, received: true };
        }
        await new Promise(r => setTimeout(r, pollInterval));
        waited += pollInterval;
      }
      return { user: idx + 1, latencyMs: null, received: false };
    })
  );

  const successCount = results.filter(r => r.received).length;
  const latencies = results.filter(r => r.received).map(r => r.latencyMs);
  const avgLatency = latencies.length
    ? (latencies.reduce((a, b) => a + b, 0) / latencies.length).toFixed(1)
    : null;
  const maxLatency = latencies.length ? Math.max(...latencies) : null;

  console.log('\n--- RESULTS ---');
  console.log(`Total users: ${NUM_USERS}`);
  console.log(`Received update: ${successCount}/${NUM_USERS - 1}`);
  console.log(`Avg latency: ${avgLatency}ms`);
  console.log(`Max latency: ${maxLatency}ms`);

  const failed = results.filter(r => !r.received);
  if (failed.length) {
    console.log(`Failed users: ${failed.map(f => f.user).join(', ')}`);
  }

  await browser.close();
})();