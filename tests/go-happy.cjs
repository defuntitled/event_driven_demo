const { chromium } = require(process.env.PLAYWRIGHT_MODULE || '/tmp/last-ticket-browser/node_modules/playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
(async () => {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'});
  try {
    const page = await browser.newPage({viewport: {width: 1440, height: 1000}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(process.env.DEMO_BASE_URL || 'http://localhost:18080');
    const buy = page.getByRole('button', {name: 'Купить за 500 ₽', exact: true});
    await buy.waitFor({timeout: 3000});
    const started = Date.now();
    await buy.click({timeout: 45000});
    await page.getByText('Билет подтверждён', {exact: true}).waitFor({timeout: 15000});
    assert.deepEqual(errors, []);
    const order = await page.locator('#order-id').textContent();
    assert.match(order, /^ord_[0-9a-f]{32}$/);
    await page.reload();
    await page.getByText('Билет подтверждён', {exact: true}).waitFor({timeout: 10000});
    assert.equal(await page.locator('#order-id').textContent(), order);
    await page.screenshot({path: 'artifacts/go-first-purchase.png', fullPage: true});
    fs.writeFileSync('artifacts/go-first-purchase.json', JSON.stringify({order_id: order, duration_ms: Date.now() - started, browser_errors: errors}, null, 2));
    console.log(JSON.stringify({order_id: order, result: 'confirmed'}));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
