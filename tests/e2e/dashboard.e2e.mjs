// End-to-end check of the seismic Dash dashboard (http://localhost:8050) with headless Microsoft Edge.
// Needs Node 18+ and the "playwright" npm package (no browser download; it drives the installed Edge):
//   cd tests\e2e; $env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1; npm install; node dashboard.e2e.mjs
// Expects the stack to be running with both the replay and the live producer (scripts\start.ps1).
// Screenshots go to SCREENSHOT_DIR (default: tests/e2e/screenshots).
import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const shotDir = process.env.SCREENSHOT_DIR || path.join(here, 'screenshots');
fs.mkdirSync(shotDir, { recursive: true });
const shot = (name) => path.join(shotDir, name);
const URL = process.env.DASH_URL || 'http://localhost:8050/';
const results = [];
const check = (name, ok, detail = '') => {
  results.push({ name, ok: !!ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  -- ' + detail : ''}`);
};

const browser = await chromium.launch({ channel: 'msedge', headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1800 } });
const consoleErrors = [];
const callbackErrors = [];
page.on('console', (m) => { if (m.type() === 'error') consoleErrors.push(m.text()); });
page.on('response', (r) => {
  if (r.url().includes('_dash-update-component') && r.status() >= 400) callbackErrors.push(`${r.status()} ${r.url()}`);
});

async function metric(id) {
  const txt = await page.locator(`#${id} h2`).innerText();
  return Number(txt.replace(/,/g, ''));
}
async function graph(id) {
  return page.evaluate((gid) => {
    const el = document.querySelector(`#${gid} .js-plotly-plot`);
    if (!el || !el.data) return null;
    return {
      title: (el.layout && el.layout.title && (el.layout.title.text || el.layout.title)) || '',
      traces: el.data.map((t) => ({ name: t.name, n: (t.x || t.lon || []).length, y: t.y ? Array.from(t.y) : null })),
    };
  }, id);
}
async function settle() {
  // Wait until Dash has no callback in flight.
  await page.waitForFunction(() => !document.querySelector('[data-dash-is-loading="true"]'), null, { timeout: 120000 });
  await page.waitForTimeout(1500);
  await page.waitForFunction(() => !document.querySelector('[data-dash-is-loading="true"]'), null, { timeout: 120000 });
}
async function chooseSource(label) {
  await page.getByLabel(label, { exact: true }).check();
  await settle();
}
async function waitForMetric(id, predicate, timeout = 90000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try { if (predicate(await metric(id))) return true; } catch { /* not rendered yet */ }
    await page.waitForTimeout(1000);
  }
  return false;
}

await page.goto(URL, { waitUntil: 'networkidle' });
await page.waitForSelector('#metric-events h2', { timeout: 120000 });
await settle();
check('dashboard loads with title', (await page.title()) === 'Real-Time Seismic Activity Analysis');

// ---- Both sources (default)
const bothEvents = await metric('metric-events');
check('events from both sources are shown', bothEvents > 84, `events=${bothEvents}`);
await page.screenshot({ path: shot('01-both-sources.png'), fullPage: true });

// ---- Replay demo
await chooseSource('Replay demo');
await waitForMetric('metric-events', (v) => v === 84);
const replayEvents = await metric('metric-events');
check('replay demo: 84 events stored and shown', replayEvents === 84, `events=${replayEvents}`);
const replayCells = await metric('metric-anomalous-cells');
const replayRegions = await metric('metric-active-regions');
check('replay demo: anomalous cells flagged by z-score', replayCells > 0, `cells=${replayCells}`);
check('replay demo: K-Means active regions shown', replayRegions > 0, `regions=${replayRegions}`);
const replayTableRows = await page.locator('#regions-table-content tbody tr').count();
check('replay demo: active-region table lists every region', replayTableRows === replayRegions, `rows=${replayTableRows}`);
const replayMap = await graph('world-map');
check(
  'replay demo: map has events, anomalous cells and K-Means centroids',
  replayMap && ['Events', 'Anomalous grid cells', 'K-Means active regions'].every((n) => replayMap.traces.some((t) => t.name === n)),
  replayMap && replayMap.traces.map((t) => `${t.name}:${t.n}`).join(', '),
);
const replayB = await graph('bvalue-chart');
const replayRegionsShown = replayB ? new Set(replayB.traces.map((t) => t.name.replace(/ alert$/, ''))).size : 0;
check('replay demo: b-value chart shows every replay region with windows (up to 4)', replayRegionsShown >= 2, replayB && replayB.traces.map((t) => `${t.name}:${t.n}`).join(', '));
const replayF = await graph('forecast-chart');
check(
  'replay demo: Omori-Utsu chart shows observed, fitted curve and 6 h forecast',
  replayF && replayF.traces.length === 3 && /M5\.6/.test(replayF.title) && replayF.traces[2].n === 6,
  replayF && `${replayF.title.split('<br>')[0]} | ${replayF.traces.map((t) => `${t.name}:${t.n}`).join(', ')}`,
);
const mainshockLabel = await page.locator('#mainshock').innerText();
check('replay demo: mainshock selector shows the M5.6 demo mainshock', /Demo M5\.6/.test(mainshockLabel), mainshockLabel.trim());
await page.screenshot({ path: shot('02-replay-demo.png'), fullPage: true });

// ---- Live USGS feed
await chooseSource('Live USGS feed');
await waitForMetric('metric-events', (v) => v !== 84);
const liveEvents = await metric('metric-events');
check('live mode: real USGS events shown', liveEvents > 100, `events=${liveEvents}`);
const liveWindows = await metric('metric-bvalue-windows');
check('live mode: b-value windows computed from live data', liveWindows > 0, `bvalue windows=${liveWindows}`);
const liveB = await graph('bvalue-chart');
const liveRegionsShown = liveB ? new Set(liveB.traces.map((t) => t.name.replace(/ alert$/, ''))).size : 0;
check('live mode: b-value chart shows the 4 busiest regions', liveRegionsShown === 4, liveB && liveB.traces.map((t) => `${t.name}:${t.n}`).join(', '));
const liveMap = await graph('world-map');
const liveEventTrace = liveMap && liveMap.traces.find((t) => t.name === 'Events');
check('live mode: map plots the live events', liveEventTrace && liveEventTrace.n === Math.min(liveEvents, 5000), liveEventTrace && `points=${liveEventTrace.n}`);
const liveF = await graph('forecast-chart');
check('live mode: Omori-Utsu forecast panel renders', liveF !== null, liveF && liveF.title.split('<br>')[0]);
await page.screenshot({ path: shot('03-live-usgs.png'), fullPage: true });

// ---- Interactive controls
// Time range
await page.locator('#time-range').click();
await page.getByText('Last 24 hours', { exact: true }).click();
await settle();
await waitForMetric('metric-events', (v) => v < liveEvents);
const live24 = await metric('metric-events');
check('time range control narrows the events', live24 < liveEvents, `7 days=${liveEvents}, 24 hours=${live24}`);

// Minimum magnitude slider (click the "4" mark)
await page.locator('#min-mag .rc-slider-mark-text', { hasText: /^4$/ }).click();
await settle();
await waitForMetric('metric-events', (v) => v < live24);
const liveM4 = await metric('metric-events');
check('magnitude slider filters the events', liveM4 < live24, `M>=0: ${live24}, M>=4: ${liveM4}`);
await page.locator('#min-mag .rc-slider-mark-text', { hasText: /^all$/ }).click();
await page.locator('#time-range').click();
await page.getByText('Last 7 days', { exact: true }).click();
await settle();

// Region selector for the b-value chart: clear, then pick one region.
const before = await graph('bvalue-chart');
await page.locator('#bvalue-regions .Select-clear').click().catch(() => {});
await settle();
await page.locator('#bvalue-regions input').fill('R-160');
await page.keyboard.press('Enter');
await settle();
const afterRegion = await graph('bvalue-chart');
const regionNames = afterRegion ? afterRegion.traces.map((t) => t.name).filter((n) => !/alert$/.test(n)) : [];
check(
  'region selector changes the b-value chart',
  regionNames.length >= 1 && regionNames.every((n) => n.startsWith('R-160')),
  `before=[${before && before.traces.map((t) => t.name).join(', ')}] after=[${regionNames.join(', ')}]`,
);
await page.screenshot({ path: shot('04-region-selected.png'), fullPage: true });

// Mainshock selector: pick a different mainshock if there is one.
await page.locator('#mainshock').click();
const options = page.locator('#mainshock .VirtualizedSelectOption');
const nOptions = await options.count();
if (nOptions > 1) {
  const firstTitle = (await graph('forecast-chart')).title;
  await options.nth(1).click();
  await settle();
  const secondTitle = (await graph('forecast-chart')).title;
  check('mainshock selector switches the forecast', secondTitle !== firstTitle, `${firstTitle.split('<br>')[0]} -> ${secondTitle.split('<br>')[0]}`);
} else {
  await page.keyboard.press('Escape');
  check('mainshock selector lists live mainshocks', nOptions >= 1, `options=${nOptions}`);
}
await page.screenshot({ path: shot('05-mainshock-selected.png'), fullPage: true });

// Auto-refresh: on by default (timestamp moves), off stops it.
const stamp1 = await page.locator('#last-updated').innerText();
await page.waitForTimeout(12000);
const stamp2 = await page.locator('#last-updated').innerText();
check('auto-refresh updates the dashboard every 10 s', stamp1 !== stamp2, `${stamp1.slice(8, 27)} -> ${stamp2.slice(8, 27)}`);
await page.getByLabel(/every 10 s/).uncheck();
await settle();
const stamp3 = await page.locator('#last-updated').innerText();
await page.waitForTimeout(12000);
const stamp4 = await page.locator('#last-updated').innerText();
check('auto-refresh can be switched off', stamp3 === stamp4, stamp4.slice(8, 27));

// Hover: Plotly hover text on the map
const hover = await page.evaluate(() => {
  const el = document.querySelector('#world-map .js-plotly-plot');
  const t = el.data.find((d) => d.name === 'Events');
  return t && (t.text || [])[0];
});
check('map points carry hover text', typeof hover === 'string' && hover.includes('M '), hover);

check('no failed Dash callbacks', callbackErrors.length === 0, callbackErrors.join('; '));
check('no browser console errors', consoleErrors.length === 0, consoleErrors.slice(0, 3).join(' | '));

await browser.close();
const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length ? 1 : 0);
