/* The static copy of the Dash dashboard (src/dashboard/app.py).
 *
 * The pipeline's latest run on GitHub Actions wrote the JSON files in data/ (src/export/export_site.py).
 * This script reads them and draws the same views with the same controls as the Dash page.
 *
 * Where the Dash page runs SQL per filter, this page applies the same rule in the browser to the exported
 * rows (every rule below names the Dash function it follows):
 *   - time ranges: "window_end / event_time > now - hours", with now = generated_at of meta.json
 *     (Dash uses the database's now(); the export is that moment);
 *   - events: count of all matching rows; the map draws the newest 5000 (LIMIT 5000);
 *   - anomalous cells: windows grouped per cell, max count, max z, region of the newest window, top 500;
 *   - b-value regions: windows, alerts and max events per region, top 60; defaults to the first 4;
 *   - b-value chart: the oldest 5000 windows of the chosen regions, one line per region and source.
 * Precomputed by the exporter, because they need the full event table: the mainshock list (latest forecast
 * per mainshock), the observed aftershocks per hour, and the points of the fitted curve and the forecast.
 * Not ported: the 10 s auto-refresh. The page shows one run; the workflow rewrites it on an hourly
 * schedule (GitHub starts scheduled jobs late and skips many: from 14 to 26 Nov 2026 a run started every 4 to 12
 * hours, and a failed run can leave the copy up to about a day old).
 * One rule differs from Dash: the sliding windows the export holds include ones that were still open when
 * the data was written (they end after the newest event of their source, up to half a day later, and hold
 * only part of their data). A running Dash page fills them as data arrives; this one-run copy leaves them
 * out of the map, the counts, the b-value chart and the alerts.
 */
(function () {
  "use strict";

  var SOURCES = { "usgs-live": "Live USGS feed", "replay": "Replay demo" };
  var TIME_RANGES = { "6h": 6, "24h": 24, "72h": 72, "7d": 168, "30d": 720, "all": null };
  var DEFAULT_REGIONS = 4;
  var MAP_EVENT_LIMIT = 5000;
  var CELL_LIMIT = 500;
  var REGION_OPTION_LIMIT = 60;
  var BVALUE_ROW_LIMIT = 5000;
  var CLUSTER_COLORS = ["#d62728", "#1f77b4", "#2ca02c", "#9467bd", "#ff7f0e", "#8c564b", "#e377c2", "#17becf"];
  var REPO_URL = "https://github.com/gopalinternship-unibasel/realtime-seismic-analysis";
  var OLD_AFTER_HOURS = 3;
  var FEW_AFTERSHOCKS = 20;
  var LABEL_MIN_DISTANCE_DEG = 8;

  var D = null; // the loaded data
  var state = { source: "all", range: "7d", minMag: 0, regions: null, mainshock: null, selectorsSource: null };

  function $(id) { return document.getElementById(id); }

  // ------------------------------------------------------------------ small helpers

  function pad(n) { return (n < 10 ? "0" : "") + n; }

  function fmtDate(ms) {
    var d = new Date(ms);
    return d.getUTCFullYear() + "-" + pad(d.getUTCMonth() + 1) + "-" + pad(d.getUTCDate());
  }

  function fmtMinutes(ms) {
    var d = new Date(ms);
    return fmtDate(ms) + " " + pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes());
  }

  function fmtSeconds(ms) {
    return fmtMinutes(ms) + ":" + pad(new Date(ms).getUTCSeconds());
  }

  function plotTime(ms) { return fmtSeconds(ms); } // "YYYY-MM-DD HH:MM:SS" (UTC clock), what Plotly expects

  function fixed(value, digits) { return Number(value).toFixed(digits); }

  function thousands(n) { return Math.round(n).toLocaleString("en-US"); }

  function isNum(v) { return typeof v === "number" && isFinite(v); }

  function sourceLabel(s) { return SOURCES[s] || s; }

  function isPhone() { return window.innerWidth <= 640; }

  /* An expected count below 0.1 keeps two significant figures (0.004, not 0.00); larger ones keep two decimals. */
  function fmtExpected(value) {
    var v = Number(value);
    if (!isFinite(v) || v <= 0 || v >= 0.1) { return fixed(v, 2); }
    if (v < 0.0001) { return "less than 0.0001"; }
    var shown = Number(v.toFixed(1 - Math.floor(Math.log10(v))));
    // 0.0995 rounds up to 0.1: keep the two-decimal form of the larger values.
    return shown >= 0.1 ? fixed(shown, 2) : String(shown);
  }

  /* "R-130_+30" is the 10 x 10 degree box from 130 W to 120 W and 30 N to 40 N. */
  function boxText(regionId) {
    var m = /^R([+-]?\d+)_([+-]?\d+)$/.exec(String(regionId));
    if (!m) { return String(regionId); }
    var lon = parseInt(m[1], 10), lat = parseInt(m[2], 10);
    return edgeText(lon, "W", "E", 180) + ", " + edgeText(lat, "S", "N", 90);
  }

  /* One 10 degree interval from "start": "130-120\u00b0W", "10\u00b0W-0\u00b0", "0-10\u00b0E", capped at the pole or the date line. */
  function edgeText(start, neg, pos, cap) {
    var a = start, b = Math.min(start + 10, cap);
    if (a >= cap) { a = cap - 10; b = cap; }
    if (a < 0) {
      return b === 0 ? Math.abs(a) + "\u00b0" + neg + "-0\u00b0" : Math.abs(a) + "-" + Math.abs(b) + "\u00b0" + neg;
    }
    return a + "-" + b + "\u00b0" + pos;
  }

  function selectedSources() {
    return SOURCES[state.source] ? [state.source] : Object.keys(SOURCES);
  }

  function cutoff() {
    var hours = TIME_RANGES[state.range];
    if (hours === null || hours === undefined) { return null; }
    return D.now - hours * 3600 * 1000;
  }

  function inRange(ms, limit) { return limit === null || ms > limit; }

  function el(tag, props, children) {
    var node = document.createElement(tag);
    Object.keys(props || {}).forEach(function (k) {
      if (k === "text") { node.textContent = props[k]; }
      else if (k === "className") { node.className = props[k]; }
      else { node.setAttribute(k, props[k]); }
    });
    (children || []).forEach(function (c) { node.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
    return node;
  }

  function rowsToObjects(table, timeColumns) {
    return table.rows.map(function (row) {
      var o = {};
      table.columns.forEach(function (name, i) { o[name] = row[i]; });
      (timeColumns || []).forEach(function (c) { o[c + "_ms"] = o[c] ? Date.parse(o[c]) : null; });
      return o;
    });
  }

  function cssVar(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }

  function theme() {
    var dark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    return dark
      ? { text: "#e2e8f0", grid: "#334155", land: "#334155", ocean: "#0f2a44", border: "#64748b", bar: "#64748b",
          // A dark red band hid the red forecast points; a slate band keeps them visible.
          line: "#60a5fa", vrect: "#475569", vrectOpacity: 0.35, mapLine: "#94a3b8" }
      : { text: "#111827", grid: "#e5e7eb", land: "rgb(243,244,246)", ocean: "rgb(229,247,255)", border: "#9ca3af",
          bar: "#94a3b8", line: "#1d4ed8", vrect: "#fee2e2", vrectOpacity: 0.5, mapLine: "#9ca3af" };
  }

  function baseLayout(height) {
    var t = theme();
    return {
      height: height,
      autosize: true,
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: t.text, size: 12 },
    };
  }

  // On phones the toolbar would cover the top of the charts.
  function plotConfig() { return { responsive: true, displaylogo: false, displayModeBar: !isPhone() }; }

  function draw(id, data, layout) {
    if (!window.Plotly) { return; }
    window.Plotly.react(id, data, layout, plotConfig());
  }

  // ------------------------------------------------------------------ loading

  function fetchJson(name) {
    return fetch("data/" + name, { cache: "no-cache" }).then(function (r) {
      if (!r.ok) { throw new Error(name + ": HTTP " + r.status); }
      return r.json();
    });
  }

  function load() {
    var names = ["meta", "events", "anomaly_windows", "active_regions", "bvalue_windows", "mainshocks", "status"];
    return Promise.all(names.map(function (n) { return fetchJson(n + ".json"); })).then(function (v) {
      var events = rowsToObjects(v[1], ["time"]);
      events.sort(function (a, b) { return b.time_ms - a.time_ms; });
      var mainshocks = v[5];
      mainshocks.items.forEach(function (m) {
        m.time_ms = Date.parse(m.time);
        m.clock_ms = m.clock ? Date.parse(m.clock) : null;
        m.forecast.start_ms = m.forecast.start ? Date.parse(m.forecast.start) : null;
      });
      // A window is closed once its end is not after the newest event of its source (see the header).
      var clocks = v[0].newest_event || {};
      var closed = function (w) {
        var clock = clocks[w.source] ? Date.parse(clocks[w.source]) : null;
        return clock === null || w.window_end_ms <= clock;
      };
      var allWindows = rowsToObjects(v[2], ["window_end"]), allBvalues = rowsToObjects(v[4], ["window_end"]);
      var windows = allWindows.filter(closed), bvalues = allBvalues.filter(closed);
      return {
        meta: v[0],
        now: Date.parse(v[0].generated_at),
        events: events,
        windows: windows,
        regions: rowsToObjects(v[3], []),
        bvalues: bvalues,
        // Windows that were still open when the data was written: stored, but left out of the page.
        openAnomalyWindows: allWindows.length - windows.length,
        openBvalueWindows: allBvalues.length - bvalues.length,
        mainshocks: mainshocks,
        status: v[6],
      };
    });
  }

  // ------------------------------------------------------------------ the rules of app.py

  /* is_stale(): an active mainshock's forecast is stale when it starts before the clock of its source's
     latest forecast run by more than STALE_FORECAST_SECONDS. */
  function isStale(startMs, clockMs, active) {
    if (!active || startMs === null || clockMs === null || startMs === undefined || clockMs === undefined) { return false; }
    return (clockMs - startMs) / 1000 > D.mainshocks.stale_seconds;
  }

  function eventsInView() {
    var sources = selectedSources(), limit = cutoff(), minMag = state.minMag;
    return D.events.filter(function (e) {
      if (sources.indexOf(e.source) < 0 || !inRange(e.time_ms, limit)) { return false; }
      // SQL "magnitude >= min": a missing magnitude never matches.
      return minMag > 0 ? (isNum(e.mag) && e.mag >= minMag) : true;
    });
  }

  /* The anomalous-cells query: one row per source and cell. */
  function anomalousCells() {
    var sources = selectedSources(), limit = cutoff(), groups = {}, order = [];
    D.windows.forEach(function (w) {
      if (sources.indexOf(w.source) < 0 || !inRange(w.window_end_ms, limit)) { return; }
      var key = w.source + "|" + w.cell_id + "|" + w.grid_lon + "|" + w.grid_lat;
      var g = groups[key];
      if (!g) {
        g = groups[key] = { source: w.source, cell_id: w.cell_id, grid_lon: w.grid_lon, grid_lat: w.grid_lat,
          max_count: 0, max_z: null, cluster_id: null, latest: -Infinity, windows: 0 };
        order.push(key);
      }
      g.windows += 1;
      if (w.event_count > g.max_count) { g.max_count = w.event_count; }
      if (isNum(w.z) && (g.max_z === null || w.z > g.max_z)) { g.max_z = w.z; }
      if (w.window_end_ms >= g.latest) { g.latest = w.window_end_ms; g.cluster_id = w.cluster_id; }
    });
    var cells = order.map(function (k) { return groups[k]; });
    // ORDER BY max_z DESC: PostgreSQL puts missing values first in a descending sort.
    cells.sort(function (a, b) {
      if (a.max_z === b.max_z) { return 0; }
      if (a.max_z === null) { return -1; }
      if (b.max_z === null) { return 1; }
      return b.max_z - a.max_z;
    });
    return cells.slice(0, CELL_LIMIT);
  }

  function activeRegions() {
    var sources = selectedSources();
    return D.regions.filter(function (r) { return sources.indexOf(r.source) >= 0; }).sort(function (a, b) {
      return a.source < b.source ? -1 : a.source > b.source ? 1 : a.cluster_id - b.cluster_id;
    });
  }

  function bvalueWindows() {
    var sources = selectedSources(), limit = cutoff();
    return D.bvalues.filter(function (b) { return sources.indexOf(b.source) >= 0 && inRange(b.window_end_ms, limit); });
  }

  /* load_bvalue_region_options(): regions with the most windows first, at most 60. */
  function regionOptions() {
    var groups = {};
    bvalueWindows().forEach(function (b) {
      var g = groups[b.region_id] || (groups[b.region_id] = { region_id: b.region_id, windows: 0, alerts: 0, max_events: 0 });
      g.windows += 1;
      if (b.is_alert) { g.alerts += 1; }
      if (isNum(b.event_count) && b.event_count > g.max_events) { g.max_events = b.event_count; }
    });
    return Object.keys(groups).map(function (k) { return groups[k]; }).sort(function (a, b) {
      return (b.windows - a.windows) || (b.max_events - a.max_events);
    }).slice(0, REGION_OPTION_LIMIT);
  }

  /* pick_regions(): keep the chosen regions that still exist; refill the default when none is left, unless the
     visitor cleared the selection on purpose (an empty list). */
  function pickRegions(options, current) {
    if (!options.length) { return []; }
    var valid = {};
    options.forEach(function (o) { valid[o.region_id] = true; });
    var kept = (current || []).filter(function (v) { return valid[v]; });
    if (!kept.length && (current === null || current.length > 0)) {
      kept = options.slice(0, DEFAULT_REGIONS).map(function (o) { return o.region_id; });
    }
    return kept;
  }

  /* load_mainshock_options(): mainshocks with a forecast, active first, newest first (the exporter sorted them). */
  function mainshockOptions() {
    var sources = selectedSources();
    return D.mainshocks.items.filter(function (m) { return sources.indexOf(m.source) >= 0; });
  }

  function mainshockLabel(m) {
    var f = m.forecast, status = "";
    if (!m.active) { status = " (inactive)"; }
    else if (isStale(f.start_ms, m.clock_ms, true)) { status = " (not refitted since " + fmtMinutes(f.start_ms).slice(5) + ")"; }
    var place = m.place || m.id, magText = "M" + fixed(m.mag, 1);
    // The replay places already start with the magnitude ("Demo M5.6 - Northern California").
    return (place.indexOf(magText) >= 0 ? "" : magText + " ") + place + " · " + fmtMinutes(m.time_ms) + " UTC · next 6 h ≈ " +
      fmtExpected(f.next_six_hours) + status;
  }

  function pickMainshock(items, current) {
    if (!items.length) { return null; }
    for (var i = 0; i < items.length; i++) { if (items[i].id === current) { return current; } }
    return items[0].id;
  }

  /* update_selectors(): a different data source starts from the defaults of that source. */
  function updateSelectors() {
    if (state.selectorsSource !== state.source) {
      state.regions = null;
      state.mainshock = null;
    }
    var options = regionOptions();
    state.regions = pickRegions(options, state.regions);
    var items = mainshockOptions();
    state.mainshock = pickMainshock(items, state.mainshock);
    state.selectorsSource = state.source;
    renderRegionPicker(options);
    renderMainshockPicker(items);
  }

  // ------------------------------------------------------------------ pickers

  function renderRegionPicker(options) {
    var list = $("bvalue-regions-list");
    list.textContent = "";
    if (!options.length) { list.appendChild(el("div", { className: "empty", text: "No regions with b-value windows." })); }
    options.forEach(function (o) {
      var input = el("input", { type: "checkbox", value: o.region_id });
      input.checked = state.regions.indexOf(o.region_id) >= 0;
      input.addEventListener("change", function () {
        state.regions = Array.prototype.map.call(list.querySelectorAll("input:checked"), function (i) { return i.value; });
        // The summary changes with the ticks too (it used to keep the old count until another control changed).
        showRegionSummary(options.length);
        render();
      });
      var label = boxText(o.region_id) + "  (" + o.windows + " windows" + (o.alerts ? ", " + o.alerts + " alert windows" : "") + ")";
      list.appendChild(el("label", { title: o.region_id }, [input, " " + label]));
    });
    showRegionSummary(options.length);
  }

  function showRegionSummary(optionCount) {
    $("bvalue-regions-summary").textContent = state.regions.length
      ? state.regions.length + " of " + optionCount + " regions chosen"
      : "Choose regions";
  }

  function renderMainshockPicker(items) {
    var select = $("mainshock");
    select.textContent = "";
    if (!items.length) {
      select.appendChild(el("option", { value: "", text: "No mainshock with a forecast" }));
      select.disabled = true;
      return;
    }
    select.disabled = false;
    items.forEach(function (m) {
      var option = el("option", { value: m.id, text: mainshockLabel(m) });
      if (m.id === state.mainshock) { option.selected = true; }
      select.appendChild(option);
    });
  }

  // ------------------------------------------------------------------ figures

  function mapHover(e) {
    // USGS also lists quarry blasts, explosions and ice quakes; the analytics leave them out.
    var kind = e.type && e.type !== "earthquake" ? e.type + ", " : "";
    return (e.place || "") + "<br>" + kind + "M " + (isNum(e.mag) ? e.mag : "n/a") + "<br>" + fmtSeconds(e.time_ms) + " UTC<br>" +
      sourceLabel(e.source);
  }

  /* "R1" for one source; "Live R1" / "Demo R1" when both are shown, since each source numbers its own regions. */
  function regionName(r, multiSource) {
    return (multiSource ? (r.source === "replay" ? "Demo " : "Live ") : "") + "R" + r.cluster_id;
  }

  function cellsText(r) { return r.cell_count === 1 ? "1 cell" : String(r.cell_count); }

  function radiusText(r) {
    // One 1-degree cell has no spread around its centre: the radius is not meaningful.
    if (r.cell_count === 1) { return "\u2013"; }
    var shown = thousands(r.radius_km) + " km";
    if (r.within_radius === false) {
      shown += isNum(r.radius_limit_km) ? " (over the " + thousands(r.radius_limit_km) + " km limit)" : " (over the limit)";
    }
    return shown;
  }

  function clusterColor(id) { return CLUSTER_COLORS[Math.abs(Math.round(id)) % CLUSTER_COLORS.length]; }

  /* Text-only labels. They go to the regions with the highest max z first. A region gets none when it lies within
     the label distance of another region's centroid (its text would sit on that region's markers) or of a region
     already labelled. Every marker keeps its hover text. */
  function regionLabelTrace(regions, multiSource, t) {
    var labelled = [], labels = regions.map(function () { return ""; });
    var close = function (r, o) {
      var dlon = Math.abs(r.centroid_lon - o.centroid_lon);
      if (dlon > 180) { dlon = 360 - dlon; }
      // A label is wider than tall: about 8 degrees apart in latitude, twice that in longitude.
      return dlon < 2 * LABEL_MIN_DISTANCE_DEG && Math.abs(r.centroid_lat - o.centroid_lat) < LABEL_MIN_DISTANCE_DEG;
    };
    var zOf = function (r) { return isNum(r.max_z_score) ? r.max_z_score : -Infinity; };
    regions.map(function (r, i) { return i; }).sort(function (a, b) {
      var za = zOf(regions[a]), zb = zOf(regions[b]);
      return (za === zb ? 0 : zb - za) || 0;
    }).forEach(function (i) {
      var r = regions[i];
      var covers = regions.some(function (o, j) { return j !== i && close(r, o); });
      if (!covers) { labelled.push(r); labels[i] = regionName(r, multiSource); }
    });
    if (!labelled.length) { return null; }
    return {
      type: "scattergeo", mode: "text", name: "Region labels", hoverinfo: "skip", showlegend: false,
      textposition: "top center", textfont: { color: t.text, size: 12 },
      lon: regions.map(function (r) { return r.centroid_lon; }),
      lat: regions.map(function (r) { return r.centroid_lat; }),
      text: labels,
    };
  }

  function drawMap(events, cells, regions) {
    var t = theme(), traces = [], phone = isPhone();
    var shown = events.slice(0, MAP_EVENT_LIMIT);
    var mags = shown.filter(function (e) { return isNum(e.mag); }).map(function (e) { return e.mag; });
    var cmin = mags.length ? Math.min.apply(null, mags) : 0, cmax = mags.length ? Math.max.apply(null, mags) : 1;
    var multiSource = selectedSources().length > 1;
    // Live and replay events as two traces with one colour scale: the synthetic replay events are diamonds,
    // so they are not read as real earthquakes.
    [["usgs-live", "Events (live USGS feed)", "circle"], ["replay", "Replay demo events (synthetic)", "diamond"]].forEach(function (s, i) {
      var part = shown.filter(function (e) { return e.source === s[0] && isNum(e.mag); });
      if (!part.length) { return; }
      traces.push({
        type: "scattergeo", mode: "markers", name: s[1], hoverinfo: "text",
        lon: part.map(function (e) { return e.lon; }),
        lat: part.map(function (e) { return e.lat; }),
        text: part.map(mapHover),
        marker: {
          symbol: s[2],
          size: part.map(function (e) { return (Math.max(e.mag, 0.5) * 3 + 3) * (phone ? 0.6 : 1); }),
          color: part.map(function (e) { return e.mag; }),
          colorscale: "Viridis", cmin: cmin, cmax: cmax, opacity: 0.72,
          showscale: traces.length === 0,
          colorbar: phone
            ? { title: { text: "M" }, x: 1.0, thickness: 8, len: 0.5 }
            : { title: { text: "Magnitude" }, x: 1.0, thickness: 12, len: 0.8 },
        },
      });
    });
    // Events without a magnitude: small and grey, not coloured like the smallest magnitude.
    var noMag = shown.filter(function (e) { return !isNum(e.mag); });
    if (noMag.length) {
      traces.push({
        type: "scattergeo", mode: "markers", name: "No magnitude", hoverinfo: "text", showlegend: false,
        lon: noMag.map(function (e) { return e.lon; }),
        lat: noMag.map(function (e) { return e.lat; }),
        text: noMag.map(mapHover),
        marker: { size: phone ? 4 : 6, color: "#9ca3af", opacity: 0.72,
          symbol: noMag.map(function (e) { return e.source === "replay" ? "diamond" : "circle"; }) },
      });
    }
    if (cells.length) {
      traces.push({
        type: "scattergeo", mode: "markers", name: "Anomalous grid cells", hoverinfo: "text",
        lon: cells.map(function (c) { return c.grid_lon + 0.5; }),
        lat: cells.map(function (c) { return c.grid_lat + 0.5; }),
        text: cells.map(function (c) {
          return "Anomalous cell " + c.cell_id + "<br>max 6 h count in the time range=" + Math.round(c.max_count) +
            "<br>max z in the time range=" + (c.max_z === null ? "n/a" : fixed(c.max_z, 2)) +
            "<br>active region: " + (c.cluster_id === null ? "none" : regionName({ source: c.source, cluster_id: c.cluster_id }, multiSource)) +
            "<br>" + sourceLabel(c.source);
        }),
        marker: {
          size: 16, symbol: "square-open", line: { width: 3 },
          color: cells.map(function (c) { return c.cluster_id === null ? (t.text === "#111827" ? "#111827" : "#f8fafc") : clusterColor(c.cluster_id); }),
        },
      });
    }
    var labelTrace = null;
    if (regions.length) {
      traces.push({
        type: "scattergeo", mode: "markers", name: "K-Means active regions", hoverinfo: "text",
        lon: regions.map(function (r) { return r.centroid_lon; }),
        lat: regions.map(function (r) { return r.centroid_lat; }),
        hovertext: regions.map(function (r) {
          return "Active region " + regionName(r, multiSource) + " (" + sourceLabel(r.source) + ")<br>" +
            (r.cell_count === 1 ? "1 cell" : r.cell_count + " cells, radius " + radiusText(r)) + "<br>max z=" + fixed(r.max_z_score, 2);
        }),
        marker: { size: 14, symbol: "x", color: regions.map(function (r) { return clusterColor(r.cluster_id); }) },
      });
      // On phones the labels crowd over the western US and the colour bar cuts them: hover only there.
      if (!phone) { labelTrace = regionLabelTrace(regions, multiSource, t); }
    }
    // The region labels are a trace of their own, added after every marker trace, so the text is drawn on top of the cell squares.
    if (labelTrace) { traces.push(labelTrace); }
    var layout = baseLayout(phone ? 380 : 520);
    layout.margin = { r: 0, t: 20, l: 0, b: 0 };
    layout.legend = phone ? { orientation: "h", y: -0.02, font: { size: 10 } } : { orientation: "h" };
    layout.geo = {
      projection: { type: "natural earth" }, showland: true, landcolor: t.land, showcountries: true,
      countrycolor: t.mapLine, coastlinecolor: t.mapLine, showocean: true, oceancolor: t.ocean, bgcolor: "rgba(0,0,0,0)",
    };
    draw("world-map", traces, layout);
  }

  function drawBvalues(rows) {
    var phone = isPhone();
    var t = theme(), traces = [], layout = baseLayout(0);
    var chosen = {};
    state.regions.forEach(function (r) { chosen[r] = true; });
    var selected = rows.filter(function (b) { return chosen[b.region_id]; });
    selected.sort(function (a, b) { return a.window_end_ms - b.window_end_ms; });
    selected = selected.slice(0, BVALUE_ROW_LIMIT);
    if (selected.length) {
      var sourcesSeen = {};
      selected.forEach(function (b) { sourcesSeen[b.source] = true; });
      var multiSource = Object.keys(sourcesSeen).length > 1;
      var groups = {};
      selected.forEach(function (b) {
        var key = b.region_id + "\u0000" + b.source;
        (groups[key] || (groups[key] = [])).push(b);
      });
      var alertLegendShown = false;
      Object.keys(groups).sort().forEach(function (key) {
        // Short legend names ("live" / "demo", the words of the region table's "Live R1" / "Demo R1") keep the legend
        // small; the Dash page writes the source out ("R-130_+30 (Replay demo)").
        var g = groups[key], id = String(g[0].region_id), box = boxText(id);
        var name = multiSource ? box + (g[0].source === "replay" ? " · demo" : " · live") : box;
        // The hover keeps the raw region id next to the box in words.
        var hoverName = name + " (" + id + ")";
        // A window is stored only with 20 or more events above Mc, so hours without one are gaps: a null point
        // breaks the line there instead of drawing a straight line across a day without an estimate.
        var x = [], y = [], sigma = [], custom = [];
        g.forEach(function (b, i) {
          if (i > 0 && b.window_end_ms - g[i - 1].window_end_ms > 3600 * 1000) {
            x.push(null); y.push(null); sigma.push(null); custom.push([null, null]);
          }
          x.push(plotTime(b.window_end_ms)); y.push(b.b_value); sigma.push(b.b_sigma); custom.push([b.event_count, b.long_term_b]);
        });
        traces.push({
          type: "scatter", mode: "lines+markers", name: name, connectgaps: false,
          x: x, y: y,
          error_y: { type: "data", array: sigma, visible: true },
          customdata: custom,
          hovertemplate: "%{x}<br>b=%{y:.2f}<br>events above Mc=%{customdata[0]}<br>long-term b=%{customdata[1]:.2f}<extra>" + hoverName + "</extra>",
        });
        var alerts = g.filter(function (b) { return b.is_alert; });
        if (alerts.length) {
          traces.push({
            // The rule is in the note under the chart and in the hover text; the legend keeps the short name.
            type: "scatter", mode: "markers", name: "b-value alert",
            legendgroup: "alerts", showlegend: !alertLegendShown, legendrank: 2000,
            x: alerts.map(function (b) { return plotTime(b.window_end_ms); }),
            y: alerts.map(function (b) { return b.b_value; }),
            marker: { color: "#dc2626", size: 11, symbol: "circle-open", line: { width: 2 } },
            hovertemplate: "b-value alert: |b − long-term b| > max(0.25, 2σ)<br>b=%{y:.2f}<extra>" + hoverName + "</extra>",
          });
          alertLegendShown = true;
        }
      });
    } else {
      // "Choose" only when the visitor cleared the list; with no windows in the time range there is nothing to choose.
      var cleared = !state.regions.length && regionOptions().length > 0;
      layout.annotations = [{ text: cleared ? "Choose one or more regions above" : "No b-value windows for this selection yet",
        showarrow: false, x: 0.5, y: 0.5, xref: "paper", yref: "paper" }];
    }
    layout.xaxis = { title: { text: "Window end (UTC)" }, gridcolor: t.grid };
    layout.yaxis = { title: { text: "b-value" }, gridcolor: t.grid };
    // The legend starts 66 px (phones: 76 px) below the plot area, under the date labels (two lines) and the
    // axis title, and the bottom margin grows with its rows: phones show one entry per row, wider screens two to
    // three (two are assumed). The chart's height follows, so the plot area keeps its size. A legend at
    // y = -0.25 of a fixed 380 px chart overlapped the axis title, and on phones Plotly squeezed the plot to fit
    // it. legendrank lists the alert entry last.
    var entries = traces.filter(function (tr) { return tr.showlegend !== false; }).length;
    var rows = phone ? entries : Math.ceil(entries / 2);
    var top = 20, plotHeight = phone ? 330 : 300, offset = phone ? 76 : 66;
    var bottom = offset + rows * (phone ? 30 : 24) + 10;
    layout.height = top + plotHeight + bottom;
    $("bvalue-chart").style.height = layout.height + "px";
    layout.margin = { r: 20, t: top, l: 50, b: bottom };
    layout.legend = { orientation: "h", x: 0, xanchor: "left", y: -offset / plotHeight, yanchor: "top" };
    // Plotly's default colours start with red, the colour of the alert circles: the region lines leave it out.
    layout.colorway = ["#1f77b4", "#2ca02c", "#9467bd", "#ff7f0e", "#8c564b", "#e377c2", "#17becf", "#bcbd22", "#7f7f7f"];
    draw("bvalue-chart", traces, layout);
  }

  function currentMainshock() {
    var items = D.mainshocks.items;
    for (var i = 0; i < items.length; i++) { if (items[i].id === state.mainshock) { return items[i]; } }
    return null;
  }

  /* forecast_summary(): the text above the chart. */
  function drawForecastSummary(m) {
    var box = $("forecast-summary");
    box.textContent = "";
    if (!m) { box.appendChild(el("div", { text: "No Omori–Utsu forecast for this selection yet." })); return; }
    var f = m.forecast, info = D.mainshocks;
    var pText = f.p_estimated
      ? "p = " + fixed(f.p, 2) + " (fitted), R² = " + (isNum(f.r_squared) ? fixed(f.r_squared, 2) : "n/a")
      : f.aftershocks < info.min_aftershocks_for_p
        ? "p = " + fixed(f.p, 2) + " (generic value: fewer than " + info.min_aftershocks_for_p + " aftershocks)"
        // seismo_models.fit_omori_utsu: the 95 % interval of p reached the edge of its search grid or was wider than 0.6.
        : "p = " + fixed(f.p, 2) + " (generic value: the aftershock times do not pin p down closely enough)";
    var startText = f.start_ms !== null && f.start_ms !== undefined ? " after " + fmtMinutes(f.start_ms) + " UTC" : "";
    var place = m.place || m.id, magText = "M" + fixed(m.mag, 1);
    box.appendChild(el("div", {}, [el("strong", { text: (place.indexOf(magText) >= 0 ? "" : magText + " ") + place + " · " + fmtMinutes(m.time_ms) + " UTC" })]));
    box.appendChild(el("div", { text: f.aftershocks + " aftershocks within " + Math.round(info.radius_km) + " km and " + info.window_days +
      " days · K = " + fixed(f.k, 1) + "/day, c = " + fixed(f.c, 2) + " d, " + pText }));
    // The aftershocks are all smaller events in the catalogue around the mainshock, so the expected number
    // has no lower magnitude of its own: it is of whatever magnitudes the network records there.
    box.appendChild(el("div", { text: "Expected aftershocks" + startText + " (of the magnitudes the catalogue records there): " +
      fmtExpected(f.next_hour) + " in the next hour, " + fmtExpected(f.next_six_hours) + " in the next 6 hours" }));
    if (f.aftershocks < FEW_AFTERSHOCKS) {
      box.appendChild(el("div", { className: "chart-note", text: "Only " + f.aftershocks + " aftershocks are catalogued for this sequence, " +
        "so the fit rests on little data; the replay demo shows a full sequence." }));
    }
    if (!m.active) {
      box.appendChild(el("div", { className: "forecast-warning", text: "Inactive: more than 30 days before the latest event; this is its last forecast." }));
    } else if (isStale(f.start_ms, m.clock_ms, true)) {
      box.appendChild(el("div", { className: "forecast-warning", text: "Not refitted since " + fmtMinutes(f.start_ms) +
        " UTC: the latest forecast run used the data up to " + fmtMinutes(m.clock_ms) + " UTC." }));
    }
  }

  function drawForecast(m) {
    var t = theme(), traces = [], layout = baseLayout(380);
    if (!m) {
      layout.annotations = [{ text: "No Omori–Utsu forecast for this selection yet", showarrow: false, x: 0.5, y: 0.5,
        xref: "paper", yref: "paper" }];
      layout.margin = { r: 20, t: 20, l: 50, b: 40 };
      draw("forecast-chart", [], layout);
      return;
    }
    var f = m.forecast;
    if (f.hist.length) {
      traces.push({
        type: "bar", name: "Observed aftershocks per hour", width: 0.9, marker: { color: t.bar },
        x: f.hist.map(function (_, i) { return i + 0.5; }), y: f.hist,
      });
    }
    traces.push({
      type: "scatter", mode: "lines", name: "Fitted Omori-Utsu rate", line: { color: t.line, width: 2 },
      x: f.curve.x, y: f.curve.y,
      hovertemplate: "%{x:.1f} h: %{y:.2f} per hour<extra>K=" + fixed(f.k, 1) + "/day, c=" + fixed(f.c, 2) + " d, p=" + fixed(f.p, 2) + "</extra>",
    });
    traces.push({
      type: "scatter", mode: "markers", name: "Forecast, next 6 h", marker: { color: "#dc2626", size: 9 },
      x: f.points.x, y: f.points.y,
    });
    layout.shapes = [{ type: "rect", xref: "x", yref: "paper", x0: f.origin_hours, x1: f.origin_hours + 6, y0: 0, y1: 1,
      fillcolor: t.vrect, opacity: t.vrectOpacity, line: { width: 0 }, layer: "below" }];
    layout.xaxis = { title: { text: "Hours after mainshock" }, gridcolor: t.grid };
    layout.yaxis = { title: { text: "Aftershocks per hour" }, gridcolor: t.grid };
    layout.margin = { r: 20, t: 20, l: 50, b: 40 };
    layout.legend = { orientation: "h", y: -0.25 };
    layout.bargap = 0.05;
    draw("forecast-chart", traces, layout);
  }

  // ------------------------------------------------------------------ the rest of the page

  function metricCard(label, value, id, note) {
    var parts = [el("div", { className: "metric-label", text: label }), el("div", { className: "metric-value", text: String(value) })];
    if (note) { parts.push(el("div", { className: "metric-label", text: note })); }
    return el("div", { className: "card", id: id }, parts);
  }

  function drawMetrics(events, cells, regions) {
    var bw = bvalueWindows(), alerts = bw.filter(function (b) { return b.is_alert; }).length;
    var counts = D.mainshocks.active_counts, active = 0;
    selectedSources().forEach(function (s) { active += counts[s] || 0; });
    // Only mainshocks with 3 or more aftershocks get a forecast, so the list below shows fewer than this count.
    var forecasts = mainshockOptions().filter(function (m) { return m.active; }).length;
    var box = $("metrics");
    box.textContent = "";
    [
      ["Events", thousands(events.length), "metric-events"],
      ["Anomalous cells", cells.length, "metric-anomalous-cells"],
      ["Active regions (K‑Means)", regions.length, "metric-active-regions"],
      ["b-value windows", thousands(bw.length), "metric-bvalue-windows"],
      // Windows slide by 1 hour, so one change in b raises several alert windows in a row.
      ["b-value alert windows", alerts, "metric-bvalue-alerts"],
      ["Active mainshocks (M ≥ 5)", active, "metric-mainshocks", forecasts + " with a forecast"],
    ].forEach(function (c) { box.appendChild(metricCard(c[0], c[1], c[2], c[3])); });
  }

  function drawRegionsTable(regions) {
    var box = $("regions-table");
    box.textContent = "";
    if (!regions.length) {
      box.appendChild(el("div", { className: "empty", text: "No active regions: no grid cell is anomalous in the last 24 hours of data." }));
      return;
    }
    var wide = regions.filter(function (r) { return r.within_radius === false; });
    if (wide.length) {
      var limits = wide.filter(function (r) { return isNum(r.radius_limit_km); }).map(function (r) { return r.radius_limit_km; });
      var caps = wide.filter(function (r) { return isNum(r.max_k); }).map(function (r) { return r.max_k; });
      var seen = {};
      wide.forEach(function (r) { seen[sourceLabel(r.source)] = true; });
      box.appendChild(el("div", { className: "regions-warning", id: "regions-warning", text:
        wide.length + " region(s) of " + Object.keys(seen).sort().join(", ") + " are wider than " +
        (limits.length ? thousands(Math.min.apply(null, limits)) + " km" : "the radius limit") +
        ": the anomalous cells lie in more separate areas than " +
        (caps.length ? "MAX_KMEANS_CLUSTERS = " + Math.min.apply(null, caps) : "MAX_KMEANS_CLUSTERS") +
        " regions can cover, so cells far apart share a region. Raise MAX_KMEANS_CLUSTERS in .env of the local setup to split them." }));
    }
    // The regions are built from the last 24 hours of data, whatever the time range above.
    var head = ["Region", "Source", "Centroid (lon, lat)", "Cells", "Radius", "Max z (last 24 h)", "Max 6 h count (last 24 h)", "Cell ids"];
    // On phones the headings are short (the hover of the map and the note above say "last 24 h") so the table fits its card.
    var phoneHead = ["Region", "Centroid", "Cells", "Radius", "Max z", "Max 6 h"];
    var multiSource = selectedSources().length > 1;
    // On phones the Source and Cell ids columns go: the region name already says Live or Demo.
    var phone = isPhone();
    var keep = function (cells) { return phone ? cells.filter(function (_, i) { return i !== 1 && i !== 7; }) : cells; };
    var table = el("table", { id: "regions-table-content" }, [
      el("thead", {}, [el("tr", {}, (phone ? phoneHead : head).map(function (h) { return el("th", { text: h }); }))]),
      el("tbody", {}, regions.map(function (r) {
        return el("tr", {}, keep([
          el("td", { text: regionName(r, multiSource) }),
          el("td", { text: sourceLabel(r.source) }),
          el("td", { text: fixed(r.centroid_lon, 2) + ", " + fixed(r.centroid_lat, 2) }),
          el("td", { text: cellsText(r) }),
          el("td", { className: r.within_radius === false ? "radius-over" : "", text: radiusText(r) }),
          el("td", { text: isNum(r.max_z_score) ? fixed(r.max_z_score, 1) : "n/a" }),
          el("td", { text: r.max_event_count }),
          el("td", { className: "cells", text: r.cells || "" }),
        ]));
      })),
    ]);
    box.appendChild(table);
  }

  function drawHeader(events) {
    var meta = D.meta, ago = Math.max(0, Date.now() - D.now), hours = ago / 3600000;
    var origin = $("origin");
    origin.textContent = "";
    var minutes = Math.max(1, Math.round(ago / 60000));
    var agoText = hours < 1 ? minutes + (minutes === 1 ? " minute ago" : " minutes ago") : hours < 48 ? fixed(hours, 1) + " hours ago" : Math.round(hours / 24) + " days ago";
    // meta.commit is set by the GitHub Actions run; an export made by hand has none.
    origin.appendChild(document.createTextNode(meta.commit
      ? "This page was written by the pipeline's latest run on GitHub Actions (Kafka, Spark and PostgreSQL started there), at " +
        fmtSeconds(D.now) + " UTC (" + agoText + "). The schedule asks for a new run every hour, but GitHub starts scheduled jobs late and skips many: a new copy usually comes every 4 to 12 hours and can take up to about a day. It is not a continuously running server. "
      : "This page was written by a local run of the exporter, at " + fmtSeconds(D.now) + " UTC (" + agoText +
        "). It shows that one run; it is not a continuously running server. "));
    if (meta.commit) {
      origin.appendChild(document.createTextNode("Code version: "));
      origin.appendChild(el("a", { href: REPO_URL + "/commit/" + meta.commit, text: meta.commit.slice(0, 7) }));
      origin.appendChild(document.createTextNode(". "));
      // The Actions run that wrote this page. GitHub shows the run's log only to signed-in visitors, so the link says so.
      if (typeof meta.run_url === "string" && meta.run_url.indexOf("https://github.com/") === 0) {
        origin.appendChild(el("a", { href: meta.run_url, text: "Pipeline run on GitHub Actions" }));
        origin.appendChild(document.createTextNode(" (the log needs a GitHub sign-in). "));
      }
    }
    origin.appendChild(el("a", { href: REPO_URL, text: "Repository" }));
    origin.appendChild(document.createTextNode("."));
    origin.className = "origin" + (hours > OLD_AFTER_HOURS ? " old" : "");
    var latest = events.length ? fmtSeconds(Math.max.apply(null, events.map(function (e) { return e.time_ms; }))) + " UTC" : "none";
    $("last-updated").textContent = "Data written " + fmtSeconds(D.now) + " UTC · source: " +
      selectedSources().map(sourceLabel).join(", ") + " · latest event in view: " + latest +
      (D.meta.events_total > D.meta.events_exported ? " · only the newest " + thousands(D.meta.events_exported) + " of " + thousands(D.meta.events_total) + " events were exported" : "");
  }

  function table2(headers, rows) {
    return el("table", {}, [
      el("thead", {}, [el("tr", {}, headers.map(function (h) { return el("th", { text: h }); }))]),
      el("tbody", {}, rows.map(function (r) { return el("tr", {}, r.map(function (c) { return el("td", { text: String(c) }); })); })),
    ]);
  }

  function drawStatus() {
    var body = $("status-body"), s = D.status;
    body.textContent = "";
    var sources = Object.keys(SOURCES);
    var tables = Object.keys(s.rows);
    // Rejected records that have no source are exported under "(none)": give them a column of their own.
    tables.forEach(function (t) {
      Object.keys(s.rows[t]).forEach(function (src) { if (sources.indexOf(src) < 0) { sources.push(src); } });
    });
    body.appendChild(el("h4", { text: "Rows stored per table and data source" }));
    body.appendChild(table2(["Table"].concat(sources.map(sourceLabel)), tables.map(function (t) {
      return [t].concat(sources.map(function (src) { return thousands(s.rows[t][src] || 0); }));
    })));
    body.appendChild(el("h4", { text: "Data clocks (UTC)" }));
    body.appendChild(table2(["Source", "Newest event", "Latest forecast run used data up to"], Object.keys(SOURCES).map(function (src) {
      var run = s.forecast_runs[src], newest = D.meta.newest_event[src];
      return [sourceLabel(src), newest ? fmtSeconds(Date.parse(newest)) : "no events", run && run.clock ? fmtSeconds(Date.parse(run.clock)) : "no run"];
    })));
    body.appendChild(el("h4", { text: "Kafka messages rejected by the cleaning rules" }));
    var reasons = Object.keys(s.rejected.by_reason);
    if (!reasons.length) { body.appendChild(el("div", { className: "empty", text: "None (this is normal)." })); }
    else { body.appendChild(table2(["Reason", "Messages"], reasons.map(function (r) { return [r, s.rejected.by_reason[r]]; }))); }
    drawStatusNote();
  }

  /* Why the table counts differ from the cards above: the table counts the whole latest run, the cards a selection. */
  function drawStatusNote() {
    var note = $("status-note"), declustered = D.mainshocks.declustered_counts, m = 0;
    if (!declustered) { note.textContent = ""; return; }
    Object.keys(declustered).forEach(function (src) { m += declustered[src]; });
    note.textContent = "The table counts the whole latest run: both sources and all 7 days. The cards above count only the selected sources and time range, " +
      "and they leave out " + thousands(D.openBvalueWindows) + " b-value windows and " + thousands(D.openAnomalyWindows) +
      " anomaly windows that were still open when the data was written, and " + thousands(m) +
      " events of M \u2265 5 that belong to a larger event's sequence (declustered, no forecast of their own).";
  }

  function render() {
    var events = eventsInView(), cells = anomalousCells(), regions = activeRegions();
    drawHeader(events);
    drawMetrics(events, cells, regions);
    drawMap(events, cells, regions);
    drawRegionsTable(regions);
    drawBvalues(bvalueWindows());
    var m = currentMainshock();
    drawForecastSummary(m);
    drawForecast(m);
    drawStatus();
  }

  function onControl() {
    state.source = document.querySelector("input[name=source]:checked").value;
    state.range = $("time-range").value;
    state.minMag = Number($("min-mag").value);
    $("min-mag-value").textContent = state.minMag === 0 ? "all" : String(state.minMag);
    updateSelectors();
    render();
  }

  function start() {
    Array.prototype.forEach.call(document.querySelectorAll("input[name=source]"), function (i) { i.addEventListener("change", onControl); });
    $("time-range").addEventListener("change", onControl);
    $("min-mag").addEventListener("input", onControl);
    $("mainshock").addEventListener("change", function () {
      state.mainshock = $("mainshock").value;
      render();
    });
    if (window.matchMedia) {
      var query = window.matchMedia("(prefers-color-scheme: dark)");
      var rerender = function () { if (D) { render(); } };
      if (query.addEventListener) { query.addEventListener("change", rerender); }
      else if (query.addListener) { query.addListener(rerender); }
    }
    var resizeTimer = null, wasPhone = isPhone();
    window.addEventListener("resize", function () {
      clearTimeout(resizeTimer);
      // The page changes its layout at the 640 px phone width (map labels and colour bar, b-value chart, toolbars, regions table).
      // Redraw only when that width is crossed: a phone also fires resize when its address bar moves, and a redraw would reset the table scroll and the chart zoom.
      resizeTimer = setTimeout(function () {
        var phone = isPhone();
        if (phone === wasPhone) { return; }
        wasPhone = phone;
        if (D) { drawMap(eventsInView(), anomalousCells(), activeRegions()); drawBvalues(bvalueWindows()); drawRegionsTable(activeRegions()); drawForecast(currentMainshock()); }
      }, 200);
    });
    if (!window.Plotly) {
      var err = $("load-error");
      err.hidden = false;
      err.textContent = "The charting library (Plotly.js from cdnjs.cloudflare.com) could not be loaded, so the charts are not drawn. The counts and tables still work.";
    }
    load().then(function (data) {
      D = data;
      onControl();
    }).catch(function (e) {
      var err = $("load-error");
      err.hidden = false;
      err.textContent = "The data files of the latest run could not be loaded (" + e.message + "). Try again in a few minutes.";
      $("origin").textContent = "No data loaded.";
    });
  }

  start();
})();
