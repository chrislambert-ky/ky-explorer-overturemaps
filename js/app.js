/**
 * app.js — Main application entry point for KY Overture Explorer
 *
 * Responsibilities:
 *  - Boot DuckDB-WASM, load spatial extension
 *  - Wire sidebar controls (theme/type selects, bbox presets, limit slider)
 *  - Handle "Load Dataset" execution
 *  - Manage tab switching
 *  - Display results table with pagination on Results tab
 *  - Render point data on Map tab via Leaflet
 *  - Delegate SQL Editor to sql-editor.js
 *  - Delegate Export/Convert to exporter.js
 *  - Persist settings (proxy URL) in localStorage
 */

import {
  getDuckDB,
  getConnection,
  loadSpatialExtension,
  runQuery,
  runStatement,
  listTables,
  describeTable,
  resetDatabase,
  getVersion,
  isSpatialLoaded,
} from './duckdb-init.js';

import {
  CATALOG,
  buildParquetUrl,
  buildLoadSQL,
  fetchParquetFileUrls,
  populateThemeSelect,
  populateTypeSelect,
  getCatalogEntry,
} from './catalog.js';

import { initSqlEditor, renderTable, rowsToCsv } from './sql-editor.js';
import { initExportUI } from './exporter.js';

// ── Constants ─────────────────────────────────────────────────────────────

const RESULTS_PAGE_SIZE = 500;
const SETTINGS_KEY      = 'ky_overture_settings';

// ── Application state ─────────────────────────────────────────────────────

const DEFAULT_PROXY_URL = 'https://overture.kypc.workers.dev';

const state = {
  settings: {
    proxyUrl:     DEFAULT_PROXY_URL,
    proxyPattern: 'path',
  },
  loadedRows:    [],
  loadedColumns: [],
  currentPage:   0,
  map:             null,
  mapMarkers:      null,  // Leaflet LayerGroup
  pendingMapBounds: null, // [south, west, north, east] to apply on map init
};

// ── Settings persistence ──────────────────────────────────────────────────

function loadSettings() {
  try {
    const raw = localStorage.getItem(SETTINGS_KEY);
    if (raw) {
      const saved = JSON.parse(raw);
      // If user had previously saved an empty proxy URL, restore the default
      if (!saved.proxyUrl) saved.proxyUrl = DEFAULT_PROXY_URL;
      Object.assign(state.settings, saved);
    }
  } catch { /* ignore */ }
}

function saveSettings() {
  try {
    localStorage.setItem(SETTINGS_KEY, JSON.stringify(state.settings));
  } catch { /* ignore */ }
}

// ── Status/feedback helpers ───────────────────────────────────────────────

function setStatus(msg) {
  const el = document.getElementById('status-text');
  if (el) el.textContent = msg;
}

function setDbStatusBadge(status, label) {
  const badge = document.getElementById('duckdb-status');
  if (!badge) return;
  badge.innerHTML = label;
  badge.className = 'badge d-flex align-items-center gap-1';
  badge.classList.add(status === 'ready' ? 'bg-success' : status === 'error' ? 'bg-danger' : 'bg-secondary');
}

function showCorsHint(show) {
  document.getElementById('cors-alert')?.classList.toggle('d-none', !show);
}

function isCorsError(err) {
  const msg = (err?.message ?? '') + (err?.stack ?? '');
  return /cors|cross.*origin|network.*error|failed.*fetch/i.test(msg);
}

// ── Tab management ────────────────────────────────────────────────────────

const TABS = ['results', 'sql', 'map', 'export', 'schema'];

function switchTab(tabId) {
  TABS.forEach(id => {
    const link = document.getElementById(`tab-${id}`);
    const pane = document.getElementById(`pane-${id}`);
    const active = id === tabId;
    link?.classList.toggle('active', active);
    pane?.classList.toggle('d-none', !active);
  });

  // Lazy init the map when the Map tab is first shown
  if (tabId === 'map' && !state.map) {
    initMap();
  }
}

document.querySelectorAll('.nav-link[data-tab]').forEach(link => {
  link.addEventListener('click', e => {
    e.preventDefault();
    switchTab(link.dataset.tab);
  });
});

// "Go to SQL Editor" link in results empty state
document.getElementById('goto-sql-link')?.addEventListener('click', e => {
  e.preventDefault();
  switchTab('sql');
});

// ── Sidebar: theme/type dropdowns ─────────────────────────────────────────

function initSidebarSelects() {
  const themeSelect = document.getElementById('theme-select');
  const typeSelect  = document.getElementById('type-select');
  const descEl      = document.getElementById('type-description');

  populateThemeSelect(themeSelect);

  function onThemeChange() {
    populateTypeSelect(typeSelect, themeSelect.value);
    onTypeChange();
  }

  function onTypeChange() {
    const entry = getCatalogEntry(themeSelect.value, typeSelect.value);
    if (descEl) descEl.textContent = entry?.description ?? '';
    updateUrlPreview();
  }

  themeSelect.addEventListener('change', onThemeChange);
  typeSelect.addEventListener('change', onTypeChange);

  // Init
  onThemeChange();
}

// ── Sidebar: bbox preset ──────────────────────────────────────────────────

function zoomMapToBbox(xmin, ymin, xmax, ymax) {
  const bounds = [[ymin, xmin], [ymax, xmax]];
  if (state.map) {
    state.map.fitBounds(bounds, { padding: [20, 20] });
  } else {
    state.pendingMapBounds = bounds;
  }
}

function initBboxPreset() {
  document.getElementById('bbox-preset')?.addEventListener('change', e => {
    const val = e.target.value;
    if (!val) return;
    const [xmin, ymin, xmax, ymax] = val.split(',').map(Number);
    document.getElementById('bbox-xmin').value = xmin;
    document.getElementById('bbox-xmax').value = xmax;
    document.getElementById('bbox-ymin').value = ymin;
    document.getElementById('bbox-ymax').value = ymax;
    updateUrlPreview();
    zoomMapToBbox(xmin, ymin, xmax, ymax);
  });

  ['bbox-xmin','bbox-xmax','bbox-ymin','bbox-ymax'].forEach(id => {
    document.getElementById(id)?.addEventListener('input', () => {
      updateUrlPreview();
      const xmin = parseFloat(document.getElementById('bbox-xmin')?.value);
      const xmax = parseFloat(document.getElementById('bbox-xmax')?.value);
      const ymin = parseFloat(document.getElementById('bbox-ymin')?.value);
      const ymax = parseFloat(document.getElementById('bbox-ymax')?.value);
      if (!isNaN(xmin) && !isNaN(xmax) && !isNaN(ymin) && !isNaN(ymax)) {
        zoomMapToBbox(xmin, ymin, xmax, ymax);
      }
    });
  });
}

// ── Sidebar: limit slider ─────────────────────────────────────────────────

function initLimitSlider() {
  const slider   = document.getElementById('limit-slider');
  const valueEl  = document.getElementById('limit-value');
  slider?.addEventListener('input', () => {
    valueEl.textContent = Number(slider.value).toLocaleString();
  });
}

// ── URL preview (no-op: element removed) ────────────────────────────────

function updateUrlPreview() {}

// ── Load Dataset ──────────────────────────────────────────────────────────

async function handleLoadDataset() {
  const theme   = document.getElementById('theme-select')?.value;
  const type    = document.getElementById('type-select')?.value;
  const release = document.getElementById('release-select')?.value ?? '2026-01-21.0';
  const limit   = parseInt(document.getElementById('limit-slider')?.value ?? '10000', 10);
  const includeSpatial = document.getElementById('load-spatial')?.checked ?? true;
  const includeWkt     = document.getElementById('add-wkt')?.checked ?? false;

  const xmin = parseFloat(document.getElementById('bbox-xmin')?.value) || null;
  const xmax = parseFloat(document.getElementById('bbox-xmax')?.value) || null;
  const ymin = parseFloat(document.getElementById('bbox-ymin')?.value) || null;
  const ymax = parseFloat(document.getElementById('bbox-ymax')?.value) || null;

  // Validate bbox — all four or none
  const bboxFields = [xmin, xmax, ymin, ymax];
  const bboxFilled = bboxFields.filter(v => v !== null).length;
  if (bboxFilled > 0 && bboxFilled < 4) {
    showResultsError('Please fill in all four bounding box fields, or leave all empty.');
    switchTab('results');
    return;
  }

  // WKT requires spatial extension
  if (includeWkt && !isSpatialLoaded()) {
    if (includeSpatial) {
      try {
        setStatus('Loading spatial extension…');
        await loadSpatialExtension();
        updateSpatialBadge();
      } catch (err) {
        showResultsError(`Spatial extension error: ${err.message}`);
        switchTab('results');
        return;
      }
    } else {
      showResultsError('Enable "Load Spatial Extension" to use the WKT geometry option.');
      switchTab('results');
      return;
    }
  }

  // UI: show loading
  const btn = document.getElementById('load-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Loading…';

  const progressWrap = document.getElementById('load-progress');
  const loadStatus   = document.getElementById('load-status');
  progressWrap?.classList.remove('d-none');

  hideResultsError();
  hideResultsTable();
  document.getElementById('results-empty')?.classList.add('d-none');

  const entry = getCatalogEntry(theme, type);
  setStatus(`Fetching file list from S3…`);
  if (loadStatus) loadStatus.textContent = `Listing parquet files…`;

  switchTab('results');

  const start = performance.now();

  try {
    // Resolve exact file URLs via S3 ListObjectsV2 — DuckDB-WASM cannot expand
    // *.parquet globs over plain HTTP proxy URLs.
    const urls = await fetchParquetFileUrls(
      theme, type, release,
      state.settings.proxyUrl,
      state.settings.proxyPattern,
    );

    setStatus(`Found ${urls.length} file${urls.length === 1 ? '' : 's'}, loading ${entry?.label ?? type}…`);
    if (loadStatus) loadStatus.textContent = `Querying ${entry?.label ?? type} (${urls.length} files)…`;

    const sql = buildLoadSQL({ urls, xmin, xmax, ymin, ymax, limit, includeWkt });

    // Show generated SQL in the SQL Editor tab
    import('./sql-editor.js').then(({ setSqlEditorValue }) => setSqlEditorValue(sql));

    await runStatement(sql);
    const elapsed = ((performance.now() - start) / 1000).toFixed(2);

    // Read results for display
    const { rows, columns } = await runQuery('SELECT * FROM explorer_result');
    state.loadedRows    = rows;
    state.loadedColumns = columns;
    state.currentPage   = 0;

    // Update status bar
    const rowCount = rows.length;
    document.getElementById('status-rows').textContent = `${rowCount.toLocaleString()} rows loaded`;
    setStatus(`Loaded ${rowCount.toLocaleString()} ${entry?.label ?? type} features in ${elapsed}s`);

    // Results badge
    const badge = document.getElementById('results-badge');
    if (badge) {
      badge.textContent = rowCount.toLocaleString();
      badge.classList.remove('d-none');
    }

    // Render results table
    showResultsTable(columns, rows);

    // Update schema sidebar
    await refreshSchemaTree();

    // Update export source description
    const exportSel = document.getElementById('export-source');
    if (exportSel?.options[0]) {
      exportSel.options[0].textContent = `Explorer Result — ${rowCount.toLocaleString()} rows (explorer_result)`;
    }

  } catch (err) {
    setStatus(`Error loading dataset: ${err.message}`);
    const corsHit = isCorsError(err);
    showResultsError(err.message, corsHit);
    showCorsHint(corsHit && !state.settings.proxyUrl);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-cloud-download-fill"></i> Fetch Dataset';
    progressWrap?.classList.add('d-none');
  }
}

// ── Results table display ─────────────────────────────────────────────────

function showResultsTable(columns, rows) {
  document.getElementById('results-empty')?.classList.add('d-none');
  document.getElementById('results-error')?.classList.add('d-none');

  const wrap  = document.getElementById('results-table-wrap');
  const thead = document.getElementById('results-thead');
  const tbody = document.getElementById('results-tbody');
  wrap?.classList.remove('d-none');

  showResultsPage(columns, rows, 0);
}

function showResultsPage(columns, rows, page) {
  state.currentPage = page;
  const start = page * RESULTS_PAGE_SIZE;
  const end   = Math.min(start + RESULTS_PAGE_SIZE, rows.length);
  const slice = rows.slice(start, end);

  const thead = document.getElementById('results-thead');
  const tbody = document.getElementById('results-tbody');
  renderTable(thead, tbody, columns, slice);

  // Pagination
  const totalPages = Math.max(1, Math.ceil(rows.length / RESULTS_PAGE_SIZE));
  const pagWrap    = document.getElementById('result-pagination');
  const pageInfo   = document.getElementById('page-info');
  const prevBtn    = document.getElementById('prev-page');
  const nextBtn    = document.getElementById('next-page');
  const infoEl     = document.getElementById('result-info');

  if (infoEl) {
    infoEl.textContent = `${rows.length.toLocaleString()} rows · showing ${start+1}–${end}`;
  }

  if (totalPages > 1) {
    pagWrap?.classList.remove('d-none');
    if (pageInfo) pageInfo.textContent = `${page + 1} / ${totalPages}`;
    if (prevBtn) prevBtn.disabled = page === 0;
    if (nextBtn) nextBtn.disabled = page >= totalPages - 1;
  } else {
    pagWrap?.classList.add('d-none');
  }

  document.getElementById('copy-results-btn')?.classList.remove('d-none');
}

function hideResultsTable() {
  document.getElementById('results-table-wrap')?.classList.add('d-none');
}

function showResultsError(msg, isCors = false) {
  const errBox = document.getElementById('results-error');
  document.getElementById('results-empty')?.classList.add('d-none');
  hideResultsTable();
  if (!errBox) return;
  errBox.classList.remove('d-none');
  const msgEl  = document.getElementById('error-message');
  const hint   = document.getElementById('error-cors-hint');
  if (msgEl)   msgEl.textContent = msg;
  if (hint)    hint.classList.toggle('d-none', !isCors);
}

function hideResultsError() {
  document.getElementById('results-error')?.classList.add('d-none');
}

// ── Result pagination buttons ─────────────────────────────────────────────

function initResultsPagination() {
  document.getElementById('prev-page')?.addEventListener('click', () => {
    if (state.currentPage > 0)
      showResultsPage(state.loadedColumns, state.loadedRows, state.currentPage - 1);
  });
  document.getElementById('next-page')?.addEventListener('click', () => {
    const total = Math.ceil(state.loadedRows.length / RESULTS_PAGE_SIZE);
    if (state.currentPage < total - 1)
      showResultsPage(state.loadedColumns, state.loadedRows, state.currentPage + 1);
  });

  // Copy CSV
  document.getElementById('copy-results-btn')?.addEventListener('click', () => {
    if (!state.loadedRows.length) return;
    navigator.clipboard.writeText(rowsToCsv(state.loadedColumns, state.loadedRows)).then(() => {
      const btn = document.getElementById('copy-results-btn');
      const orig = btn.innerHTML;
      btn.innerHTML = '<i class="bi bi-check-lg"></i>';
      setTimeout(() => { btn.innerHTML = orig; }, 1500);
    }).catch(() => {});
  });
}

// ── Schema tree ───────────────────────────────────────────────────────────

async function refreshSchemaTree() {
  const section  = document.getElementById('schema-section');
  const emptyEl  = document.getElementById('schema-empty');
  const tree     = document.getElementById('schema-tree');
  if (!section || !tree) return;

  const tables = await listTables();
  if (!tables.length) {
    section.classList.add('d-none');
    emptyEl?.classList.remove('d-none');
    return;
  }

  emptyEl?.classList.add('d-none');
  section.classList.remove('d-none');
  tree.innerHTML = '';

  for (const tbl of tables) {
    // Table row
    const tblDiv = document.createElement('div');
    tblDiv.className = 'schema-tree-table';
    tblDiv.innerHTML = `<i class="bi bi-table"></i><span>${escapeHtml(tbl)}</span>`;
    tree.appendChild(tblDiv);

    // Lazy-load columns on click
    let expanded = false;
    let colsEl   = null;
    tblDiv.addEventListener('click', async () => {
      if (!expanded) {
        const cols = await describeTable(tbl);
        colsEl = document.createElement('div');
        cols.forEach(c => {
          const d = document.createElement('div');
          d.className = 'schema-tree-col';
          d.innerHTML = `<span>${escapeHtml(c.column_name ?? '')}</span><span class="col-type">${escapeHtml(c.column_type ?? '')}</span>`;
          colsEl.appendChild(d);
        });
        tblDiv.after(colsEl);
        expanded = true;
      } else {
        colsEl?.remove();
        colsEl  = null;
        expanded = false;
      }
    });
  }
}

// ── Map tab ───────────────────────────────────────────────────────────────

function initMap() {
  if (state.map) return;

  // Read bounds from whatever is currently in the bbox fields
  const xmin = parseFloat(document.getElementById('bbox-xmin')?.value);
  const xmax = parseFloat(document.getElementById('bbox-xmax')?.value);
  const ymin = parseFloat(document.getElementById('bbox-ymin')?.value);
  const ymax = parseFloat(document.getElementById('bbox-ymax')?.value);
  const hasBbox = [xmin, xmax, ymin, ymax].every(v => !isNaN(v));

  // Bootstrap the map at a neutral center; fitBounds will correct it immediately
  state.map = L.map('map', {
    center:       [37.8, -85.5],
    zoom:         7,
    preferCanvas: true,
  });

  L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/">CARTO</a>',
    maxZoom:     19,
  }).addTo(state.map);

  state.mapMarkers = L.layerGroup().addTo(state.map);

  // Zoom to bbox fields first, then fall back to any pending preset bounds
  if (hasBbox) {
    state.map.fitBounds([[ymin, xmin], [ymax, xmax]], { padding: [20, 20] });
  } else if (state.pendingMapBounds) {
    state.map.fitBounds(state.pendingMapBounds, { padding: [20, 20] });
  }
  state.pendingMapBounds = null;
}

function initMapUI() {
  document.getElementById('map-render-btn')?.addEventListener('click', renderMapPoints);
  document.getElementById('map-clear-btn')?.addEventListener('click', () => {
    state.mapMarkers?.clearLayers();
    document.getElementById('map-point-count')?.classList.add('d-none');
  });
}

async function renderMapPoints() {
  if (!state.map) initMap();
  if (!state.loadedRows.length) return;

  state.mapMarkers?.clearLayers();

  // Try to find lat/lon columns
  const cols = state.loadedColumns.map(c => c.toLowerCase());
  const lonCol = state.loadedColumns.find(c => /^(lon|lng|longitude|x)$/i.test(c));
  const latCol = state.loadedColumns.find(c => /^(lat|latitude|y)$/i.test(c));

  // If no explicit lat/lon, try to extract from bbox centroid
  const bboxXmin = state.loadedColumns.find(c => /bbox.*xmin/i.test(c));

  let pointCount = 0;
  const MAX_MARKERS = 20_000;

  try {
    let sql;
    if (lonCol && latCol) {
      sql = `SELECT "${lonCol}" AS lon, "${latCol}" AS lat,
                    COALESCE(names['primary'], id) AS label
             FROM explorer_result
             WHERE "${lonCol}" IS NOT NULL AND "${latCol}" IS NOT NULL
             LIMIT ${MAX_MARKERS}`;
    } else {
      // Try to extract centroid from bbox struct
      sql = `SELECT
               (bbox.xmin + bbox.xmax) / 2 AS lon,
               (bbox.ymin + bbox.ymax) / 2 AS lat,
               COALESCE(names['primary'], id) AS label
             FROM explorer_result
             WHERE bbox IS NOT NULL
             LIMIT ${MAX_MARKERS}`;
    }

    const { rows } = await runQuery(sql);
    const bounds = [];

    rows.forEach(row => {
      const lon = parseFloat(row.lon);
      const lat = parseFloat(row.lat);
      if (isNaN(lon) || isNaN(lat)) return;
      if (lon < -180 || lon > 180 || lat < -90 || lat > 90) return;

      const marker = L.circleMarker([lat, lon], {
        radius:      5,
        fillColor:   '#0d6efd',
        color:       '#ffffff',
        weight:      1.5,
        opacity:     1,
        fillOpacity: 0.75,
      });

      if (row.label) {
        marker.bindPopup(`<strong>${escapeHtml(String(row.label))}</strong><br/>lon:${lon.toFixed(5)}, lat:${lat.toFixed(5)}`);
      }

      marker.addTo(state.mapMarkers);
      bounds.push([lat, lon]);
      pointCount++;
    });

    if (bounds.length) {
      state.map.fitBounds(bounds, { padding: [30, 30] });
    }

    const cntEl = document.getElementById('map-point-count');
    if (cntEl) {
      cntEl.textContent = `${pointCount.toLocaleString()} points`;
      cntEl.classList.remove('d-none');
    }

  } catch (err) {
    console.error('Map render error:', err);
  }
}

// ── Settings modal ────────────────────────────────────────────────────────

function initSettingsModal() {
  const proxyInput    = document.getElementById('settings-proxy-url');
  const patternSelect = document.getElementById('settings-proxy-pattern');
  const statusBadge   = document.getElementById('settings-proxy-status');
  const removeBtn     = document.getElementById('settings-remove-proxy');

  // Track whether the user explicitly clicked Remove
  let _pendingRemove = false;

  function refreshProxyStatus() {
    const configured = !!state.settings.proxyUrl;
    if (statusBadge) {
      statusBadge.textContent  = configured ? 'Configured ✓' : 'Not configured';
      statusBadge.className    = `badge ${configured ? 'bg-success' : 'bg-secondary'}`;
    }
    removeBtn?.classList.toggle('d-none', !configured);
  }

  // Never pre-fill the URL input — just show status
  if (patternSelect) patternSelect.value = state.settings.proxyPattern;
  refreshProxyStatus();

  // Remove proxy button
  removeBtn?.addEventListener('click', () => {
    _pendingRemove = true;
    if (proxyInput) proxyInput.value = '';
    if (statusBadge) {
      statusBadge.textContent = 'Will be removed on Save';
      statusBadge.className   = 'badge bg-warning text-dark';
    }
    removeBtn.classList.add('d-none');
  });

  // Settings tab switching
  document.querySelectorAll('[data-settings-tab]').forEach(link => {
    link.addEventListener('click', e => {
      e.preventDefault();
      const tabId = link.dataset.settingsTab;
      document.querySelectorAll('[data-settings-tab]').forEach(l => l.classList.toggle('active', l === link));
      document.querySelectorAll('.settings-tab-pane').forEach(p => p.classList.add('d-none'));
      document.getElementById(`settings-${tabId}`)?.classList.remove('d-none');
    });
  });

  // Save settings
  document.getElementById('settings-save-btn')?.addEventListener('click', () => {
    const typed = proxyInput?.value?.trim() ?? '';
    if (_pendingRemove) {
      state.settings.proxyUrl = '';          // user explicitly removed it
    } else if (typed) {
      state.settings.proxyUrl = typed;       // user pasted a new URL
    }
    // else: leave state.settings.proxyUrl unchanged (blank input = keep existing)

    state.settings.proxyPattern = patternSelect?.value ?? 'path';
    _pendingRemove = false;
    saveSettings();
    updateUrlPreview();
    showCorsHint(!state.settings.proxyUrl);

    const modal = bootstrap.Modal.getInstance(document.getElementById('settingsModal'));
    modal?.hide();
  });

  // Test proxy
  document.getElementById('settings-test-proxy')?.addEventListener('click', async () => {
    const proxyUrl = proxyInput?.value?.trim() ?? '';
    const resultEl = document.getElementById('settings-proxy-test-result');
    if (!proxyUrl) {
      if (resultEl) resultEl.innerHTML = '<span class="text-warning">No proxy URL entered.</span>';
      return;
    }
    if (resultEl) resultEl.innerHTML = '<span class="text-muted">Testing…</span>';
    try {
      // Probe a tiny known Overture file — just the root listing (will 403, but proves proxy reachability)
      const testUrl = proxyUrl.replace(/\/$/, '') +
        (state.settings.proxyPattern === 'param'
          ? `?url=${encodeURIComponent('https://overturemaps-us-west-2.s3.amazonaws.com/')}`
          : '/https://overturemaps-us-west-2.s3.amazonaws.com/');

      const resp = await fetch(testUrl, { method: 'HEAD', signal: AbortSignal.timeout(5000) });
      if (resultEl) {
        resultEl.innerHTML = resp.ok || resp.status < 500
          ? `<span class="text-success"><i class="bi bi-check-circle me-1"></i>Proxy reachable (HTTP ${resp.status})</span>`
          : `<span class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>HTTP ${resp.status} — proxy reachable but returned an error</span>`;
      }
    } catch (err) {
      if (resultEl) {
        resultEl.innerHTML = `<span class="text-danger"><i class="bi bi-x-circle me-1"></i>${escapeHtml(err.message)}</span>`;
      }
    }
  });

  // Reset modal state each time it opens
  document.getElementById('settingsModal')?.addEventListener('show.bs.modal', () => {
    _pendingRemove = false;
    if (proxyInput) proxyInput.value = '';
    if (patternSelect) patternSelect.value = state.settings.proxyPattern;
    refreshProxyStatus();
    document.getElementById('settings-db-status').textContent  = getVersion() ? 'Ready' : 'Initializing';
    document.getElementById('settings-db-version').textContent = getVersion() ?? '—';
    document.getElementById('settings-db-spatial').textContent = isSpatialLoaded() ? 'Loaded' : 'Not loaded';
  });

  // Reinitialize
  document.getElementById('settings-reinit-db')?.addEventListener('click', async () => {
    const btn = document.getElementById('settings-reinit-db');
    btn.disabled = true;
    try {
      await resetDatabase();
      state.loadedRows    = [];
      state.loadedColumns = [];
      await refreshSchemaTree();
      document.getElementById('settings-db-status').textContent  = 'Ready (reset)';
    } finally {
      btn.disabled = false;
    }
  });
}

function updateSpatialBadge() {
  const badge = document.getElementById('spatial-status');
  if (!badge) return;
  badge.classList.remove('d-none', 'bg-secondary', 'bg-success');
  badge.classList.add(isSpatialLoaded() ? 'bg-success' : 'bg-secondary');
  badge.textContent = 'Spatial';
}

// ── DuckDB boot ───────────────────────────────────────────────────────────

async function bootDuckDB() {
  setDbStatusBadge('loading',
    '<span class="spinner-grow spinner-grow-sm" role="status"></span> Initializing…');

  try {
    await getDuckDB();
    const ver = getVersion();

    setDbStatusBadge('ready',
      `<i class="bi bi-database-fill-check"></i> DuckDB ${ver}`);
    document.getElementById('status-db').textContent = `DuckDB ${ver} ready`;
    setStatus('DuckDB ready');

    // Spatial extension — try to load on boot (non-fatal)
    try {
      await loadSpatialExtension();
      updateSpatialBadge();
    } catch {
      // Spatial will be loaded on-demand when user requests it
    }

  } catch (err) {
    setDbStatusBadge('error', '<i class="bi bi-exclamation-triangle-fill"></i> DuckDB Error');
    document.getElementById('status-db').textContent = 'DuckDB failed to initialize';
    showResultsError(`DuckDB failed to initialize: ${err.message}`);
    switchTab('results');
  }
}

// ── Utility ───────────────────────────────────────────────────────────────

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ── Bootstrap ─────────────────────────────────────────────────────────────

async function main() {
  loadSettings();

  // Show CORS alert only if proxy has been explicitly cleared by the user
  showCorsHint(!state.settings.proxyUrl);

  // Populate sidebar selects
  initSidebarSelects();
  initBboxPreset();
  initLimitSlider();

  // Release select change → update URL preview
  document.getElementById('release-select')?.addEventListener('change', updateUrlPreview);

  // Load button
  document.getElementById('load-btn')?.addEventListener('click', handleLoadDataset);

  // Results pagination
  initResultsPagination();

  // Map UI
  initMapUI();

  // Settings modal
  initSettingsModal();

  // SQL editor — pass runQuery as the execution callback
  initSqlEditor(async (sql) => {
    const { rows, columns, elapsed } = await runQuery(sql);
    return { rows, columns, elapsed };
  });

  // Export & Convert tab
  initExportUI();

  // Boot DuckDB (non-blocking — UI renders immediately)
  await bootDuckDB();

  // Initial URL preview
  updateUrlPreview();
}

main().catch(err => {
  console.error('Application failed to start:', err);
});
