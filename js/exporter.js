/**
 * exporter.js — Export and Parquet converter module
 *
 * Handles:
 *   1. Exporting explorer_result (or custom SQL) to Parquet / CSV / JSON / GeoJSON / Arrow
 *   2. Converting a locally-uploaded Parquet file to any supported format
 *
 * All file I/O is performed inside DuckDB-WASM's virtual file system,
 * then the bytes are streamed to the browser via a Blob download — nothing
 * is ever sent to a server.
 */

import {
  runQuery,
  runStatement,
  runQueryArrow,
  registerUploadedFile,
  readVirtualFile,
  describeTable,
} from './duckdb-init.js';

// ── Utility ───────────────────────────────────────────────────────────────

function downloadBytes(bytes, filename, mimeType = 'application/octet-stream') {
  const blob = new Blob([bytes], { type: mimeType });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

function downloadText(text, filename, mimeType = 'text/plain') {
  downloadBytes(new TextEncoder().encode(text), filename, mimeType);
}

function setStatus(wrapId, msgId, show, msg = '') {
  document.getElementById(wrapId)?.classList.toggle('d-none', !show);
  if (msgId) {
    const el = document.getElementById(msgId);
    if (el) el.textContent = msg;
  }
}

function showAlert(id, msg, type = 'danger') {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = msg;
  el.className   = `alert alert-${type} small py-2`;
  el.classList.remove('d-none');
  setTimeout(() => el.classList.add('d-none'), 8000);
}

function ext(format) {
  const map = { parquet: '.parquet', csv: '.csv', json: '.json', geojson: '.geojson', arrow: '.arrow' };
  return map[format] ?? `.${format}`;
}

// ── GeoJSON helpers ───────────────────────────────────────────────────────

/**
 * Build a GeoJSON FeatureCollection from rows that have a geom_wkt column
 * (produced by ST_AsText) or fall back to raw WKB hex strings (geometry col).
 */
function rowsToGeoJson(rows, columns) {
  const geomCol = columns.find(c => c === 'geom_wkt') ??
                  columns.find(c => c === 'geometry');
  if (!geomCol) {
    throw new Error('No geometry column found. Load data with "Include WKT geometry" enabled, or add ST_AsText(geometry) AS geom_wkt to your query.');
  }

  const propCols = columns.filter(c => c !== geomCol);

  const features = rows.map(row => {
    const rawGeom = row[geomCol];
    let geometry  = null;

    if (rawGeom && typeof rawGeom === 'string') {
      // WKT parsing (rough — handles Point, LineString, Polygon)
      geometry = parseWkt(rawGeom);
    }

    const properties = {};
    propCols.forEach(c => {
      const v = row[c];
      properties[c] = (v === null || v === undefined) ? null
                    : (typeof v === 'bigint') ? Number(v) : v;
    });

    return { type: 'Feature', geometry, properties };
  });

  return JSON.stringify({ type: 'FeatureCollection', features }, null, 2);
}

/** Minimal WKT → GeoJSON geometry parser */
function parseWkt(wkt) {
  if (!wkt) return null;
  const s = wkt.trim().toUpperCase();

  try {
    if (s.startsWith('POINT')) {
      const m = wkt.match(/POINT\s*\(\s*([\d.eE+\-]+)\s+([\d.eE+\-]+)\s*\)/i);
      if (m) return { type: 'Point', coordinates: [parseFloat(m[1]), parseFloat(m[2])] };
    }
    if (s.startsWith('LINESTRING')) {
      const inner = wkt.replace(/LINESTRING\s*\(/i, '').replace(/\)$/, '');
      return { type: 'LineString', coordinates: parseCoordList(inner) };
    }
    if (s.startsWith('POLYGON')) {
      const rings = wkt.replace(/POLYGON\s*\(/i, '').replace(/\)\s*$/, '')
                       .split(/\)\s*,\s*\(/)
                       .map(r => r.replace(/^\(/, '').replace(/\)$/, ''));
      return { type: 'Polygon', coordinates: rings.map(parseCoordList) };
    }
    if (s.startsWith('MULTIPOINT')) {
      const inner = wkt.replace(/MULTIPOINT\s*\(/i, '').replace(/\)$/, '');
      return { type: 'MultiPoint', coordinates: parseCoordList(inner) };
    }
  } catch { /* fall through */ }

  // Return raw WKT as a string property rather than crashing
  return null;
}

function parseCoordList(s) {
  return s.trim().split(/\s*,\s*/).map(pair => {
    const parts = pair.trim().split(/\s+/);
    return [parseFloat(parts[0]), parseFloat(parts[1])];
  });
}

// ── Arrow IPC export ──────────────────────────────────────────────────────

async function exportArrow(sql) {
  const table = await runQueryArrow(sql);
  // Arrow JS serialize to IPC bytes
  const { tableToIPC } = await import('https://cdn.jsdelivr.net/npm/apache-arrow@17/+esm');
  return tableToIPC(table);
}

// ── Core export function ──────────────────────────────────────────────────

/**
 * Export data from DuckDB to a file download.
 *
 * @param {object} opts
 * @param {string} opts.sql         SQL to produce the data
 * @param {string} opts.format      parquet | csv | json | geojson | arrow
 * @param {string} opts.filename    Output filename
 * @param {object} opts.csvOptions  { header: bool, delimiter: string }
 * @param {string} opts.parquetCompression  snappy | zstd | gzip | uncompressed
 */
export async function exportData(opts) {
  const {
    sql,
    format,
    filename,
    csvOptions   = { header: true, delimiter: ',' },
    parquetCompression = 'snappy',
  } = opts;

  const virtualPath = `/tmp/ky_export${ext(format)}`;

  switch (format) {
    case 'parquet': {
      await runStatement(
        `COPY (${sql}) TO '${virtualPath}' (FORMAT PARQUET, COMPRESSION ${parquetCompression.toUpperCase()});`
      );
      const bytes = await readVirtualFile(virtualPath);
      downloadBytes(bytes, filename, 'application/octet-stream');
      break;
    }

    case 'csv': {
      const delimEsc  = csvOptions.delimiter === '\t' ? 'E\'\\t\'' : `'${csvOptions.delimiter}'`;
      const headerOpt = csvOptions.header ? 'true' : 'false';
      await runStatement(
        `COPY (${sql}) TO '${virtualPath}' (FORMAT CSV, HEADER ${headerOpt}, DELIMITER ${delimEsc});`
      );
      const bytes = await readVirtualFile(virtualPath);
      downloadBytes(bytes, filename, 'text/csv');
      break;
    }

    case 'json': {
      // DuckDB COPY ... FORMAT JSON writes newline-delimited JSON
      await runStatement(
        `COPY (${sql}) TO '${virtualPath}' (FORMAT JSON);`
      );
      const bytes = await readVirtualFile(virtualPath);
      downloadBytes(bytes, filename, 'application/json');
      break;
    }

    case 'geojson': {
      // Fetch rows and columns for manual GeoJSON building
      const { rows, columns } = await runQuery(sql);
      const geojson = rowsToGeoJson(rows, columns);
      downloadText(geojson, filename, 'application/geo+json');
      break;
    }

    case 'arrow': {
      const bytes = await exportArrow(sql);
      downloadBytes(bytes, filename, 'application/vnd.apache.arrow.file');
      break;
    }

    default:
      throw new Error(`Unsupported format: ${format}`);
  }
}

// ── Uploader / Converter ──────────────────────────────────────────────────

let _uploadedVirtualName = null;   // name registered in DuckDB VFS

/**
 * Register an uploaded file with DuckDB and preview its schema.
 * @param {File} file
 * @returns {Promise<{columns: Array<{column_name, column_type}>, rowCount: number}>}
 */
export async function loadUploadedParquet(file) {
  const safeName = 'uploaded_' + file.name.replace(/[^a-zA-Z0-9._-]/g, '_');
  await registerUploadedFile(file, safeName);
  _uploadedVirtualName = safeName;

  // Create a temporary view so we can DESCRIBE it
  await runStatement(`CREATE OR REPLACE VIEW __upload_preview AS SELECT * FROM read_parquet('${safeName}') LIMIT 0;`);
  const columns = await describeTable('__upload_preview');

  // Count — DuckDB over a local file is fast
  const { rows } = await runQuery(`SELECT COUNT(*) AS n FROM read_parquet('${safeName}')`);
  const rowCount = Number(rows[0]?.n ?? 0);

  // Clean up temp view
  await runStatement('DROP VIEW IF EXISTS __upload_preview;');

  return { columns, rowCount };
}

/**
 * Convert the currently uploaded Parquet file to another format.
 * @param {object} opts
 * @param {string} opts.format
 * @param {string} opts.filename
 * @param {number} opts.limit   0 = all rows
 * @param {string} opts.parquetCompression
 */
export async function convertUploadedFile(opts) {
  if (!_uploadedVirtualName) throw new Error('No file uploaded yet.');

  const { format, filename, limit = 0, parquetCompression = 'snappy' } = opts;
  const limitClause = limit > 0 ? `LIMIT ${limit}` : '';
  const sql = `SELECT * FROM read_parquet('${_uploadedVirtualName}') ${limitClause}`;

  await exportData({ sql, format, filename, parquetCompression });
}

// ── UI wiring ─────────────────────────────────────────────────────────────

/**
 * Wire up the Export & Convert tab UI.
 * Call once on page load.
 */
export function initExportUI() {

  // ── Format radio → filename extension + option panels ─────────────────
  function updateExportFormatUI() {
    const format = document.querySelector('input[name="export-format"]:checked')?.value ?? 'parquet';
    const filenameEl = document.getElementById('export-filename');
    if (filenameEl) {
      filenameEl.value = filenameEl.value.replace(/\.[^.]+$/, ext(format));
    }
    document.getElementById('export-csv-options')?.classList.toggle('d-none', format !== 'csv');
    document.getElementById('export-geojson-options')?.classList.toggle('d-none', format !== 'geojson');
  }

  document.querySelectorAll('input[name="export-format"]').forEach(radio => {
    radio.addEventListener('change', updateExportFormatUI);
  });

  // Source → show/hide custom SQL textarea
  document.getElementById('export-source')?.addEventListener('change', e => {
    document.getElementById('export-custom-sql-wrap')?.classList.toggle('d-none', e.target.value !== 'custom');
  });

  // ── Export button ──────────────────────────────────────────────────────
  document.getElementById('export-btn')?.addEventListener('click', async () => {
    const btn    = document.getElementById('export-btn');
    const format = document.querySelector('input[name="export-format"]:checked')?.value ?? 'parquet';
    const source = document.getElementById('export-source')?.value ?? 'explorer_result';
    const filename = document.getElementById('export-filename')?.value?.trim() || `overture_export${ext(format)}`;

    const sql = source === 'custom'
      ? (document.getElementById('export-custom-sql')?.value?.trim() ?? 'SELECT 1')
      : `SELECT * FROM explorer_result`;

    const csvOptions = {
      header:    document.getElementById('csv-header')?.checked ?? true,
      delimiter: document.getElementById('csv-delimiter')?.value ?? ',',
    };

    const parquetCompression = 'snappy'; // could expose a UI control later

    btn.disabled = true;
    setStatus('export-status', 'export-status-msg', true, `Exporting as ${format.toUpperCase()}…`);
    document.getElementById('export-error')?.classList.add('d-none');
    document.getElementById('export-success')?.classList.add('d-none');

    try {
      await exportData({ sql, format, filename, csvOptions, parquetCompression });
      showAlert('export-success', `Downloaded: ${filename}`, 'success');
    } catch (err) {
      showAlert('export-error', err.message ?? String(err));
    } finally {
      btn.disabled = false;
      setStatus('export-status', null, false);
    }
  });

  // ── Convert: file input ────────────────────────────────────────────────
  document.getElementById('convert-file-input')?.addEventListener('change', async e => {
    const file = e.target.files?.[0];
    if (!file) return;

    const fileInfo = document.getElementById('convert-file-info');
    const convertBtn = document.getElementById('convert-btn');
    document.getElementById('convert-file-name').textContent = file.name;
    document.getElementById('convert-file-size').textContent = formatBytes(file.size);
    document.getElementById('convert-error')?.classList.add('d-none');

    try {
      const { columns, rowCount } = await loadUploadedParquet(file);
      fileInfo.classList.remove('d-none');

      // Render schema
      const schemaEl = document.getElementById('convert-file-schema');
      schemaEl.innerHTML = '';
      const dl = document.createElement('dl');
      dl.className = 'row g-0 mb-0';
      const dtRows = document.createElement('dt');
      dtRows.className = 'col-5 text-muted';
      dtRows.textContent = 'Rows';
      const ddRows = document.createElement('dd');
      ddRows.className = 'col-7';
      ddRows.textContent = rowCount.toLocaleString();
      dl.appendChild(dtRows);
      dl.appendChild(ddRows);

      const dtCols = document.createElement('dt');
      dtCols.className = 'col-5 text-muted';
      dtCols.textContent = 'Columns';
      const ddCols = document.createElement('dd');
      ddCols.className = 'col-7';
      ddCols.textContent = columns.length;
      dl.appendChild(dtCols);
      dl.appendChild(ddCols);
      schemaEl.appendChild(dl);

      const colList = document.createElement('div');
      colList.className = 'mt-1';
      colList.style.maxHeight = '80px';
      colList.style.overflowY = 'auto';
      columns.forEach(col => {
        const span = document.createElement('span');
        span.className = 'badge bg-secondary me-1 mb-1 font-monospace';
        span.style.fontSize = '.65rem';
        span.textContent = `${col.column_name}: ${col.column_type}`;
        colList.appendChild(span);
      });
      schemaEl.appendChild(colList);

      convertBtn.disabled = false;
    } catch (err) {
      fileInfo.classList.add('d-none');
      convertBtn.disabled = true;
      showAlert('convert-error', err.message ?? String(err));
    }
  });

  // ── Convert: format select → parquet options ───────────────────────────
  document.getElementById('convert-format')?.addEventListener('change', e => {
    document.getElementById('convert-parquet-opts')?.classList.toggle('d-none', e.target.value !== 'parquet');
    // Update file extension hint on the input
    const input = document.getElementById('convert-file-input');
    const fname = _uploadedVirtualName?.replace('uploaded_', '') ?? 'converted';
    // (no UI filename field for convert — DuckDB generates name)
  });

  // ── Convert button ─────────────────────────────────────────────────────
  document.getElementById('convert-btn')?.addEventListener('click', async () => {
    const btn    = document.getElementById('convert-btn');
    const format = document.getElementById('convert-format')?.value ?? 'csv';
    const limit  = parseInt(document.getElementById('convert-limit')?.value ?? '0', 10);
    const compression = document.getElementById('convert-parquet-compression')?.value ?? 'snappy';
    const origName = document.getElementById('convert-file-name')?.textContent ?? 'converted';
    const filename = origName.replace(/\.[^.]+$/, '') + ext(format);

    btn.disabled = true;
    setStatus('convert-status', 'convert-status-msg', true, `Converting to ${format.toUpperCase()}…`);
    document.getElementById('convert-error')?.classList.add('d-none');
    document.getElementById('convert-success')?.classList.add('d-none');

    try {
      await convertUploadedFile({ format, filename, limit, parquetCompression: compression });
      showAlert('convert-success', `Downloaded: ${filename}`, 'success');
    } catch (err) {
      showAlert('convert-error', err.message ?? String(err));
    } finally {
      btn.disabled = false;
      setStatus('convert-status', null, false);
    }
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────

function formatBytes(bytes) {
  if (bytes < 1_024)       return `${bytes} B`;
  if (bytes < 1_048_576)   return `${(bytes / 1_024).toFixed(1)} KB`;
  if (bytes < 1_073_741_824) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  return `${(bytes / 1_073_741_824).toFixed(2)} GB`;
}
