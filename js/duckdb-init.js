/**
 * duckdb-init.js — DuckDB-WASM initialization and core query helpers
 *
 * Initialises a single shared AsyncDuckDB instance using the jsDelivr CDN
 * bundles — no local WASM file hosting required, compatible with GitHub Pages.
 *
 * Usage:
 *   import { getDuckDB, getConnection, runQuery, runQueryArrow } from './duckdb-init.js';
 */

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

let _db = null;        // AsyncDuckDB instance (singleton)
let _conn = null;      // Persistent connection
let _spatialLoaded = false;
let _version = null;

// ── Public state ──────────────────────────────────────────────────────────

/** @returns {boolean} */
export const isSpatialLoaded = () => _spatialLoaded;

/** @returns {string|null} */
export const getVersion = () => _version;

// ── Initialisation ────────────────────────────────────────────────────────

/**
 * Initialise DuckDB-WASM (once).  Subsequent calls return the same instance.
 * @returns {Promise<duckdb.AsyncDuckDB>}
 */
export async function getDuckDB() {
  if (_db) return _db;

  // Select the best bundle for this browser (EH / MVP / threads)
  const bundles = duckdb.getJsDelivrBundles();
  const bundle  = await duckdb.selectBundle(bundles);

  // Build the worker from CDN to avoid same-origin restrictions on GitHub Pages
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );

  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger();

  _db = new duckdb.AsyncDuckDB(logger, worker);
  await _db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  // Get version
  const conn = await _db.connect();
  const res  = await conn.query('SELECT version() AS v');
  _version   = res.toArray()[0]?.v ?? 'unknown';
  await conn.close();

  return _db;
}

/**
 * Get (or create) the persistent named connection.
 * @returns {Promise<duckdb.AsyncDuckDBConnection>}
 */
export async function getConnection() {
  if (!_db) throw new Error('DuckDB not yet initialised. Call getDuckDB() first.');
  if (!_conn) {
    _conn = await _db.connect();
  }
  return _conn;
}

// ── Spatial extension ─────────────────────────────────────────────────────

/**
 * Install and load the DuckDB spatial extension.
 * Downloads from extensions.duckdb.org (CORS-enabled by DuckDB team).
 * @returns {Promise<void>}
 */
export async function loadSpatialExtension() {
  if (_spatialLoaded) return;
  const conn = await getConnection();
  // Install may be a no-op if already cached in the WASM worker
  try {
    await conn.query('INSTALL spatial;');
    await conn.query('LOAD spatial;');
    _spatialLoaded = true;
  } catch (err) {
    // Spatial may already be baked into some bundles
    if (err.message?.includes('already loaded')) {
      _spatialLoaded = true;
      return;
    }
    throw new Error(`Failed to load spatial extension: ${err.message}`);
  }
}

// ── Error normalisation ──────────────────────────────────────────────────

/**
 * Convert anything thrown by DuckDB (incl. WebAssembly.Exception which has no
 * .message) into a plain JS Error with a human-readable message.
 */
function toJsError(err) {
  if (err instanceof Error) return err;
  const s = String(err);
  const readable = (s && s !== '[object Object]' && s !== '[object WebAssembly.Exception]')
    ? s
    : 'DuckDB raised an internal WebAssembly exception. Check your SQL syntax and ensure the data source is accessible.';
  return new Error(readable);
}

// ── Query helpers ─────────────────────────────────────────────────────────

/**
 * Execute a SQL query and return results as an array of plain objects.
 * @param {string} sql
 * @returns {Promise<{ rows: object[], columns: string[], elapsed: number }>}
 */
export async function runQuery(sql) {
  const conn = await getConnection();
  const start = performance.now();
  let result;
  try {
    result = await conn.query(sql);
  } catch (err) {
    throw toJsError(err);
  }
  const elapsed = performance.now() - start;

  /** @type {import('@apache-arrow/ts/interfaces').Table} */
  const schema  = result.schema;
  const columns = schema.fields.map(f => f.name);
  const rows    = result.toArray().map(row => {
    const obj = {};
    columns.forEach(col => {
      const val = row[col];
      obj[col] = val !== undefined ? val : null;
    });
    return obj;
  });

  return { rows, columns, schema: schema.fields, elapsed };
}

/**
 * Execute SQL and return the raw Arrow Table (for efficient large result handling).
 * @param {string} sql
 * @returns {Promise<import('@apache-arrow/ts/interfaces').Table>}
 */
export async function runQueryArrow(sql) {
  const conn = await getConnection();
  try {
    return await conn.query(sql);
  } catch (err) {
    throw toJsError(err);
  }
}

/**
 * Execute a SQL statement that returns no rows (CREATE, INSERT, COPY…).
 * @param {string} sql
 * @returns {Promise<{ elapsed: number }>}
 */
export async function runStatement(sql) {
  const conn = await getConnection();
  const start = performance.now();
  try {
    await conn.query(sql);
  } catch (err) {
    throw toJsError(err);
  }
  return { elapsed: performance.now() - start };
}

// ── File system helpers ───────────────────────────────────────────────────

/**
 * Register an in-browser File object as a DuckDB virtual file.
 * After calling this, DuckDB can reference it by name.
 *
 * @param {File} file    - Browser File object (from <input type="file">)
 * @param {string} name  - Virtual filename to register (e.g. 'upload.parquet')
 * @returns {Promise<void>}
 */
export async function registerUploadedFile(file, name) {
  const db = await getDuckDB();
  const buf = await file.arrayBuffer();
  await db.registerFileBuffer(name, new Uint8Array(buf));
}

/**
 * Copy a file from DuckDB's virtual FS to a Uint8Array for download.
 *
 * @param {string} path  - Virtual file path (e.g. '/tmp/export.parquet')
 * @returns {Promise<Uint8Array>}
 */
export async function readVirtualFile(path) {
  const db = await getDuckDB();
  return db.copyFileToBuffer(path);
}

// ── Schema introspection ──────────────────────────────────────────────────

/**
 * List all user tables currently loaded in DuckDB.
 * @returns {Promise<string[]>}
 */
export async function listTables() {
  try {
    const { rows } = await runQuery("SHOW TABLES");
    return rows.map(r => r.name ?? r.Name ?? Object.values(r)[0]).filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * Describe the columns of a table.
 * @param {string} tableName
 * @returns {Promise<Array<{column_name:string, column_type:string, null:string}>>}
 */
export async function describeTable(tableName) {
  try {
    const { rows } = await runQuery(`DESCRIBE "${tableName}"`);
    return rows;
  } catch {
    return [];
  }
}

/**
 * Count rows in a table.
 * @param {string} tableName
 * @returns {Promise<number>}
 */
export async function countRows(tableName) {
  try {
    const { rows } = await runQuery(`SELECT COUNT(*) AS n FROM "${tableName}"`);
    return Number(rows[0]?.n ?? 0);
  } catch {
    return 0;
  }
}

// ── Teardown ──────────────────────────────────────────────────────────────

/**
 * Close the current connection and drop all tables (soft reset).
 * The DuckDB worker is kept alive.
 */
export async function resetDatabase() {
  if (_conn) {
    try { await _conn.close(); } catch { /* ignore */ }
    _conn = null;
  }
  _spatialLoaded = false;
  // Re-opening a connection gives a fresh in-memory database
  _conn = await _db.connect();
}
