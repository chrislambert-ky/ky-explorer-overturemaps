/**
 * sql-editor.js — CodeMirror SQL editor + query execution + history
 *
 * Exports:
 *   initSqlEditor(runCallback) → editor instance
 *   setSqlEditorValue(sql)
 *   getSqlEditorValue()
 */

const MAX_HISTORY = 50;
const PAGE_SIZE   = 500;

let _cm        = null;  // CodeMirror instance
let _history   = [];    // [{sql, ts}]
let _sqlRows   = [];    // last query rows
let _sqlCols   = [];    // last query columns
let _sqlPage   = 0;

// ── Helpers ───────────────────────────────────────────────────────────────

function fmtElapsed(ms) {
  if (ms < 1000) return `${ms.toFixed(0)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

function isCorsError(err) {
  const msg = (err?.message ?? '') + (err?.stack ?? '');
  return /cors|cross[-\s]?origin|network|fetch/i.test(msg)
      || (err instanceof TypeError && msg.includes('fetch'));
}

// ── Table rendering (shared with results panel) ───────────────────────────

/**
 * Render rows into a thead + tbody.
 * Values are sanitised — no innerHTML injection from data.
 */
export function renderTable(theadEl, tbodyEl, columns, rows) {
  // Header
  theadEl.innerHTML = '';
  const headerRow = document.createElement('tr');
  columns.forEach(col => {
    const th = document.createElement('th');
    th.textContent = col;
    headerRow.appendChild(th);
  });
  theadEl.appendChild(headerRow);

  // Body
  tbodyEl.innerHTML = '';
  rows.forEach(row => {
    const tr = document.createElement('tr');
    columns.forEach(col => {
      const td  = document.createElement('td');
      const val = row[col];
      applyCellStyle(td, col, val);
      tr.appendChild(td);
    });
    tbodyEl.appendChild(tr);
  });
}

function applyCellStyle(td, col, val) {
  if (val === null || val === undefined) {
    td.textContent = 'NULL';
    td.className = 'cell-null';
    return;
  }
  if (typeof val === 'boolean') {
    td.textContent = String(val);
    td.className   = val ? 'cell-bool-true' : 'cell-bool-false';
    return;
  }
  if (typeof val === 'number' || typeof val === 'bigint') {
    td.textContent = String(val);
    td.className   = 'cell-number';
    return;
  }
  const s = String(val);
  // Detect WKB / geometry blobs
  if (col === 'geometry' || col === 'geom_wkt' || col.endsWith('_geom')) {
    td.textContent = s.length > 80 ? s.slice(0, 80) + '…' : s;
    td.className   = 'cell-geometry';
    return;
  }
  // IDs
  if (col === 'id') {
    td.textContent = s.length > 40 ? s.slice(0, 40) + '…' : s;
    td.className   = 'cell-id';
    return;
  }
  // Truncate long values; the CSS hover will expand them
  td.textContent = s.length > 200 ? s.slice(0, 200) + '…' : s;
}

/** Convert current rows to CSV string */
export function rowsToCsv(columns, rows, delimiter = ',') {
  const escape = v => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    if (s.includes(delimiter) || s.includes('"') || s.includes('\n')) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };
  const header = columns.map(escape).join(delimiter);
  const body   = rows.map(r => columns.map(c => escape(r[c])).join(delimiter)).join('\n');
  return header + '\n' + body;
}

// ── History ───────────────────────────────────────────────────────────────

function loadHistory() {
  try {
    _history = JSON.parse(localStorage.getItem('ky_sql_history') ?? '[]');
  } catch {
    _history = [];
  }
}

function saveHistory() {
  try {
    localStorage.setItem('ky_sql_history', JSON.stringify(_history.slice(0, MAX_HISTORY)));
  } catch { /* storage full — ignore */ }
}

function pushHistory(sql) {
  const trimmed = sql.trim();
  if (!trimmed) return;
  // De-duplicate: remove existing entry if same SQL
  _history = _history.filter(h => h.sql !== trimmed);
  _history.unshift({ sql: trimmed, ts: Date.now() });
  if (_history.length > MAX_HISTORY) _history.length = MAX_HISTORY;
  saveHistory();
  rebuildHistorySelect();
}

function rebuildHistorySelect() {
  const sel = document.getElementById('sql-history-select');
  if (!sel) return;
  // Keep the placeholder
  while (sel.options.length > 1) sel.remove(1);
  _history.forEach((h, i) => {
    const opt    = document.createElement('option');
    opt.value    = i;
    const preview = h.sql.replace(/\s+/g, ' ').slice(0, 60);
    opt.textContent = preview + (h.sql.length > 60 ? '…' : '');
    sel.appendChild(opt);
  });
}

// ── Pagination ────────────────────────────────────────────────────────────

function showSqlPage(page) {
  _sqlPage = page;
  const total = _sqlRows.length;
  const start = page * PAGE_SIZE;
  const end   = Math.min(start + PAGE_SIZE, total);
  const slice = _sqlRows.slice(start, end);

  const thead = document.getElementById('sql-results-thead');
  const tbody = document.getElementById('sql-results-tbody');
  renderTable(thead, tbody, _sqlCols, slice);

  // Pagination controls
  const pageInfo = document.getElementById('sql-page-info');
  const pagWrap  = document.getElementById('sql-pagination');
  const prevBtn  = document.getElementById('sql-prev-page');
  const nextBtn  = document.getElementById('sql-next-page');

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  if (totalPages > 1) {
    pagWrap.classList.remove('d-none');
    pageInfo.textContent = `Page ${page + 1} / ${totalPages}`;
    prevBtn.disabled = page === 0;
    nextBtn.disabled = page >= totalPages - 1;
  } else {
    pagWrap.classList.add('d-none');
  }
}

// ── SQL Editor init ───────────────────────────────────────────────────────

/**
 * Initialise the CodeMirror editor and wire up all UI events.
 * @param {function(string):Promise<void>} runCallback  Called with the SQL to execute.
 * @returns {CodeMirror.Editor} editor instance
 */
export function initSqlEditor(runCallback) {
  loadHistory();

  const textarea = document.getElementById('sql-editor');
  _cm = CodeMirror.fromTextArea(textarea, {
    mode:            'text/x-sql',
    theme:           'eclipse',
    lineNumbers:     true,
    matchBrackets:   true,
    autoCloseBrackets: true,
    tabSize:         2,
    indentWithTabs:  false,
    extraKeys: {
      'Ctrl-Enter': () => _runCurrentQuery(),
      'Cmd-Enter':  () => _runCurrentQuery(),
      'Ctrl-Space': 'autocomplete',
    },
    hintOptions: {
      tables: {
        explorer_result: ['id', 'geometry', 'bbox', 'names'],
      },
    },
    viewportMargin: Infinity,
  });

  // Resize CodeMirror to fill its container
  const wrap = document.getElementById('sql-editor-wrap');
  const ro   = new ResizeObserver(() => {
    _cm.setSize(null, wrap.clientHeight);
  });
  ro.observe(wrap);

  // ── Button wiring ──────────────────────────────────────────────────────

  document.getElementById('sql-run-btn')?.addEventListener('click', _runCurrentQuery);

  document.getElementById('sql-clear-btn')?.addEventListener('click', () => {
    _cm.setValue('');
    _cm.focus();
  });

  // Samples menu toggle
  const samplesBtn  = document.getElementById('sql-format-btn');
  const samplesMenu = document.getElementById('samples-menu');

  // Position samples menu relative to the toolbar
  samplesBtn?.addEventListener('click', e => {
    e.stopPropagation();
    samplesMenu.classList.toggle('d-none');
    if (!samplesMenu.classList.contains('d-none')) {
      const rect = samplesBtn.getBoundingClientRect();
      samplesMenu.style.position = 'fixed';
      samplesMenu.style.top  = `${rect.bottom + 4}px`;
      samplesMenu.style.left = `${rect.left}px`;
    }
  });

  document.addEventListener('click', () => {
    samplesMenu?.classList.add('d-none');
  });

  // Insert sample query on click
  document.querySelectorAll('.sample-query').forEach(btn => {
    btn.addEventListener('click', () => {
      const sql = btn.dataset.query.replace(/\\n/g, '\n');
      _cm.setValue(sql);
      _cm.focus();
      samplesMenu.classList.add('d-none');
    });
  });

  // History select
  document.getElementById('sql-history-select')?.addEventListener('change', e => {
    const idx = parseInt(e.target.value, 10);
    if (!isNaN(idx) && _history[idx]) {
      _cm.setValue(_history[idx].sql);
      _cm.focus();
      e.target.value = '';
    }
  });

  // Result pagination
  document.getElementById('sql-prev-page')?.addEventListener('click', () => {
    if (_sqlPage > 0) showSqlPage(_sqlPage - 1);
  });
  document.getElementById('sql-next-page')?.addEventListener('click', () => {
    showSqlPage(_sqlPage + 1);
  });

  // Copy button (on results tab - reuse rowsToCsv)
  document.getElementById('copy-results-btn')?.addEventListener('click', () => {
    if (!_sqlRows.length) return;
    navigator.clipboard.writeText(rowsToCsv(_sqlCols, _sqlRows)).then(() => {
      const btn = document.getElementById('copy-results-btn');
      const orig = btn.innerHTML;
      btn.innerHTML = '<i class="bi bi-check-lg"></i>';
      setTimeout(() => { btn.innerHTML = orig; }, 1500);
    }).catch(() => {});
  });

  // Internal run function
  async function _runCurrentQuery() {
    const sql = _cm.getValue().trim();
    if (!sql) return;
    await _executeQuery(sql, runCallback);
    pushHistory(sql);
  }

  rebuildHistorySelect();
  return _cm;
}

// ── Query execution (called from editor or externally) ────────────────────

/**
 * Execute SQL, update the SQL Editor results pane.
 * @param {string} sql
 * @param {function(string):Promise<{rows,columns,elapsed}>} runCallback
 */
async function _executeQuery(sql, runCallback) {
  const runBtn      = document.getElementById('sql-run-btn');
  const execTime    = document.getElementById('sql-exec-time');
  const rowCount    = document.getElementById('sql-row-count');
  const emptyState  = document.getElementById('sql-results-empty');
  const errBox      = document.getElementById('sql-results-error');
  const errMsg      = document.getElementById('sql-error-message');
  const errCors     = document.getElementById('sql-error-cors-hint');
  const tableEl     = document.getElementById('sql-results-table');

  // Loading state
  runBtn.disabled  = true;
  runBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Running…';
  execTime.textContent  = '';
  rowCount.classList.add('d-none');
  emptyState.classList.add('d-none');
  errBox.classList.add('d-none');
  tableEl.classList.add('d-none');

  try {
    const result = await runCallback(sql);
    const { rows, columns, elapsed } = result;

    _sqlRows = rows;
    _sqlCols = columns;
    _sqlPage = 0;

    execTime.textContent = fmtElapsed(elapsed);

    if (rows.length === 0) {
      emptyState.classList.remove('d-none');
      emptyState.innerHTML = `
        <i class="bi bi-check-circle" style="font-size:2rem;opacity:.4;color:#198754;"></i>
        <p class="mt-2 mb-0 small text-muted">Query executed successfully — 0 rows returned</p>
        <p class="small text-muted">${fmtElapsed(elapsed)}</p>`;
      return;
    }

    rowCount.textContent = `${rows.length.toLocaleString()} rows`;
    rowCount.classList.remove('d-none');

    tableEl.classList.remove('d-none');
    showSqlPage(0);

  } catch (err) {
    emptyState.classList.add('d-none');
    errBox.classList.remove('d-none');
    errMsg.textContent = err.message ?? String(err);
    errCors.classList.toggle('d-none', !isCorsError(err));
  } finally {
    runBtn.disabled  = false;
    runBtn.innerHTML = '<i class="bi bi-play-fill"></i> Run <kbd class="ms-1" style="font-size:.65rem;">Ctrl+↵</kbd>';
  }
}

// ── Public API ────────────────────────────────────────────────────────────

/** Set the editor content */
export function setSqlEditorValue(sql) {
  _cm?.setValue(sql);
}

/** Get the current editor content */
export function getSqlEditorValue() {
  return _cm?.getValue() ?? '';
}

/**
 * Programmatically execute SQL from outside (e.g. clicking "Load Dataset").
 * Updates the SQL Editor results pane.
 */
export function executeInEditor(sql, runCallback) {
  if (_cm) _cm.setValue(sql);
  return _executeQuery(sql, runCallback);
}
