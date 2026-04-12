# KY Overture Explorer

A browser-based tool for exploring, querying, and exporting [Overture Maps](https://overturemaps.org/) datasets — no backend required. Powered by [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview.html), hosted on GitHub Pages.

Inspired by: [SQL in the Browser: Querying 2.3 Billion Records with Zero Backend](https://marchen.co/blog/browser-sql-no-backend)

---

## Features

| Feature | Description |
|---|---|
| **Dataset Browser** | Filter and select any Overture Maps theme/type and release version |
| **Spatial BBox Filter** | Limit queries by bounding box — preset cities or custom coordinates |
| **Results Table** | Paginated, sortable results with NULL / number / geometry cell formatting |
| **SQL Editor** | Full CodeMirror editor with syntax highlighting, autocomplete, query history |
| **Map Preview** | Leaflet map rendering of point geometry from loaded datasets |
| **Export** | Export to Parquet, CSV, JSON, GeoJSON, or Arrow IPC with one click |
| **Converter** | Upload any local Parquet/GeoParquet file and convert it to another format |

---

## Architecture

```
GitHub Pages (static)
├── index.html          — UI shell (Bootstrap 5, CodeMirror, Leaflet)
├── css/app.css         — Custom dark-theme styles
└── js/
    ├── app.js          — Main application, event wiring
    ├── catalog.js      — Overture Maps dataset catalog + URL generation
    ├── duckdb-init.js  — DuckDB-WASM singleton, query helpers
    ├── sql-editor.js   — CodeMirror editor, execution, history, table render
    └── exporter.js     — Export & Parquet converter

Cloudflare Worker (proxy/)
└── cloudflare-worker.js  — CORS proxy for Overture Maps S3 bucket
```

**Data flow:**
```
User selects dataset
  → DuckDB-WASM builds SQL with read_parquet('https://...')
  → HTTP range requests → Cloudflare Worker → Overture Maps S3
  → DuckDB processes only the relevant Parquet row groups
  → Results rendered in the browser
```

---

## Getting Started

### 1. Deploy to GitHub Pages

Push this repository to GitHub, then enable **Pages** under *Settings → Pages → Deploy from branch → main / (root)*.

The app will be live at `https://<your-username>.github.io/ky-explorer-overturemaps/`.

### 2. Cloudflare Worker (CORS proxy for S3 access)

The Overture Maps S3 bucket blocks direct browser requests (CORS). A Cloudflare Worker proxy is required to relay them.

**A default shared proxy is pre-configured in the app** — you can start querying immediately without deploying your own. If you expect heavy usage or want full control, deploy your own worker:

**Steps (no CLI required — use the Cloudflare dashboard):**

1. Go to [dash.cloudflare.com](https://dash.cloudflare.com) → **Workers & Pages → Create**
2. Choose **Create Worker**
3. Paste the entire contents of `proxy/cloudflare-worker.js` into the editor
4. Click **Deploy**
5. Copy the worker URL shown (e.g. `https://ky-overture-proxy.YOUR_SUBDOMAIN.workers.dev`)

**Point the app to your worker:**

1. Open the app in your browser
2. Click the gear icon (⚙) in the top-right nav
3. Paste your worker URL into **CORS Proxy Base URL**
4. Click **Test Connection**, then **Save Settings**

> The proxy only passes requests through to `overturemaps-us-west-2.s3.amazonaws.com` — all other targets return 403.
> Cloudflare's free plan allows 100,000 requests/day.

---

## Usage

### Loading a Dataset

1. Select a **Theme** (e.g. *Places*) and **Type** (e.g. *Place*) from the sidebar
2. Optionally select a **BBox** preset (city) or enter custom coordinates
3. Adjust the **Row Limit** slider
4. Click **Load Dataset**

Initial queries to a new area take 3–15 seconds depending on the dataset size and your bounding box. Subsequent queries against the loaded `explorer_result` table are instant.

### SQL Editor

Click the **SQL Editor** tab to write arbitrary SQL against any loaded table:

```sql
-- After loading Places for LA:
SELECT categories.primary AS category, COUNT(*) AS count
FROM explorer_result
GROUP BY category
ORDER BY count DESC
LIMIT 20;
```

The loaded table is always named `explorer_result`. Use `SHOW TABLES` to see everything currently in DuckDB.

### Export

On the **Export & Convert** tab:

- **Export Query Results** — downloads `explorer_result` (or custom SQL) as Parquet, CSV, JSON, GeoJSON, or Arrow
- **Convert Parquet** — upload a local `.parquet` file and convert it to any format

### Map Preview

After loading a dataset with point geometry, click **Map Preview → Render Points** to visualize the data on an interactive Leaflet map.

---

## Overture Maps Dataset Reference

| Theme | Types | Approx. Size |
|---|---|---|
| places | place | ~60M POIs, ~8GB |
| buildings | building, building_part | ~2.3B footprints, 500GB+ |
| divisions | division, division_area, division_boundary | Admin boundaries |
| transportation | segment, connector | Roads, paths |
| base | land, land_cover, land_use, water | Polygons |
| addresses | address | Global address points |
| infrastructure | infrastructure | Towers, power lines, etc. |

Overture releases new snapshots roughly every 2 months. The current latest release is `2026-03-18.0`. Select the release version from the sidebar.

---

## Development

No build step required. All dependencies are loaded from CDN.

```bash
# Serve locally (any static server)
npx serve .
# or
python -m http.server 8080
```

Then open `http://localhost:8080`.

---

## Credits

- [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview.html)
- [Overture Maps Foundation](https://overturemaps.org/)
- [Bootstrap 5](https://getbootstrap.com/)
- [CodeMirror 5](https://codemirror.net/)
- [Leaflet.js](https://leafletjs.com/)
- Article inspiration: [marchen.co](https://marchen.co/blog/browser-sql-no-backend)

