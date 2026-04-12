/**
 * catalog.js — Overture Maps dataset catalog and URL generation
 *
 * Defines all themes/types available in Overture Maps releases and
 * generates the correct parquet glob URLs for DuckDB-WASM to query.
 *
 * S3 path format:
 *   s3://overturemaps-us-west-2/release/{version}/theme={theme}/type={type}/
 * HTTP equivalent:
 *   https://overturemaps-us-west-2.s3.amazonaws.com/release/{version}/theme={theme}/type={type}/*.parquet
 */

export const S3_BASE = 'https://overturemaps-us-west-2.s3.amazonaws.com';

export const RELEASES = [
  { value: '2026-03-18.0', label: '2026-03-18.0 (Latest)' },
  { value: '2026-02-18.0', label: '2026-02-18.0' },
];

/**
 * Full catalog of Overture Maps themes and their types.
 * Each type includes display metadata and the key columns available.
 */
export const CATALOG = {
  places: {
    label: 'Places (POIs)',
    icon: 'bi-geo-alt-fill',
    description: '60M+ points of interest worldwide — restaurants, shops, parks, etc.',
    types: {
      place: {
        label: 'Place',
        description: '60M+ POIs. Key columns: names.primary, categories.primary, confidence, websites, socials',
        geometryType: 'Point',
        keyColumns: ['id', 'names', 'categories', 'confidence', 'addresses', 'websites', 'socials', 'phones', 'bbox', 'geometry'],
        sampleWhere: "categories.primary = 'restaurant'",
      },
    },
  },

  buildings: {
    label: 'Buildings',
    icon: 'bi-building',
    description: '2.3 billion building footprints globally.',
    types: {
      building: {
        label: 'Building',
        description: '2.3B building footprints. Key columns: height, num_floors, class, names',
        geometryType: 'Polygon',
        keyColumns: ['id', 'names', 'class', 'height', 'num_floors', 'facade_material', 'roof_material', 'bbox', 'geometry'],
        sampleWhere: 'height > 100',
      },
      building_part: {
        label: 'Building Part',
        description: 'Sub-components of buildings. Key columns: height, building_id',
        geometryType: 'Polygon',
        keyColumns: ['id', 'building_id', 'height', 'num_floors', 'bbox', 'geometry'],
        sampleWhere: null,
      },
    },
  },

  divisions: {
    label: 'Divisions (Admin Boundaries)',
    icon: 'bi-compass',
    description: 'Country, state, county, city, and neighbourhood boundaries.',
    types: {
      division: {
        label: 'Division',
        description: 'Administrative units (country, region, county, city). Key columns: names, subtype, admin_level, country',
        geometryType: 'Point',
        keyColumns: ['id', 'names', 'subtype', 'admin_level', 'country', 'region', 'parent_division_id', 'bbox', 'geometry'],
        sampleWhere: "subtype = 'country'",
      },
      division_area: {
        label: 'Division Area',
        description: 'Polygon areas for administrative units.',
        geometryType: 'Polygon',
        keyColumns: ['id', 'division_id', 'subtype', 'country', 'region', 'bbox', 'geometry'],
        sampleWhere: "subtype = 'county'",
      },
      division_boundary: {
        label: 'Division Boundary',
        description: 'LineString boundaries between administrative units.',
        geometryType: 'LineString',
        keyColumns: ['id', 'subtype', 'admin_level', 'left_division_id', 'right_division_id', 'bbox', 'geometry'],
        sampleWhere: null,
      },
    },
  },

  transportation: {
    label: 'Transportation',
    icon: 'bi-sign-intersection-fill',
    description: 'Roads, paths, railways, and their connectors.',
    types: {
      segment: {
        label: 'Segment',
        description: 'Road and transport segments. Key columns: class, subclass, names, speed_limits, access_restrictions',
        geometryType: 'LineString',
        keyColumns: ['id', 'names', 'class', 'subclass', 'connector_ids', 'speed_limits', 'access_restrictions', 'bbox', 'geometry'],
        sampleWhere: "class = 'primary'",
      },
      connector: {
        label: 'Connector',
        description: 'Network junction points where segments meet.',
        geometryType: 'Point',
        keyColumns: ['id', 'bbox', 'geometry'],
        sampleWhere: null,
      },
    },
  },

  base: {
    label: 'Base (Land & Water)',
    icon: 'bi-water',
    description: 'Land, water, land cover, and land use polygons.',
    types: {
      land: {
        label: 'Land',
        description: 'Land polygons. Key columns: subtype, class, names',
        geometryType: 'Polygon',
        keyColumns: ['id', 'names', 'subtype', 'class', 'bbox', 'geometry'],
        sampleWhere: null,
      },
      land_cover: {
        label: 'Land Cover',
        description: 'Vegetation, snow, barren and other cover types.',
        geometryType: 'Polygon',
        keyColumns: ['id', 'subtype', 'cartography', 'bbox', 'geometry'],
        sampleWhere: "subtype = 'forest'",
      },
      land_use: {
        label: 'Land Use',
        description: 'Urban, agricultural, and natural land use areas.',
        geometryType: 'Polygon',
        keyColumns: ['id', 'subtype', 'class', 'names', 'bbox', 'geometry'],
        sampleWhere: "class = 'residential'",
      },
      water: {
        label: 'Water',
        description: 'Rivers, lakes, oceans, and other water bodies.',
        geometryType: 'Polygon/LineString',
        keyColumns: ['id', 'subtype', 'class', 'names', 'is_salt', 'is_intermittent', 'bbox', 'geometry'],
        sampleWhere: "subtype = 'lake'",
      },
    },
  },

  addresses: {
    label: 'Addresses',
    icon: 'bi-mailbox',
    description: 'Global address points with structured address components.',
    types: {
      address: {
        label: 'Address',
        description: 'Physical addresses. Key columns: freeform, number, street, unit, city, region, country, postcode',
        geometryType: 'Point',
        keyColumns: ['id', 'freeform', 'number', 'street', 'unit', 'city', 'region', 'postcode', 'country', 'bbox', 'geometry'],
        sampleWhere: "country = 'US'",
      },
    },
  },

  infrastructure: {
    label: 'Infrastructure',
    icon: 'bi-transmission-tower',
    description: 'Towers, power lines, barriers, piers, and other infrastructure.',
    types: {
      infrastructure: {
        label: 'Infrastructure',
        description: 'Physical infrastructure features. Key columns: subtype, class, names',
        geometryType: 'Mixed',
        keyColumns: ['id', 'names', 'subtype', 'class', 'sources', 'bbox', 'geometry'],
        sampleWhere: "subtype = 'tower'",
      },
    },
  },
};

/**
 * Build the Parquet glob URL preview for a given theme/type/release.
 * Used only for the sidebar URL preview — actual loading uses fetchParquetFileUrls.
 *
 * @param {string} theme
 * @param {string} type
 * @param {string} release
 * @param {string} proxyUrl
 * @param {string} proxyPattern - 'path' | 'param'
 * @returns {string}
 */
export function buildParquetUrl(theme, type, release, proxyUrl = '', proxyPattern = 'path') {
  const s3Path = `/release/${release}/theme=${theme}/type=${type}/*.parquet`;
  const s3Url = `${S3_BASE}${s3Path}`;
  if (!proxyUrl) return s3Url;
  const base = proxyUrl.replace(/\/$/, '');
  if (proxyPattern === 'param') return `${base}?url=${encodeURIComponent(s3Url)}`;
  return `${base}/${s3Url}`;
}

/**
 * Resolve the exact list of parquet file URLs by calling the S3 ListObjectsV2
 * API through the proxy.  DuckDB-WASM cannot expand *.parquet globs over plain
 * HTTP URLs, so we must enumerate concrete file paths before querying.
 *
 * Always uses param-mode for the listing request so the S3 query string
 * (list-type, prefix) is forwarded correctly through the Cloudflare worker.
 *
 * @param {string} theme
 * @param {string} type
 * @param {string} release
 * @param {string} proxyUrl
 * @param {string} proxyPattern - 'path' | 'param'
 * @returns {Promise<string[]>} Array of proxied URLs for each parquet file
 */
export async function fetchParquetFileUrls(theme, type, release, proxyUrl = '', proxyPattern = 'path') {
  const prefix = `release/${release}/theme=${theme}/type=${type}/`;
  const base   = proxyUrl.replace(/\/$/, '');
  const keys   = [];
  let continuationToken = null;

  do {
    let s3ListUrl = `${S3_BASE}/?list-type=2&max-keys=1000&prefix=${encodeURIComponent(prefix)}`;
    if (continuationToken) {
      s3ListUrl += `&continuation-token=${encodeURIComponent(continuationToken)}`;
    }

    // Always use param mode for listing — path mode forfeits the S3 query string
    const fetchUrl = proxyUrl
      ? `${base}?url=${encodeURIComponent(s3ListUrl)}`
      : s3ListUrl;

    const resp = await fetch(fetchUrl);
    if (!resp.ok) throw new Error(`S3 listing failed (${resp.status}): ${resp.statusText}`);
    const xml = await resp.text();

    [...xml.matchAll(/<Key>([^<]+\.parquet)<\/Key>/g)].forEach(m => keys.push(m[1]));

    if (/<IsTruncated>true<\/IsTruncated>/.test(xml)) {
      const m = xml.match(/<NextContinuationToken>([^<]+)<\/NextContinuationToken>/);
      continuationToken = m ? m[1] : null;
    } else {
      continuationToken = null;
    }
  } while (continuationToken);

  if (!keys.length) {
    throw new Error(`No parquet files found for ${theme}/${type} in release ${release}.`);
  }

  return keys.map(key => {
    const s3Url = `${S3_BASE}/${key}`;
    if (!proxyUrl) return s3Url;
    if (proxyPattern === 'param') return `${base}?url=${encodeURIComponent(s3Url)}`;
    return `${base}/${s3Url}`;
  });
}

/**
 * Generate the CREATE TABLE SQL for loading a dataset into explorer_result.
 *
 * @param {object} opts
 * @param {string[]} opts.urls     - Explicit parquet URLs from fetchParquetFileUrls
 * @param {number|null} opts.xmin
 * @param {number|null} opts.xmax
 * @param {number|null} opts.ymin
 * @param {number|null} opts.ymax
 * @param {number} opts.limit
 * @param {boolean} opts.includeWkt
 * @returns {string} SQL string
 */
export function buildLoadSQL(opts) {
  const { urls, xmin, xmax, ymin, ymax, limit, includeWkt } = opts;

  const urlList = urls.length === 1
    ? `'${urls[0]}'`
    : `[\n  '${urls.join("',\n  '")}',\n]`;

  const hasBbox = xmin != null && xmax != null && ymin != null && ymax != null;
  const bboxWhere = hasBbox
    ? `\nWHERE bbox.xmin >= ${xmin}\n  AND bbox.xmax <= ${xmax}\n  AND bbox.ymin >= ${ymin}\n  AND bbox.ymax <= ${ymax}`
    : '';

  const wktSelect = includeWkt ? ',\n  ST_AsText(geometry) AS geom_wkt' : '';

  return `CREATE OR REPLACE TABLE explorer_result AS\nSELECT *${wktSelect}\nFROM read_parquet(${urlList}, hive_partitioning=true)${bboxWhere}\nLIMIT ${limit};`;
}

/**
 * Return the catalog entry for a theme/type, or null.
 */
export function getCatalogEntry(theme, type) {
  return CATALOG[theme]?.types[type] ?? null;
}

/**
 * Populate a <select> element with theme options.
 */
export function populateThemeSelect(selectEl) {
  selectEl.innerHTML = Object.entries(CATALOG)
    .map(([key, t]) => `<option value="${key}">${t.label}</option>`)
    .join('');
}

/**
 * Populate a <select> element with type options for a given theme.
 */
export function populateTypeSelect(selectEl, theme) {
  const types = CATALOG[theme]?.types ?? {};
  selectEl.innerHTML = Object.entries(types)
    .map(([key, t]) => `<option value="${key}">${t.label}</option>`)
    .join('');
}
