/**
 * Cloudflare Worker — CORS Proxy for Overture Maps S3
 * =====================================================
 *
 * SETUP (Cloudflare Dashboard — no CLI needed):
 *  1. Go to https://dash.cloudflare.com → Workers & Pages → Create
 *  2. Choose "Create Worker"
 *  3. Replace the default code with this entire file
 *  4. Click "Deploy"
 *  5. Copy the worker URL (e.g. https://ky-overture-proxy.YOUR_SUBDOMAIN.workers.dev)
 *  6. Paste it into the KY Explorer Settings → CORS Proxy field
 *
 * HOW IT WORKS:
 *  DuckDB-WASM issues HTTP requests like:
 *    GET https://your-worker.workers.dev/https://overturemaps-us-west-2.s3.amazonaws.com/...
 *      Range: bytes=0-8191
 *
 *  The worker strips its own hostname prefix, forwards the request to S3
 *  with the Range header intact, and adds CORS headers to the response.
 *  DuckDB receives the response as if it came from the same origin.
 *
 * SECURITY:
 *  - Only proxies requests to the Overture Maps S3 bucket (allowlisted).
 *  - Returns 403 for any other target host.
 *  - No credentials or secrets required — the S3 bucket is public.
 *
 * COST:
 *  Cloudflare's free plan: 100,000 requests/day.
 *  Each DuckDB range request counts as one Worker invocation.
 *  Typical page load (small bbox): ~20–50 requests.
 */

// Only pass through requests to this S3 host
const ALLOWED_HOST = 'overturemaps-us-west-2.s3.amazonaws.com';

export default {
  async fetch(request) {
    const url = new URL(request.url);

    // ── CORS preflight ─────────────────────────────────────────────────
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(),
      });
    }

    // ── Extract target S3 URL from path ────────────────────────────────
    // Expected format: /https://overturemaps-us-west-2.s3.amazonaws.com/...
    // Strip the leading slash to get the full target URL.
    let targetUrl = url.pathname.slice(1);

    // Also support query param mode: ?url=https://...
    if (!targetUrl && url.searchParams.has('url')) {
      targetUrl = url.searchParams.get('url');
    }

    // Validate URL
    let target;
    try {
      target = new URL(targetUrl);
    } catch {
      return errorResponse(400, 'Invalid or missing target URL. Expected path: /{full-s3-url}');
    }

    // Security: only proxy to the known Overture Maps bucket
    if (target.hostname !== ALLOWED_HOST) {
      return errorResponse(403, `Forbidden: this proxy only allows requests to ${ALLOWED_HOST}`);
    }

    // ── Forward the request to S3 ──────────────────────────────────────
    const forwardHeaders = new Headers();

    // Pass through Range header — essential for DuckDB Parquet row-group fetching
    const rangeHeader = request.headers.get('Range');
    if (rangeHeader) {
      forwardHeaders.set('Range', rangeHeader);
    }

    // Pass through If-None-Match / If-Modified-Since for caching
    const ifNoneMatch = request.headers.get('If-None-Match');
    if (ifNoneMatch) forwardHeaders.set('If-None-Match', ifNoneMatch);

    let s3Response;
    try {
      // Do NOT use cf.cacheEverything when a Range header is present — Cloudflare
      // strips Range from cached requests, causing S3 to return the full file
      // instead of the requested byte slice. DuckDB-WASM depends on 206 responses.
      const cfOptions = rangeHeader
        ? { cacheTtl: 0 }  // bypass cache for range requests
        : {
            cacheEverything: true,
            cacheTtlByStatus: {
              '200-299': 86400,   // Cache full-file responses for 24h
              '400-499': 5,
              '500-599': 0,
            },
          };

      s3Response = await fetch(target.toString(), {
        method:  'GET',
        headers: forwardHeaders,
        cf:      cfOptions,
      });
    } catch (err) {
      return errorResponse(502, `Failed to reach S3: ${err.message}`);
    }

    // ── Build proxied response ─────────────────────────────────────────
    const responseHeaders = new Headers(corsHeaders());

    // Forward headers that DuckDB / browsers need
    const passthroughHeaders = [
      'Content-Type',
      'Content-Length',
      'Content-Range',
      'Accept-Ranges',
      'ETag',
      'Last-Modified',
      'Cache-Control',
    ];
    passthroughHeaders.forEach(h => {
      const val = s3Response.headers.get(h);
      if (val) responseHeaders.set(h, val);
    });

    return new Response(s3Response.body, {
      status:  s3Response.status,
      headers: responseHeaders,
    });
  },
};

// ── Helpers ───────────────────────────────────────────────────────────────

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
    'Access-Control-Allow-Headers': 'Range, If-None-Match, If-Modified-Since, Content-Type',
    'Access-Control-Expose-Headers': 'Content-Range, Accept-Ranges, Content-Length, ETag',
    'Access-Control-Max-Age': '86400',
  };
}

function errorResponse(status, message) {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders(),
    },
  });
}
