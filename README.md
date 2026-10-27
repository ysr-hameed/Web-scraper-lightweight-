# Lightweight SPA Scraper + Website Crawler

Node.js + TypeScript + Fastify + Playwright/Chromium. Designed to process one browser job at a time on small instances.

## Endpoints

- `GET /health` — queue/browser health.
- `POST /scrape` — scrape one URL (existing behavior).
- `POST /crawl` — asynchronously discover and crawl important pages from a website.
- `GET /crawl/:job_id` — poll a crawl job; when complete, the response includes the full structured result.

## Start a whole-site crawl

```bash
curl -sS -X POST 'https://YOUR-SERVICE.onrender.com/crawl' \
  -H 'content-type: application/json' \
  -d '{"url":"https://example.com","maxPages":20,"maxDepth":3}'
```

The response is `202 Accepted` and includes `job_id` and `status_url`. Poll the returned URL every 3 seconds until `status` becomes `completed` or `failed`:

```bash
curl -sS 'https://YOUR-SERVICE.onrender.com/crawl/JOB_ID' -o website-result.json
```

Save the completed JSON immediately. Crawl jobs/results are kept in memory and are lost on a service restart; this version does not persist crawl data to a database or object storage.

### Crawl request options

| Field | Default | Allowed / cap | Purpose |
|---|---:|---:|---|
| `url` | required | URL up to 2048 chars | Website starting point |
| `maxPages` | 20 | 1 to `MAX_CRAWL_PAGES` (default 50) | Maximum pages attempted |
| `maxDepth` | 3 | 0 to 5 | Link depth; sitemap pages start at depth 1 |
| `maxDurationMs` | 180000 | 10000 to `MAX_CRAWL_DURATION_MS` | Overall crawl duration budget |
| `maxTotalChars` | 300000 | 10000 to `MAX_CRAWL_TOTAL_CHARS` | Total extracted text budget |

The crawler starts with the homepage, checks `robots.txt` for sitemap declarations, tries common sitemap paths, then ranks discovered internal links. It prefers pricing, features, product, solutions/use cases, integrations, customer stories, FAQs, security, about and contact pages. It crawls only the same hostname, allowing the `www` variant, and ignores query strings to avoid filter/tracking URL loops. It does not crawl external domains or subdomains by default.

## Output structure

The completed crawl response includes:

- `website`: canonical starting URL, title, description, language and social metadata.
- `crawl`: status, limits, counts, duration, sitemap discovery details, truncation flag and failures.
- `site_profile.page_counts_by_type`: page counts by heuristic category.
- `site_profile.key_pages`: important pages grouped by category.
- `site_profile.evidence`: quote-like extracted evidence for value proposition, features, pricing, use cases, integrations, audience, FAQ, security, social proof, CTAs, company/about and content topics. Every item includes its source URL and title.
- `site_profile.structured_data_summary`: JSON-LD type counts, selected organization/product/service/offer/etc. entities and FAQ question/answer entries.
- `page_inventory`: compact per-page inventory with title, description, category and headings.
- `pages`: per-page text, headings, links, buttons, images, raw structured data and metadata.

Use evidence items with `source_url` to ground post generation. Page classifications and evidence categories are heuristics, not verified facts. Never invent features, prices, integrations, customer claims or guarantees that the site does not substantiate.

## Install / run

Requires Node.js 20+. Keep Playwright npm version aligned with the official Playwright Docker image.

```bash
npm install
npm run build
npm start
```

The included Dockerfile uses `mcr.microsoft.com/playwright:v1.64.1-noble`, installs dependencies, builds TypeScript, and runs as the image's non-root `pwuser` account.

## Environment variables

- `HOST` default `127.0.0.1`
- `PORT` default `3000`
- `MAX_JOBS` default `50` (recycle Chromium after this many successful page scrapes)
- `NAV_TIMEOUT_MS` default `20000`
- `READY_TIMEOUT_MS` default `8000`
- `MAX_TEXT_CHARS` default `80000` (single-page scrape)
- `MAX_LINKS` default `500`
- `MAX_CRAWL_PAGES` default `50`
- `CRAWL_PAGE_TEXT_CHARS` default `25000`
- `MAX_CRAWL_TOTAL_CHARS` default `300000`
- `MAX_CRAWL_DURATION_MS` default `180000`
- `MAX_CRAWL_JOBS` default `3` active/queued crawls
- `MAX_CRAWL_JOB_HISTORY` default `5` completed/failed job results retained in memory

## Safety and operational notes

1. Protect this API with authentication/rate limits before public multi-tenant use.
2. The crawler targets public websites. URL/DNS checks are defense-in-depth, not a complete SSRF boundary; production deployments should isolate browser egress at the network/container level.
3. The service uses one queued browser job at a time. A whole-site crawl occupies that queue until it finishes; concurrent requests wait or may be rejected if the queue is full.
4. `content_complete` and crawl status are heuristics. Some sites require interaction, scrolling, authentication, or site-specific logic.
5. Crawl job results are in memory only. A Render restart loses active jobs and completed results. Persist results to durable storage before using this for customer data.
6. Keep crawl limits low on Render Free. Start with 10–20 pages, inspect response quality, then increase only after stability testing.
