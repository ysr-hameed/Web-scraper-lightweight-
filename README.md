# Lightweight SPA Scraper

A small Node.js + TypeScript API using Playwright and Chromium for extracting readable content from JavaScript-heavy React/Vue/Angular websites.

Designed for sequential jobs on small VPS machines. It is not a guarantee that every website can be rendered within 1 GB RAM.

## Features

- Persistent Chromium process; one active scrape at a time.
- Fresh browser context per URL.
- JavaScript enabled; waits for useful body text rather than `networkidle`.
- Blocks images, fonts, media and common analytics requests.
- Extracts title, description, canonical URL, language, headings, readable body text, links, images, buttons, JSON-LD and Open Graph metadata.
- Returns Markdown and structured JSON.
- Bounded queue and periodic browser recycling.
- Basic SSRF protections against localhost/private IP destinations.

## Requirements

Node.js 20+ recommended. Install dependencies:

```bash
npm install
npx playwright install --with-deps --only-shell chromium
npm run build
```

On a small Linux VPS, system dependencies may take substantial disk space. The Dockerfile uses the official Playwright image, which is convenient but larger on disk than a custom minimal image.

Start:

```bash
HOST=0.0.0.0 PORT=3000 npm start
```

For development:

```bash
npm run dev
```

## API

Health:

```bash
curl http://127.0.0.1:3000/health
```

Scrape:

```bash
curl -X POST http://127.0.0.1:3000/scrape \
  -H 'content-type: application/json' \
  -d '{"url":"https://example.com"}'
```

The `/scrape` endpoint returns JSON with `content_markdown`, `body_text`, `headings`, `links`, `structured_data`, metadata and extraction warnings.

## Environment variables

- `HOST` default `127.0.0.1`
- `PORT` default `3000`
- `MAX_JOBS` default `50` (recycle Chromium after this many successful jobs)
- `NAV_TIMEOUT_MS` default `20000`
- `READY_TIMEOUT_MS` default `8000`
- `MAX_TEXT_CHARS` default `80000`
- `MAX_LINKS` default `500`
- `MAX_RESPONSE_BYTES` reserved for future response-size enforcement; currently not applied to Chromium response bodies

## Important production notes

1. Put authentication or an API key in front of this endpoint before exposing it publicly.
2. Add request rate limiting and HTTPS at your reverse proxy.
3. This is intended for public websites. The URL validation is defense-in-depth, not a complete network security boundary. For a public multi-tenant service, run the browser in a separate restricted container/network with outbound access controls.
4. The current version blocks images/fonts/media but leaves scripts and API requests enabled so SPAs can render. Some sites need exceptions.
5. `content_complete` is a heuristic, not proof that all page content was captured.
6. Some SPAs require clicks, scrolling, authentication or specific API calls. Add site-specific strategies where needed.
7. Measure peak RSS and OOM events on your actual VPS before opening the service to customers.
8. Use a non-root runtime and keep Chromium's sandbox enabled. Do not expose a raw browser debugging port.
# Web-scraper-lightweight-
# Web-scraper-lightweight-
