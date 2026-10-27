import Fastify from "fastify";
import { chromium, type Browser, type BrowserContext, type Page, type Route } from "playwright";
import PQueue from "p-queue";
import dns from "node:dns/promises";
import net from "node:net";
import { randomUUID } from "node:crypto";

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? "127.0.0.1";
const MAX_JOBS = Number(process.env.MAX_JOBS ?? 50);
const NAV_TIMEOUT_MS = Number(process.env.NAV_TIMEOUT_MS ?? 20_000);
const READY_TIMEOUT_MS = Number(process.env.READY_TIMEOUT_MS ?? 8_000);
const MAX_TEXT_CHARS = Number(process.env.MAX_TEXT_CHARS ?? 80_000);
const MAX_LINKS = Number(process.env.MAX_LINKS ?? 500);
const MAX_CRAWL_PAGES = Number(process.env.MAX_CRAWL_PAGES ?? 25);
const CRAWL_PAGE_TEXT_CHARS = Number(process.env.CRAWL_PAGE_TEXT_CHARS ?? 12_000);
const MAX_CRAWL_TOTAL_CHARS = Number(process.env.MAX_CRAWL_TOTAL_CHARS ?? 180_000);
const MAX_CRAWL_DURATION_MS = Number(process.env.MAX_CRAWL_DURATION_MS ?? 150_000);
const MAX_CRAWL_JOBS = Number(process.env.MAX_CRAWL_JOBS ?? 1);
const MAX_CRAWL_JOB_HISTORY = Number(process.env.MAX_CRAWL_JOB_HISTORY ?? 3);
const MAX_REQUESTS_PER_PAGE = Math.max(30, Math.min(250, Number(process.env.MAX_REQUESTS_PER_PAGE ?? 120)));

const app = Fastify({
  logger: true,
  bodyLimit: 16 * 1024,
  requestTimeout: 30_000
});

// One active browser job at a time to protect a 1 GB VPS.
const queue = new PQueue({ concurrency: 1 });
let browser: Browser | undefined;
let completedJobs = 0;
let browserStartedAt: string | null = null;

type CrawlJob = {
  id: string;
  status: "queued" | "running" | "completed" | "failed";
  created_at: string;
  updated_at: string;
  request: { url: string; max_pages: number; max_depth: number; max_duration_ms: number; max_total_chars: number };
  result?: Record<string, unknown>;
  error?: string;
};
const crawlJobs = new Map<string, CrawlJob>();

function ipv4ToInt(ip: string): number {
  return ip.split(".").reduce((acc, part) => ((acc << 8) | Number(part)) >>> 0, 0);
}

function isPrivateIPv4(ip: string): boolean {
  const n = ipv4ToInt(ip);
  const ranges: Array<[string, number]> = [
    ["0.0.0.0", 8], ["10.0.0.0", 8], ["100.64.0.0", 10],
    ["127.0.0.0", 8], ["169.254.0.0", 16], ["172.16.0.0", 12],
    ["192.0.0.0", 24], ["192.0.2.0", 24], ["192.168.0.0", 16],
    ["198.18.0.0", 15], ["198.51.100.0", 24], ["203.0.113.0", 24],
    ["224.0.0.0", 4], ["240.0.0.0", 4]
  ];
  return ranges.some(([base, bits]) => {
    const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
    return (n & mask) === (ipv4ToInt(base) & mask);
  });
}

function isPrivateIPv6(ip: string): boolean {
  const s = ip.toLowerCase().split("%")[0];
  return s === "::" || s === "::1" || s.startsWith("fc") || s.startsWith("fd") ||
    /^fe[89ab]/.test(s) || s.startsWith("ff") || s.startsWith("2001:db8:");
}

async function assertPublicHttpUrl(raw: string): Promise<URL> {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error("Invalid URL");
  }

  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("Only http:// and https:// URLs are allowed");
  }
  if (url.username || url.password) throw new Error("URLs containing credentials are not allowed");

  const host = url.hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (host === "localhost" || host.endsWith(".localhost") || host.endsWith(".local") ||
      host === "metadata.google.internal" || host === "metadata") {
    throw new Error("Private or local hosts are not allowed");
  }

  const ipType = net.isIP(host);
  if (ipType === 4 && isPrivateIPv4(host)) throw new Error("Private IP addresses are not allowed");
  if (ipType === 6 && isPrivateIPv6(host)) throw new Error("Private IP addresses are not allowed");

  if (!ipType) {
    let addresses: Array<{ address: string; family: number }>;
    try {
      addresses = await dns.lookup(host, { all: true, verbatim: true });
    } catch {
      throw new Error("Could not resolve hostname");
    }
    if (!addresses.length) throw new Error("Hostname did not resolve");
    for (const item of addresses) {
      if ((item.family === 4 && isPrivateIPv4(item.address)) ||
          (item.family === 6 && isPrivateIPv6(item.address))) {
        throw new Error("Hostname resolves to a private or reserved IP");
      }
    }
  }
  url.hash = "";
  return url;
}

async function launchBrowser(): Promise<Browser> {
  const b = await chromium.launch({
    headless: true,
    // Keep Chromium's sandbox enabled. Run this service as a non-root user.
    args: [
      "--disable-dev-shm-usage",
      "--disable-background-networking",
      "--disable-component-update",
      "--disable-default-apps",
      "--disable-extensions",
      "--disable-sync",
      "--metrics-recording-only",
      "--no-first-run"
    ]
  });
  browserStartedAt = new Date().toISOString();
  return b;
}

async function getBrowser(): Promise<Browser> {
  if (!browser || !browser.isConnected()) {
    browser = await launchBrowser();
  }
  return browser;
}

function trimText(value: string, max = MAX_TEXT_CHARS): string {
  const normalized = value.replace(/\u00a0/g, " ").replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n").trim();
  return normalized.length > max ? normalized.slice(0, max) : normalized;
}

async function scrapeOne(rawUrl: string, maxTextChars = MAX_TEXT_CHARS, maxLinks = MAX_LINKS) {
  const initialUrl = await assertPublicHttpUrl(rawUrl);
  const b = await getBrowser();
  let context: BrowserContext | undefined;
  let page: Page | undefined;
  const started = Date.now();
  let navigationStatus: number | null = null;
  let navigationError: string | null = null;
  const blocked: Record<string, number> = {};
  let requestCount = 0;

  try {
    context = await b.newContext({
      viewport: { width: 1280, height: 800 },
      serviceWorkers: "block",
      javaScriptEnabled: true,
      acceptDownloads: false,
      ignoreHTTPSErrors: false
    });
    page = await context.newPage();
    page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
    page.setDefaultTimeout(READY_TIMEOUT_MS);

    // Validate every requested URL, including redirects and subresources, to reduce SSRF risk.
    await page.route("**/*", async (route: Route) => {
      const request = route.request();
      requestCount += 1;
      if (requestCount > MAX_REQUESTS_PER_PAGE) {
        blocked["request_limit"] = (blocked["request_limit"] ?? 0) + 1;
        await route.abort("blockedbyclient").catch(() => {});
        return;
      }
      const requestUrl = request.url();
      try {
        await assertPublicHttpUrl(requestUrl);
      } catch {
        blocked["private_or_invalid_url"] = (blocked["private_or_invalid_url"] ?? 0) + 1;
        await route.abort("blockedbyclient").catch(() => {});
        return;
      }

      const type = request.resourceType();
      const url = requestUrl.toLowerCase();

      // Images/fonts/media are unnecessary for text-first extraction.
      // Keep scripts, stylesheets, documents, XHR/fetch and GraphQL traffic.
      if (["image", "font", "media"].includes(type)) {
        blocked[type] = (blocked[type] ?? 0) + 1;
        await route.abort().catch(() => {});
        return;
      }

      // Optional noisy third-party requests. Keep this conservative; many SPAs use APIs.
      if (/google-analytics\.com|googletagmanager\.com|doubleclick\.net|facebook\.com\/tr\//.test(url)) {
        blocked["analytics"] = (blocked["analytics"] ?? 0) + 1;
        await route.abort().catch(() => {});
        return;
      }

      const headers = request.headers();
      // A response body cap is enforced for the main document below. Browser subresources
      // are constrained by timeouts and blocked resource types.
      await route.continue({ headers }).catch(() => {});
    });

    try {
      const response = await page.goto(initialUrl.toString(), {
        waitUntil: "domcontentloaded",
        timeout: NAV_TIMEOUT_MS
      });
      navigationStatus = response?.status() ?? null;
    } catch (err) {
      navigationError = err instanceof Error ? err.message : String(err);
      // Some SPAs keep long-running requests; attempt extraction if a document loaded.
      if (page.url() === "about:blank") throw err;
    }

    // Wait for rendered body text, not networkidle (SPAs often keep connections open).
    await page.waitForFunction(
      () => (document.body?.innerText?.trim().length ?? 0) > 80,
      undefined,
      { timeout: READY_TIMEOUT_MS }
    ).catch(() => {});

    // Give hydration a short, bounded settling window without waiting for every request.
    await page.waitForTimeout(350);

    const extracted = await page.evaluate(({ maxText, maxLinks }) => {
      const clean = (s: string | null | undefined) => (s ?? "").replace(/\s+/g, " ").trim();
      const meta = (name: string) =>
        document.querySelector<HTMLMetaElement>(`meta[name="${CSS.escape(name)}"]`)?.content ?? null;
      const property = (name: string) =>
        document.querySelector<HTMLMetaElement>(`meta[property="${CSS.escape(name)}"]`)?.content ?? null;

      const title = clean(document.title);
      const description = meta("description") || property("og:description");
      const canonical = document.querySelector<HTMLLinkElement>('link[rel="canonical"]')?.href ?? null;
      const headings = Array.from(document.querySelectorAll("h1,h2,h3,h4"))
        .map(el => ({ level: Number(el.tagName.slice(1)), text: clean(el.textContent) }))
        .filter(x => x.text)
        .slice(0, 300);

      const links = Array.from(document.querySelectorAll<HTMLAnchorElement>("a[href]"))
        .map(a => ({
          text: clean(a.innerText || a.getAttribute("aria-label") || a.title),
          url: a.href
        }))
        .filter(x => x.url && x.url.startsWith("http"))
        .filter((x, i, arr) => arr.findIndex(y => y.url === x.url) === i)
        .slice(0, maxLinks);

      const images = Array.from(document.querySelectorAll<HTMLImageElement>("img"))
        .map(img => ({
          alt: clean(img.alt),
          src: img.currentSrc || img.src
        }))
        .filter(x => x.src)
        .slice(0, 200);

      const jsonLd = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
        .map(s => {
          try { return JSON.parse(s.textContent || "null"); }
          catch { return { parse_error: true, raw: (s.textContent || "").slice(0, 2000) }; }
        }).slice(0, 50);

      // Remove non-content regions from a clone, leaving the live page untouched.
      const clone = document.body?.cloneNode(true) as HTMLElement | undefined;
      clone?.querySelectorAll("script,style,noscript,svg,canvas,iframe,nav,footer,header,aside,pre,code,button,input,select,textarea,[role=navigation],[aria-hidden=true],[role=menu],[role=dialog]")
        .forEach(el => el.remove());
      const bodyText = (clone?.innerText || document.body?.innerText || "")
        .replace(/\u00a0/g, " ")
        .replace(/[ \t]+\n/g, "\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim()
        .slice(0, maxText);

      const buttons = Array.from(document.querySelectorAll("button,[role=button],input[type=submit]"))
        .map(el => clean((el as HTMLElement).innerText || (el as HTMLInputElement).value ||
          el.getAttribute("aria-label") || el.getAttribute("title")))
        .filter(Boolean)
        .slice(0, 100);

      return {
        title,
        description,
        canonical_url: canonical,
        language: document.documentElement.lang || null,
        headings,
        body_text: bodyText,
        links,
        images,
        buttons,
        json_ld: jsonLd,
        page_title: property("og:title") || title,
        og_image: property("og:image"),
        robots: meta("robots")
      };
    }, { maxText: maxTextChars, maxLinks });

    const finalUrl = page.url();
    // Validate the final URL too; don't return content after a redirect to a private host.
    await assertPublicHttpUrl(finalUrl);

    const markdown = [
      `# ${extracted.title || finalUrl}`,
      extracted.description ? `\n> ${extracted.description}` : "",
      "\n## Page content",
      extracted.body_text || "(No readable body text extracted)",
      extracted.headings.length ? "\n## Headings\n" + extracted.headings.map(h => `${"#".repeat(h.level)} ${h.text}`).join("\n") : "",
      extracted.links.length ? "\n## Internal and external links\n" + extracted.links.map(l => `- [${l.text || l.url}](${l.url})`).join("\n") : ""
    ].filter(Boolean).join("\n");

    completedJobs += 1;
    const result = {
      url: finalUrl,
      requested_url: initialUrl.toString(),
      status: navigationStatus,
      title: extracted.title,
      description: extracted.description,
      canonical_url: extracted.canonical_url,
      language: extracted.language,
      headings: extracted.headings,
      content_markdown: markdown,
      body_text: extracted.body_text,
      links: extracted.links,
      images: extracted.images,
      buttons: extracted.buttons,
      structured_data: extracted.json_ld,
      social_metadata: {
        og_title: extracted.page_title,
        og_image: extracted.og_image,
        robots: extracted.robots
      },
      extraction: {
        method: "playwright_chromium_headless",
        javascript_enabled: true,
        content_complete: extracted.body_text.length > 100,
        warnings: [
          ...(navigationError ? [`Navigation warning: ${navigationError}`] : []),
          ...(extracted.body_text.length <= 100 ? ["Very little body text was extracted; the page may need interaction or may block automation."] : [])
        ],
        blocked_resources: blocked,
        duration_ms: Date.now() - started
      }
    };

    return result;
  } finally {
    await context?.close().catch(() => {});
    // Recycle periodically to reduce long-running memory growth.
    if (browser && completedJobs > 0 && completedJobs % MAX_JOBS === 0) {
      await browser.close().catch(() => {});
      browser = undefined;
    }
  }
}

type CrawlCandidate = {
  url: string;
  depth: number;
  score: number;
  discovered_from: string;
  anchor_text?: string;
};

function normalizedHost(hostname: string): string {
  return hostname.toLowerCase().replace(/^www\./, "");
}

function normalizeCrawlUrl(raw: string): string | null {
  try {
    const u = new URL(raw);
    if (u.protocol !== "http:" && u.protocol !== "https:") return null;
    u.hash = "";
    // Query strings frequently create endless filter/tracking variants. Crawl the canonical path.
    u.search = "";
    if (u.pathname.length > 1) u.pathname = u.pathname.replace(/\/+$/, "");
    return u.toString();
  } catch {
    return null;
  }
}

function isSameSite(raw: string, root: URL): boolean {
  try {
    const u = new URL(raw);
    return normalizedHost(u.hostname) === normalizedHost(root.hostname) &&
      (u.protocol === "http:" || u.protocol === "https:");
  } catch { return false; }
}

function classifyPage(rawUrl: string, title = "", description = "", headings: Array<{text: string}> = []): string {
  const u = new URL(rawUrl);
  const path = u.pathname.replace(/%20/gi, " ").toLowerCase().replace(/[-_]+/g, " ");
  const meta = `${title} ${description} ${headings.slice(0, 12).map(h => h.text).join(" ")}`.toLowerCase();

  // Prefer URL structure over broad body/heading keywords; this avoids classifying
  // API reference pages as contact pages just because they mention "contact".
  if (path === "/" || path === "") return "home";
  if (/^\/(docs?|documentation|reference|api|learn)(\/|$)/.test(path) || /\b(api reference|documentation|developer guide|getting started)\b/.test(`${path} ${title.toLowerCase()}`)) return "docs";
  if (/^\/(blog|news|articles?|resources)(\/|$)/.test(path)) return "blog_resources";
  if (/^\/(pricing|plans|billing)(\/|$)/.test(path)) return "pricing";
  if (/^\/(features?|capabilities)(\/|$)/.test(path)) return "features";
  if (/^\/(integrations?|connectors?|marketplace)(\/|$)/.test(path)) return "integrations";
  if (/^\/(solutions?|use cases?|industries)(\/|$)/.test(path)) return "use_cases";
  if (/^\/(customers?|case studies?|testimonials?)(\/|$)/.test(path)) return "customers";
  if (/^\/(security|privacy|trust|compliance)(\/|$)/.test(path)) return "security";
  if (/^\/(about|company|our story|team)(\/|$)/.test(path)) return "about";
  if (/^\/(contact|support|sales|demo)(\/|$)/.test(path)) return "contact";
  if (/^\/(terms|legal|cookies|acceptable use)(\/|$)/.test(path)) return "legal";

  if (/\b(pricing|plans|billing|cost|price|packages)\b/.test(meta)) return "pricing";
  if (/\b(use cases?|industries|solutions|for teams|for developers|for agencies)\b/.test(meta)) return "use_cases";
  if (/\b(integrations?|connectors?|marketplace|apps directory)\b/.test(meta)) return "integrations";
  if (/\b(faq|frequently asked questions|questions and answers)\b/.test(meta)) return "faq";
  if (/\b(security|privacy|compliance|trust center|gdpr|soc 2)\b/.test(meta)) return "security";
  if (/\b(customers?|case studies|customer stories|testimonials|success stories)\b/.test(meta)) return "customers";
  if (/\b(blog|articles?|news|resources|guides?)\b/.test(`${path} ${title.toLowerCase()}`)) return "blog_resources";
  if (/\b(features?|capabilities|what you can do)\b/.test(meta)) return "features";
  if (/\b(product|platform|how it works|workflow)\b/.test(meta)) return "product";
  if (/\b(about|our story|company|team|mission)\b/.test(meta)) return "about";
  if (/\b(contact|support|sales|book a demo|request a demo)\b/.test(meta)) return "contact";
  if (/\b(terms|legal|cookies|acceptable use)\b/.test(meta)) return "legal";
  return "other";
}

function scoreCandidate(rawUrl: string, anchor = "", source = "internal_link"): number {
  let u: URL;
  try { u = new URL(rawUrl); } catch { return -1000; }
  const path = `${u.pathname} ${anchor}`.toLowerCase();
  if (/\.(?:png|jpe?g|gif|webp|svg|ico|pdf|zip|gz|css|js|map|woff2?|ttf|mp4|mp3|xml)$/i.test(u.pathname)) return -1000;
  if (/\/(?:logout|log-out|signout|sign-out|unsubscribe|cart|checkout|account|login|signin|sign-in|register|search)(?:\/|$)/i.test(u.pathname)) return -500;
  if (/(?:^|[?&])(utm_|gclid|fbclid|sort=|filter=|page=)/i.test(u.search)) return -200;
  if (source === "start_url") return 1000;
  let score = source === "sitemap" ? 25 : 0;
  const rules: Array<[RegExp, number]> = [
    [/pricing|plans|billing|price|packages/, 100],
    [/features?|capabilities/, 85],
    [/product|platform|how-it-works/, 75],
    [/solutions?|use-cases?|industries/, 72],
    [/integrations?|connectors?|marketplace/, 65],
    [/customers?|case-stud|testimonials?|success-stor/, 62],
    [/faq|frequently-asked/, 55],
    [/security|trust|compliance|privacy/, 48],
    [/about|our-story|company|team/, 42],
    [/contact|demo|sales/, 40],
    [/docs|documentation|api|guides?/, 32],
    [/blog|resources|articles?|news/, 20],
    [/terms|legal|cookies/, 4]
  ];
  for (const [re, weight] of rules) if (re.test(path)) score += weight;
  if (u.pathname.split("/").filter(Boolean).length <= 1) score += 8;
  if (anchor && /pricing|features|product|integrations|customers|solutions|use cases|about|contact|demo/i.test(anchor)) score += 15;
  if (source === "sitemap") score += 5;
  return score;
}

async function fetchTextLimited(rawUrl: string, maxBytes = 1_000_000): Promise<{url: string; status: number; text: string} | null> {
  let current = await assertPublicHttpUrl(rawUrl);
  for (let redirects = 0; redirects <= 4; redirects++) {
    const response = await fetch(current, {
      redirect: "manual",
      signal: AbortSignal.timeout(5_000),
      headers: { "user-agent": "LightweightSiteCrawler/1.0 (+public-page-indexing)" }
    }).catch(() => null);
    if (!response) return null;
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      const location = response.headers.get("location");
      if (!location || redirects === 4) return null;
      current = await assertPublicHttpUrl(new URL(location, current).toString());
      continue;
    }
    if (!response.ok) return null;
    const declared = Number(response.headers.get("content-length") || 0);
    if (declared > maxBytes) return null;
    if (!response.body) return { url: current.toString(), status: response.status, text: "" };
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) { await reader.cancel().catch(() => {}); return null; }
      chunks.push(value);
    }
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    return { url: current.toString(), status: response.status, text: new TextDecoder().decode(bytes) };
  }
  return null;
}

function parseXmlLocs(xml: string): string[] {
  return [...xml.matchAll(/<loc\b[^>]*>([\s\S]*?)<\/loc>/gi)]
    .map(m => m[1].trim().replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">"))
    .filter(Boolean);
}

async function discoverSitemapPages(root: URL, maxUrls = 500): Promise<{urls: string[]; sitemap_count: number; robots_found: boolean}> {
  const sitemapQueue: string[] = [];
  let robotsFound = false;
  const robots = await fetchTextLimited(new URL("/robots.txt", root).toString(), 256_000);
  if (robots) {
    robotsFound = true;
    for (const line of robots.text.split(/\r?\n/)) {
      const match = line.match(/^\s*sitemap\s*:\s*(\S+)/i);
      if (match) sitemapQueue.push(match[1]);
    }
  }
  for (const path of ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml", "/wp-sitemap.xml"]) {
    sitemapQueue.push(new URL(path, root).toString());
  }
  const seenSitemaps = new Set<string>();
  const pages = new Set<string>();
  while (sitemapQueue.length && seenSitemaps.size < 10 && pages.size < maxUrls) {
    const candidate = sitemapQueue.shift()!;
    const normalized = normalizeCrawlUrl(candidate);
    if (!normalized || seenSitemaps.has(normalized) || !isSameSite(normalized, root)) continue;
    seenSitemaps.add(normalized);
    const fetched = await fetchTextLimited(normalized, 1_000_000);
    if (!fetched || !/<(?:[\w.-]+:)?(?:urlset|sitemapindex)\b/i.test(fetched.text.slice(0, 4000))) continue;
    const locs = parseXmlLocs(fetched.text);
    if (/<(?:[\w.-]+:)?sitemapindex\b/i.test(fetched.text.slice(0, 4000))) {
      for (const loc of locs) if (sitemapQueue.length < 20) sitemapQueue.push(loc);
    } else {
      for (const loc of locs) {
        const pageUrl = normalizeCrawlUrl(loc);
        if (pageUrl && isSameSite(pageUrl, root)) pages.add(pageUrl);
        if (pages.size >= maxUrls) break;
      }
    }
  }
  return { urls: [...pages], sitemap_count: seenSitemaps.size, robots_found: robotsFound };
}

function flattenJsonLd(value: unknown, output: Record<string, any>[], depth = 0): void {
  if (depth > 6 || output.length > 500 || !value) return;
  if (Array.isArray(value)) { for (const item of value) flattenJsonLd(item, output, depth + 1); return; }
  if (typeof value !== "object") return;
  const obj = value as Record<string, any>;
  output.push(obj);
  if (obj["@graph"]) flattenJsonLd(obj["@graph"], output, depth + 1);
  if (obj["mainEntity"]) flattenJsonLd(obj["mainEntity"], output, depth + 1);
  if (obj["@type"] === "FAQPage" && obj["mainEntity"]) flattenJsonLd(obj["mainEntity"], output, depth + 1);
}

function textValue(value: unknown): string | null {
  if (typeof value === "string") return value.slice(0, 1000);
  if (typeof value === "number") return String(value);
  return null;
}

function buildWebsiteFacts(pages: Array<Record<string, any>>) {
  const categories: Record<string, RegExp> = {
    value_proposition: /\b(helps?|enables?|allows?|designed to|built to|makes it easier|all-in-one|platform for|solution for)\b/i,
    features: /\b(features?|automate|analytics|dashboard|workflow|templates?|generate|schedule|integrat|manage|track|monitor|publish|collaborat)\b/i,
    pricing: /\b(pricing|free plan|free trial|per month|per year|\/month|\/year|\$\s?\d|₹\s?\d|pricing plans?|enterprise plan)\b/i,
    use_cases: /\b(use case|for teams|for agencies|for developers|for creators|for businesses|designed for|ideal for|work with)\b/i,
    integrations: /\b(integrat|connects? with|works with|sync with|api access|webhooks?)\b/i,
    target_audience: /\b(marketers?|developers?|founders?|creators?|agencies|small businesses|enterprise teams|startups?|e-commerce|ecommerce)\b/i,
    faq: /\b(how do i|how does|what is|can i |do you |is there|frequently asked|common questions)\b/i,
    trust_and_security: /\b(security|privacy|compliance|encryption|gdpr|soc 2|data protection|permissions|secure)\b/i,
    social_proof: /\b(customers?|trusted by|testimonials?|case stud|reviews?|ratings?|teams use|companies use|success stories)\b/i,
    calls_to_action: /\b(get started|start free|try for free|book a demo|request a demo|sign up|create account|join now|contact sales)\b/i,
    company_about: /\b(our mission|our story|about us|founded|our team|who we are|company)\b/i,
    content_topics: /\b(blog|guide|tutorial|learn|best practices|resources|latest|announcements|case study)\b/i
  };
  const facts: Record<string, Array<{text: string; source_url: string; source_title: string; page_type: string}>> = {};
  for (const key of Object.keys(categories)) facts[key] = [];
  const seen = new Set<string>();
  const boilerplate = /^(home|menu|close|open menu|skip to content|search|sign in|log in|sign up|next|previous|copy|edit this page|on this page|table of contents|github|twitter|linkedin|discord|youtube|copyright|all rights reserved)$/i;
  const priority: Record<string, number> = { home: 8, product: 7, features: 7, pricing: 7, use_cases: 6, integrations: 6, customers: 5, security: 4, faq: 4, about: 3, contact: 2, docs: 1, blog_resources: 1, other: 0 };

  for (const page of pages) {
    const pageType = String(page.page_type || "other");
    const title = String(page.title || "");
    const rawChunks = String(page.body_text || "").split(/\n+|(?<=[.!?])\s+/);
    const chunks = rawChunks.map((x: string) => x.replace(/\s+/g, " ").trim());
    const headingChunks = (page.headings || []).map((h: any) => String(h.text || "").replace(/\s+/g, " ").trim());
    const candidates = [...headingChunks, ...chunks];
    for (const raw of candidates) {
      const snippet = String(raw || "").replace(/\s+/g, " ").trim();
      if (snippet.length < 28 || snippet.length > 280 || boilerplate.test(snippet)) continue;
      // Discard code-like fragments and text with too little natural-language signal.
      const letters = (snippet.match(/[a-zA-Z]/g) || []).length;
      const symbols = (snippet.match(/[{}<>;=]/g) || []).length;
      if (letters < 18 || symbols > Math.max(5, letters * 0.12) || /^(const|function|import|export|return|class|interface|type)\b/i.test(snippet)) continue;
      for (const [category, pattern] of Object.entries(categories)) {
        if (facts[category].length >= 12 || !pattern.test(snippet)) continue;
        // Avoid using generic docs/blog pages as first-choice marketing evidence when
        // the same category can be sourced from a more relevant product page.
        const normalized = snippet.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
        const key = `${category}:${normalized}`;
        const existingIndex = facts[category].findIndex(item => item.text.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim() === normalized);
        if (existingIndex >= 0) {
          const existing = facts[category][existingIndex];
          const existingPriority = priority[existing.page_type] ?? 0;
          if ((priority[pageType] ?? 0) <= existingPriority) continue;
          facts[category].splice(existingIndex, 1);
        }
        if (seen.has(key)) seen.delete(key);
        seen.add(key);
        facts[category].push({ text: snippet, source_url: page.url, source_title: title, page_type: pageType });
      }
    }
  }
  return facts;
}

function summarizeStructuredData(pages: Array<Record<string, any>>) {
  const typeCounts: Record<string, number> = {};
  const entities: Array<Record<string, unknown>> = [];
  const questions: Array<Record<string, unknown>> = [];
  const seen = new Set<string>();
  for (const page of pages) {
    const flattened: Record<string, any>[] = [];
    flattenJsonLd(page.structured_data, flattened);
    for (const item of flattened) {
      const rawTypes = Array.isArray(item["@type"]) ? item["@type"] : [item["@type"]];
      const types = rawTypes.filter((x: unknown) => typeof x === "string") as string[];
      for (const type of types) typeCounts[type] = (typeCounts[type] || 0) + 1;
      const name = textValue(item.name || item.headline || item.legalName);
      const description = textValue(item.description);
      const url = textValue(item.url || item["@id"]);
      const offers = item.offers && typeof item.offers === "object" ? item.offers : null;
      if (types.some(t => /Organization|LocalBusiness|Product|Service|SoftwareApplication|WebSite|WebPage|Offer|Event|Article|Course|Person|Brand|AggregateRating|BreadcrumbList/i.test(t))) {
        const entity = {
          types, name, description, url,
          price: offers ? textValue(offers.price ?? offers.lowPrice) : textValue(item.price),
          price_currency: offers ? textValue(offers.priceCurrency) : textValue(item.priceCurrency),
          rating: item.aggregateRating ? { value: textValue(item.aggregateRating.ratingValue), count: textValue(item.aggregateRating.reviewCount ?? item.aggregateRating.ratingCount) } : null,
          source_url: page.url
        };
        const key = JSON.stringify([types, name, url, page.url]);
        if (!seen.has(key) && entities.length < 150) { seen.add(key); entities.push(entity); }
      }
      if (types.includes("Question") || item.acceptedAnswer) {
        const q = { question: name || textValue(item.text), answer: textValue(item.acceptedAnswer?.text || item.acceptedAnswer?.name), source_url: page.url };
        if ((q.question || q.answer) && questions.length < 100) questions.push(q);
      }
    }
  }
  return { type_counts: typeCounts, entities, faq_questions: questions };
}

async function crawlWebsite(rawUrl: string, options: {maxPages: number; maxDepth: number; maxDurationMs: number; maxTotalChars: number}) {
  const root = await assertPublicHttpUrl(rawUrl);
  const started = Date.now();
  const limits = { max_pages: options.maxPages, max_depth: options.maxDepth, max_duration_ms: options.maxDurationMs, max_total_chars: options.maxTotalChars };
  const sitemap = await discoverSitemapPages(root).catch(() => ({ urls: [] as string[], sitemap_count: 0, robots_found: false }));
  const candidates = new Map<string, CrawlCandidate>();
  const visited = new Set<string>();
  const pages: Array<Record<string, any>> = [];
  const failures: Array<{url: string; error: string}> = [];
  let totalChars = 0;
  let truncated = false;

  const enqueue = (raw: string, depth: number, source: string, anchor = "") => {
    const normalized = normalizeCrawlUrl(raw);
    if (!normalized || !isSameSite(normalized, root) || visited.has(normalized)) return;
    if (depth > options.maxDepth) return;
    const score = scoreCandidate(normalized, anchor, source);
    if (score < -100) return;
    const old = candidates.get(normalized);
    if (!old || score > old.score || depth < old.depth) {
      candidates.set(normalized, { url: normalized, depth, score, discovered_from: source, anchor_text: anchor.slice(0, 160) });
    }
  };

  enqueue(root.toString(), 0, "start_url");
  for (const url of sitemap.urls) enqueue(url, 1, "sitemap");
  const rootNormalized = normalizeCrawlUrl(root.toString())!;

  while (pages.length + failures.length < options.maxPages && candidates.size > 0) {
    if (Date.now() - started > options.maxDurationMs || totalChars >= options.maxTotalChars) {
      truncated = true;
      break;
    }
    const next = [...candidates.values()].sort((a, b) => b.score - a.score || a.depth - b.depth)[0];
    candidates.delete(next.url);
    if (visited.has(next.url)) continue;
    visited.add(next.url);
    try {
      const result = await scrapeOne(next.url, Math.min(CRAWL_PAGE_TEXT_CHARS, options.maxTotalChars - totalChars), 150);
      const finalUrl = normalizeCrawlUrl(String(result.url || next.url));
      if (!finalUrl || !isSameSite(finalUrl, root)) {
        failures.push({ url: next.url, error: "Redirected outside the allowed site; page was excluded." });
        continue;
      }
      const pageText = String(result.body_text || "").slice(0, Math.max(0, options.maxTotalChars - totalChars));
      totalChars += pageText.length;
      const pageType = classifyPage(finalUrl, String(result.title || ""), String(result.description || ""), (result.headings as Array<{text: string}>) || []);
      const page: Record<string, any> = {
        url: finalUrl,
        requested_url: next.url,
        status: result.status,
        title: result.title,
        description: result.description,
        canonical_url: result.canonical_url,
        language: result.language,
        page_type: pageType,
        priority_score: next.score,
        depth: next.depth,
        discovered_from: next.discovered_from,
        headings: (result.headings as unknown[] || []).slice(0, 100),
        body_text: pageText,
        links: (result.links as unknown[] || []).slice(0, 100),
        buttons: (result.buttons as unknown[] || []).slice(0, 30),
        images: (result.images as unknown[] || []).slice(0, 30),
        structured_data: result.structured_data || [],
        social_metadata: result.social_metadata || {},
        extraction: result.extraction || {}
      };
      page.content_markdown = `# ${page.title || finalUrl}\n\n${page.description ? `> ${page.description}\n\n` : ""}## Content\n${pageText}`;
      pages.push(page);
      for (const link of (result.links as Array<{url?: string; text?: string}> || [])) {
        if (link.url) enqueue(link.url, next.depth + 1, "internal_link", link.text || "");
      }
      // Don't let a redirect/canonical duplicate consume additional page slots.
      visited.add(finalUrl);
      if (totalChars >= options.maxTotalChars) { truncated = candidates.size > 0; break; }
    } catch (err) {
      failures.push({ url: next.url, error: err instanceof Error ? err.message.slice(0, 500) : String(err).slice(0, 500) });
    }
  }
  if (candidates.size > 0 && pages.length + failures.length >= options.maxPages) truncated = true;

  const homepage = pages.find(p => normalizeCrawlUrl(p.url) === rootNormalized) || pages[0];
  const inventory = pages.map(p => ({ url: p.url, title: p.title, description: p.description, page_type: p.page_type, priority_score: p.priority_score, depth: p.depth, headings: (p.headings || []).slice(0, 25) }));
  const byType: Record<string, Array<{url: string; title: string}>> = {};
  for (const page of pages) {
    byType[page.page_type] ||= [];
    byType[page.page_type].push({ url: page.url, title: page.title || page.url });
  }
  const structured = summarizeStructuredData(pages);
  const facts = buildWebsiteFacts(pages);
  const keyTypes = ["pricing", "features", "product", "use_cases", "integrations", "customers", "about", "security", "faq", "contact", "docs", "blog_resources"];
  const keyPages = keyTypes.flatMap(type => (byType[type] || []).slice(0, 8).map(p => ({ ...p, page_type: type })));
  const siteNameEntity = structured.entities.find(e => (e.types as string[]).some(t => /Organization|LocalBusiness|Brand/i.test(t)) && e.name);
  return {
    schema_version: "1.0",
    website: {
      requested_url: root.toString(),
      origin: root.origin,
      name: siteNameEntity?.name || homepage?.title || root.hostname,
      title: homepage?.title || null,
      description: homepage?.description || null,
      canonical_url: homepage?.canonical_url || null,
      language: homepage?.language || null,
      social_metadata: homepage?.social_metadata || {}
    },
    crawl: {
      status: (truncated || failures.length > 0) ? "partial" : "completed",
      started_at: new Date(started).toISOString(),
      duration_ms: Date.now() - started,
      limits,
      pages_attempted: pages.length + failures.length,
      pages_succeeded: pages.length,
      pages_failed: failures.length,
      total_text_characters: totalChars,
      remaining_candidates: candidates.size,
      truncated,
      discovery: { robots_txt_found: sitemap.robots_found, sitemap_files_checked: sitemap.sitemap_count, sitemap_urls_found: sitemap.urls.length },
      failures
    },
    site_profile: {
      overview: homepage?.description || (homepage?.title ? `${homepage.title}${homepage.description ? ` — ${homepage.description}` : ""}` : null),
      page_counts_by_type: Object.fromEntries(Object.entries(byType).map(([k, v]) => [k, v.length])),
      key_pages: keyPages,
      evidence: facts,
      structured_data_summary: structured,
      content_generation_notes: [
        "Use evidence items with source_url as factual grounding; do not invent features, prices, integrations, customer claims, or guarantees.",
        "Use pricing pages and structured Offer/Product data as the source of truth for pricing claims.",
        "Use feature, use-case, integration and customer pages to vary post angles; avoid repeating the same page evidence across posts.",
        "Page classification and evidence categories are heuristic labels, not verified business facts."
      ]
    },
    page_inventory: inventory,
    pages
  };
}

function pruneCrawlJobs() {
  const finished = [...crawlJobs.values()].filter(j => j.status === "completed" || j.status === "failed")
    .sort((a, b) => b.updated_at.localeCompare(a.updated_at));
  for (const job of finished.slice(MAX_CRAWL_JOB_HISTORY)) crawlJobs.delete(job.id);
}

app.get("/health", async () => ({
  ok: true,
  queue_size: queue.size,
  queue_pending: queue.pending,
  browser_connected: Boolean(browser?.isConnected()),
  browser_started_at: browserStartedAt,
  completed_jobs: completedJobs
}));

app.post<{ Body: { url?: string; maxPages?: number; maxDepth?: number; maxDurationMs?: number; maxTotalChars?: number } }>("/crawl", async (request, reply) => {
  const body = request.body || {};
  const url = body.url;
  if (typeof url !== "string" || url.length > 2048) {
    return reply.code(400).send({ error: "Provide a URL string up to 2048 characters." });
  }
  try { await assertPublicHttpUrl(url); }
  catch (err) { return reply.code(400).send({ error: err instanceof Error ? err.message : "Invalid URL" }); }

  const maxPages = Math.max(1, Math.min(MAX_CRAWL_PAGES, Number.isFinite(body.maxPages) ? Math.floor(body.maxPages!) : 20));
  const maxDepth = Math.max(0, Math.min(5, Number.isFinite(body.maxDepth) ? Math.floor(body.maxDepth!) : 3));
  const maxDurationMs = Math.max(10_000, Math.min(MAX_CRAWL_DURATION_MS, Number.isFinite(body.maxDurationMs) ? Math.floor(body.maxDurationMs!) : MAX_CRAWL_DURATION_MS));
  const maxTotalChars = Math.max(10_000, Math.min(MAX_CRAWL_TOTAL_CHARS, Number.isFinite(body.maxTotalChars) ? Math.floor(body.maxTotalChars!) : MAX_CRAWL_TOTAL_CHARS));
  const activeCrawls = [...crawlJobs.values()].filter(j => j.status === "queued" || j.status === "running").length;
  if (activeCrawls >= MAX_CRAWL_JOBS || queue.size + queue.pending >= 10) {
    return reply.code(429).send({ error: "Crawl capacity is full; retry after an existing crawl finishes." });
  }

  const id = randomUUID();
  const now = new Date().toISOString();
  const job: CrawlJob = {
    id, status: "queued", created_at: now, updated_at: now,
    request: { url, max_pages: maxPages, max_depth: maxDepth, max_duration_ms: maxDurationMs, max_total_chars: maxTotalChars }
  };
  crawlJobs.set(id, job);
  pruneCrawlJobs();
  void queue.add(async () => {
    job.status = "running";
    job.updated_at = new Date().toISOString();
    try {
      job.result = await crawlWebsite(url, { maxPages, maxDepth, maxDurationMs, maxTotalChars });
      job.status = "completed";
    } catch (err) {
      job.error = err instanceof Error ? err.message : String(err);
      job.status = "failed";
    } finally {
      job.updated_at = new Date().toISOString();
      pruneCrawlJobs();
    }
  }).catch(err => {
    job.status = "failed";
    job.error = err instanceof Error ? err.message : String(err);
    job.updated_at = new Date().toISOString();
    pruneCrawlJobs();
  });

  return reply.code(202).send({
    job_id: id,
    status: job.status,
    status_url: `/crawl/${id}`,
    poll_interval_ms: 3000,
    limits: job.request,
    note: "Crawling runs asynchronously with bounded page, text, duration, and per-page request limits. Poll status_url until status is completed or failed. Results are held in memory; download/save the completed JSON promptly."
  });
});

app.get<{ Params: { id: string } }>("/crawl/:id", async (request, reply) => {
  const job = crawlJobs.get(request.params.id);
  if (!job) return reply.code(404).send({ error: "Crawl job not found or expired. Jobs are stored in memory and may be lost after a service restart." });
  return reply.send({
    job_id: job.id,
    status: job.status,
    created_at: job.created_at,
    updated_at: job.updated_at,
    request: job.request,
    ...(job.error ? { error: job.error } : {}),
    ...(job.result ? { result: job.result } : {})
  });
});

app.post<{ Body: { url?: string } }>("/scrape", async (request, reply) => {
  const url = request.body?.url;
  if (typeof url !== "string" || url.length > 2048) {
    return reply.code(400).send({ error: "Provide a URL string up to 2048 characters." });
  }
  try {
    await assertPublicHttpUrl(url);
  } catch (err) {
    return reply.code(400).send({ error: err instanceof Error ? err.message : "Invalid URL" });
  }

  // Prevent unbounded queue growth on a small VPS.
  if (queue.size + queue.pending >= 10) {
    return reply.code(429).send({ error: "Scrape queue is full; retry later." });
  }

  try {
    const result = await queue.add(() => scrapeOne(url));
    return reply.send(result);
  } catch (err) {
    request.log.error({ err }, "Scrape failed");
    return reply.code(502).send({
      error: "Scrape failed",
      details: err instanceof Error ? err.message : String(err)
    });
  }
});

app.addHook("onClose", async () => {
  await browser?.close().catch(() => {});
});

async function main() {
  browser = await launchBrowser();
  await app.listen({ port: PORT, host: HOST });
  app.log.info(`Scraper listening on http://${HOST}:${PORT}`);
}

process.on("SIGINT", async () => {
  await app.close();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await app.close();
  process.exit(0);
});

main().catch(err => {
  app.log.error(err);
  process.exit(1);
});
