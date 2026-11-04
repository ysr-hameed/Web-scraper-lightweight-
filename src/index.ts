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
// Read several pages in parallel; the browser is shared but contexts are isolated.
const CRAWL_CONCURRENCY = Math.max(1, Math.min(4, Number(process.env.CRAWL_CONCURRENCY ?? 3)));
const CRAWL_SETTLE_MS = Math.max(50, Math.min(1_000, Number(process.env.CRAWL_SETTLE_MS ?? 120)));
const MAX_REQUESTS_PER_PAGE = Math.max(30, Math.min(250, Number(process.env.MAX_REQUESTS_PER_PAGE ?? 120)));
const MAX_BRAND_PAGES = Math.max(1, Math.min(5, Number(process.env.MAX_BRAND_PAGES ?? 3)));
const MAX_BRAND_DURATION_MS = Math.max(8_000, Math.min(60_000, Number(process.env.MAX_BRAND_DURATION_MS ?? 25_000)));
const MAX_BRAND_JOBS = Math.max(1, Math.min(5, Number(process.env.MAX_BRAND_JOBS ?? 2)));
const MAX_BRAND_JOB_HISTORY = Math.max(1, Math.min(25, Number(process.env.MAX_BRAND_JOB_HISTORY ?? 6)));

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
let activePages = 0;

type CrawlJob = {
  id: string;
  status: "queued" | "running" | "completed" | "failed";
  created_at: string;
  updated_at: string;
  request: { url: string; max_pages: number; max_depth: number; max_duration_ms: number; max_total_chars: number };
  result?: Record<string, unknown>;
  error?: string;
  progress?: CrawlProgress;
};

export interface CrawlProgress {
  /** Pages discovered so far (may grow as crawling continues). */
  pages_found: number;
  /** Pages fully read and extracted. */
  pages_processed: number;
  /** Page currently being read, if any. */
  current_page: string | null;
  /** Overall percent (0-100). */
  percent: number;
  /** Human-readable status message from the scraper. */
  message: string | null;
}
const crawlJobs = new Map<string, CrawlJob>();

type BrandJob = {
  id: string;
  status: "queued" | "running" | "completed" | "failed";
  created_at: string;
  updated_at: string;
  request: { url: string; max_pages: number; max_duration_ms: number };
  result?: Record<string, unknown>;
  error?: string;
  progress?: CrawlProgress;
};
const brandJobs = new Map<string, BrandJob>();

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

async function scrapeOne(
  rawUrl: string,
  maxTextChars = MAX_TEXT_CHARS,
  maxLinks = MAX_LINKS,
  brandMode = false,
  sharedBrowser?: Browser
) {
  const initialUrl = await assertPublicHttpUrl(rawUrl);
  const b = sharedBrowser ?? (await getBrowser());
  activePages += 1;
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
    await page.waitForTimeout(CRAWL_SETTLE_MS);

    const extracted = await page.evaluate(({ maxText, maxLinks, brandMode }) => {
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

      let brandInfo: Record<string, any> | null = null;
      if (brandMode) {
        const attr = (selector: string, name: string) =>
          (document.querySelector(selector) as Element | null)?.getAttribute(name) ?? null;
        const metaAny = (key: string) =>
          clean(document.querySelector<HTMLMetaElement>(`meta[name="${CSS.escape(key)}"]`)?.content ??
            document.querySelector<HTMLMetaElement>(`meta[property="${CSS.escape(key)}"]`)?.content);
        const toHex = (input: string | null | undefined): string | null => {
          if (!input) return null;
          const value = input.trim().toLowerCase();
          const rgb = value.match(/rgba?\(([^)]+)\)/i);
          if (rgb) {
            const parts = rgb[1].split(",").map(part => part.trim());
            const r = Number(parts[0]);
            const g = Number(parts[1]);
            const b = Number(parts[2]);
            const a = parts[3] === undefined ? 1 : Number(parts[3]);
            if (![r, g, b].every(Number.isFinite) || !Number.isFinite(a) || a === 0) return null;
            if (r > 245 && g > 245 && b > 245) return null;
            if (r < 8 && g < 8 && b < 8) return null;
            return "#" + [r, g, b]
              .map(v => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0"))
              .join("");
          }
          if (/^#[0-9a-f]{3,8}$/i.test(value)) return value.slice(0, 7);
          return null;
        };
        const colorCounts = new Map<string, number>();
        const addColor = (value: string | null | undefined) => {
          const hex = toHex(value);
          if (!hex) return;
          colorCounts.set(hex, (colorCounts.get(hex) ?? 0) + 1);
        };
        addColor(metaAny("theme-color"));
        const colorSources = [
          document.body,
          document.querySelector("header"),
          document.querySelector("main"),
          document.querySelector("nav"),
          ...Array.from(document.querySelectorAll("button,a,[class*=btn],[class*=button],[class*=primary],[class*=accent]")).slice(0, 30)
        ];
        for (const el of colorSources) {
          if (!el) continue;
          const style = getComputedStyle(el as Element);
          addColor(style.backgroundColor);
          addColor(style.color);
          addColor(style.borderTopColor);
        }
        const colors = [...colorCounts.entries()].sort((a, b) => b[1] - a[1]).map(([hex]) => hex).slice(0, 6);

        const fontSet = new Set<string>();
        const fontSources = [document.body, document.querySelector("h1"), document.querySelector("h2"), document.querySelector("button"), document.querySelector("nav a")];
        for (const el of fontSources) {
          if (!el) continue;
          const family = getComputedStyle(el as Element).fontFamily.split(",")[0]?.replace(/["']/g, "").trim();
          if (family && family.length < 60 && !/^(ui-|system-ui|sans-serif|serif|monospace|inherit|initial|unset)/i.test(family)) {
            fontSet.add(family);
          }
        }
        const fonts = [...fontSet].slice(0, 4);

        const imageCandidates = Array.from(document.querySelectorAll<HTMLImageElement>("img"))
          .map(img => ({
            src: img.currentSrc || img.src,
            alt: clean(img.alt),
            inHeader: Boolean(img.closest("header,nav")),
            className: typeof img.className === "string" ? img.className : ""
          }))
          .filter(img => img.src && /^https?:\/\//i.test(img.src));
        const logo =
          imageCandidates.find(img => /logo|brand/i.test(`${img.alt} ${img.src} ${img.className}`))?.src ??
          imageCandidates.find(img => img.inHeader && !/avatar|icon|sprite/i.test(`${img.alt} ${img.src}`))?.src ??
          attr('link[rel="apple-touch-icon"]', "href") ??
          attr('link[rel="icon"]', "href") ??
          null;

        const socialDomains: Array<[string, RegExp]> = [
          ["instagram", /instagram\.com/i],
          ["linkedin", /linkedin\.com/i],
          ["x", /(?:twitter|x)\.com/i],
          ["facebook", /facebook\.com/i],
          ["youtube", /youtube\.com|youtu\.be/i],
          ["tiktok", /tiktok\.com/i],
          ["github", /github\.com/i],
          ["pinterest", /pinterest\.com/i]
        ];
        const allAnchors = Array.from(document.querySelectorAll<HTMLAnchorElement>("a[href]"))
          .map(a => ({ url: a.href, text: clean(a.innerText || a.getAttribute("aria-label") || a.title) }))
          .filter(a => a.url.startsWith("http") || a.url.startsWith("mailto:") || a.url.startsWith("tel:"));
        const social: Record<string, string> = {};
        for (const anchor of allAnchors) {
          for (const [key, pattern] of socialDomains) {
            if (!social[key] && pattern.test(anchor.url)) social[key] = anchor.url;
          }
        }
        const emails = Array.from(new Set(allAnchors
          .filter(a => a.url.startsWith("mailto:"))
          .map(a => decodeURIComponent(a.url.slice(7).split("?")[0]))
          .filter(Boolean))).slice(0, 5);
        const phones = Array.from(new Set(allAnchors
          .filter(a => a.url.startsWith("tel:"))
          .map(a => decodeURIComponent(a.url.slice(4)).trim())
          .filter(Boolean))).slice(0, 5);
        const nav = Array.from(new Map(
          Array.from(document.querySelectorAll<HTMLAnchorElement>("header a[href], nav a[href]"))
            .map(a => ({ text: clean(a.innerText || a.getAttribute("aria-label") || ""), url: a.href }))
            .filter(a => a.text && a.text.length <= 70 && /^https?:\/\//i.test(a.url))
            .map(a => [a.text.toLowerCase(), a] as const)
        ).values()).slice(0, 20).map(a => a.text);

        const headingList = headings.slice(0, 40);
        const siteName = metaAny("og:site_name") || metaAny("application-name");
        const titleParts = title.split(/\s*[|–—•·]\s*|\s+-\s+/).map(part => part.trim()).filter(Boolean);
        const name = clean(siteName) || titleParts[0] || "";
        const tagline = metaAny("og:description") || clean(description) || headingList.find(h => h.level === 1)?.text || "";

        const jsonLdEntities: Array<Record<string, any>> = [];
        const walk = (value: unknown, depth = 0) => {
          if (depth > 5 || !value || jsonLdEntities.length > 100) return;
          if (Array.isArray(value)) { for (const item of value) walk(item, depth + 1); return; }
          if (typeof value === "object") {
            const obj = value as Record<string, any>;
            jsonLdEntities.push(obj);
            if (obj["@graph"]) walk(obj["@graph"], depth + 1);
          }
        };
        walk(jsonLd);
        const organization = jsonLdEntities.find(entity => {
          const types = Array.isArray(entity["@type"]) ? entity["@type"] : [entity["@type"]];
          return types.some((type: unknown) => typeof type === "string" && /Organization|LocalBusiness|Brand/i.test(type)) && entity.name;
        });
        const offers = jsonLdEntities
          .filter(entity => entity.offers || /Product|Service|Offer/i.test(String(entity["@type"])))
          .slice(0, 10)
          .map(entity => ({
            name: clean(entity.name || entity.headline || ""),
            description: clean(entity.description || ""),
            price: entity.offers ? clean(String(entity.offers.price ?? entity.offers.lowPrice ?? "")) : clean(String(entity.price ?? "")),
            url: clean(entity.url || entity["@id"] || "")
          }))
          .filter(offer => offer.name);

        brandInfo = {
          name: clean(organization?.name) || name,
          title,
          description: clean(description),
          tagline,
          language: document.documentElement.lang || null,
          logo_url: logo,
          og_image: property("og:image"),
          theme_color: metaAny("theme-color"),
          colors,
          fonts,
          social_links: social,
          emails,
          phones,
          nav,
          headings: headingList,
          offers,
          image_count: imageCandidates.length
        };
      }

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
        robots: meta("robots"),
        brand: brandInfo
      };
    }, { maxText: maxTextChars, maxLinks, brandMode });

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
      brand: extracted.brand ?? null,
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
    activePages = Math.max(0, activePages - 1);
    // Recycle the browser only when nothing else is reading, to reduce long-running memory growth.
    if (browser && activePages === 0 && completedJobs > 0 && completedJobs % MAX_JOBS === 0) {
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
  if (/\/(?:logout|log-out|signout|sign-out|unsubscribe|cart|checkout|account|my-account|login|signin|sign-in|signup|sign-up|register|forgot-password|reset-password|change-password|verify-email|confirm-email|magic-link|sso|oauth|callback|search)(?:\/|$)/i.test(u.pathname)) return -1000;
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

async function crawlWebsite(
  rawUrl: string,
  options: {maxPages: number; maxDepth: number; maxDurationMs: number; maxTotalChars: number},
  onProgress?: (progress: CrawlProgress) => void
) {
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

  const emitProgress = (currentPage: string | null, message: string | null) => {
    const pagesFound = candidates.size + visited.size + failures.length;
    const pagesProcessed = pages.length;
    const percent = options.maxPages > 0 ? Math.min(100, Math.round((pagesProcessed / options.maxPages) * 100)) : 0;
    const progress: CrawlProgress = {
      pages_found: pagesFound,
      pages_processed: pagesProcessed,
      current_page: currentPage,
      percent,
      message: message ?? `Read ${pagesProcessed} of ${pagesFound} pages${currentPage ? ` (${currentPage})` : ""}`
    };
    if (onProgress) onProgress(progress);
  };

  enqueue(root.toString(), 0, "start_url");
  for (const url of sitemap.urls) enqueue(url, 1, "sitemap");
  const rootNormalized = normalizeCrawlUrl(root.toString())!;

  emitProgress(null, "Starting site read…");

  const pickBatch = (limit: number): CrawlCandidate[] => {
    const batch: CrawlCandidate[] = [];
    while (batch.length < limit && candidates.size > 0) {
      let bestKey: string | null = null;
      let bestEntry: CrawlCandidate | null = null;
      for (const [key, entry] of candidates) {
        if (visited.has(entry.url)) continue;
        if (
          !bestEntry ||
          entry.score > bestEntry.score ||
          (entry.score === bestEntry.score && entry.depth < bestEntry.depth)
        ) {
          bestKey = key;
          bestEntry = entry;
        }
      }
      if (!bestKey || !bestEntry) break;
      candidates.delete(bestKey);
      if (visited.has(bestEntry.url)) continue;
      visited.add(bestEntry.url);
      batch.push(bestEntry);
    }
    return batch;
  };

  while (pages.length + failures.length < options.maxPages && candidates.size > 0) {
    if (Date.now() - started > options.maxDurationMs || totalChars >= options.maxTotalChars) {
      truncated = true;
      break;
    }
    const remaining = options.maxPages - (pages.length + failures.length);
    const batch = pickBatch(Math.min(CRAWL_CONCURRENCY, remaining));
    if (!batch.length) break;
    emitProgress(batch[0].url, batch.length === 1 ? `Reading ${batch[0].url}` : `Reading ${batch.length} pages in parallel`);

    const settled = await Promise.all(
      batch.map((candidate) =>
        scrapeOne(candidate.url, Math.min(CRAWL_PAGE_TEXT_CHARS, Math.max(1_000, options.maxTotalChars - totalChars)), 150)
          .then((result) => ({ candidate, result, error: null as string | null }))
          .catch((error: unknown) => ({
            candidate,
            result: null,
            error: (error instanceof Error ? error.message : String(error)).slice(0, 500),
          })),
      ),
    );

    const scrapedUrls = new Set(pages.map((page) => normalizeCrawlUrl(String(page.url))));
    for (const item of settled) {
      const { candidate, result, error } = item;
      if (error || !result) {
        failures.push({ url: candidate.url, error: error ?? "Unknown scraper error." });
        continue;
      }
      try {
        const finalUrl = normalizeCrawlUrl(String(result.url || candidate.url));
        if (!finalUrl || !isSameSite(finalUrl, root)) {
          failures.push({ url: candidate.url, error: "Redirected outside the allowed site; page was excluded." });
          continue;
        }
        if (scrapedUrls.has(finalUrl)) continue; // canonical/redirect duplicate already captured
        scrapedUrls.add(finalUrl);
        const pageText = String(result.body_text || "").slice(0, Math.max(0, options.maxTotalChars - totalChars));
        totalChars += pageText.length;
        const pageType = classifyPage(finalUrl, String(result.title || ""), String(result.description || ""), (result.headings as Array<{text: string}>) || []);
        const page: Record<string, any> = {
          url: finalUrl,
          requested_url: candidate.url,
          status: result.status,
          title: result.title,
          description: result.description,
          canonical_url: result.canonical_url,
          language: result.language,
          page_type: pageType,
          priority_score: candidate.score,
          depth: candidate.depth,
          discovered_from: candidate.discovered_from,
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
        visited.add(finalUrl);
        for (const link of (result.links as Array<{url?: string; text?: string}> || [])) {
          if (link.url) enqueue(link.url, candidate.depth + 1, "internal_link", link.text || "");
        }
        if (totalChars >= options.maxTotalChars) truncated = candidates.size > 0;
      } catch (err) {
        failures.push({ url: candidate.url, error: err instanceof Error ? err.message.slice(0, 500) : String(err).slice(0, 500) });
      }
    }

    pages.sort((a, b) => (Number(b.priority_score) || 0) - (Number(a.priority_score) || 0));
    emitProgress(null, `Read ${pages.length} page${pages.length !== 1 ? "s" : ""}${failures.length ? ` (${failures.length} skipped)` : ""}`);
    if (totalChars >= options.maxTotalChars) break;
  }
  if (candidates.size > 0 && pages.length + failures.length >= options.maxPages) truncated = true;

  emitProgress(null, truncated ? `Stopped at page limit (${pages.length} read)` : `Finished reading ${pages.length} page${pages.length !== 1 ? 's' : ''}`);

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

function brandLinkScore(anchor: string, rawUrl: string): number {
  let url: URL;
  try { url = new URL(rawUrl); } catch { return -1000; }
  const path = url.pathname.toLowerCase();
  const text = anchor.toLowerCase();
  const joined = `${path} ${text}`;
  if (/\.(?:png|jpe?g|gif|webp|svg|ico|pdf|zip|gz|css|js|map|woff2?|ttf|mp4|mp3|xml)$/i.test(path)) return -1000;
  if (/\/(?:logout|log-out|signout|sign-out|unsubscribe|cart|checkout|account|my-account|login|signin|sign-in|signup|sign-up|register|forgot-password|reset-password|change-password|verify-email|confirm-email|magic-link|sso|oauth|callback|search)(?:\/|$)/i.test(path)) return -1000;
  if (/\/(?:blog|news|articles?|resources|docs?|documentation|api|careers?|jobs?|legal|terms|privacy|cookies)(?:\/|$)/i.test(path)) return -500;
  if (path === "/" || path === "") return -1000;
  let score = 0;
  if (/about|company|story|team|mission|who-we-are/.test(joined)) score += 100;
  if (/product|service|feature|solution|platform|what-we-do|offer/.test(joined)) score += 80;
  if (/pricing|plans|packages/.test(joined)) score += 60;
  if (/contact|demo/.test(joined)) score += 20;
  return score;
}

const GENERIC_BRAND_HEADING = /^(home|menu|close|search|sign in|log in|sign up|get started|learn more|contact|about|pricing|features|product|products|services|solutions|resources|blog|docs|support|faq|cookie|privacy|terms|accept|subscribe|newsletter)$/i;

function brandOfferingsFromPages(pages: Array<Record<string, any>>): Array<{name: string; description: string; url: string | null}> {
  const out: Array<{name: string; description: string; url: string | null}> = [];
  const seen = new Set<string>();
  for (const page of pages) {
    const type = classifyPage(String(page.url || ""), String(page.title || ""), String(page.description || ""), (page.headings || []) as Array<{text: string}>);
    if (!["product", "features", "pricing", "use_cases", "home"].includes(type)) continue;
    for (const heading of (page.headings || []) as Array<{level: number; text: string}>) {
      const text = String(heading.text || "").trim();
      if (heading.level < 2 || heading.level > 3) continue;
      if (text.length < 4 || text.length > 80 || GENERIC_BRAND_HEADING.test(text)) continue;
      if (text.split(/\s+/).length > 10) continue;
      const key = text.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ name: text, description: String(page.description || "").slice(0, 220), url: String(page.url || "") || null });
      if (out.length >= 8) return out;
    }
  }
  // Fall back to product/service/pricing links in the navigation.
  for (const page of pages) {
    const nav = Array.isArray(page.brand?.nav) ? page.brand.nav as string[] : [];
    for (const label of nav) {
      const text = String(label || "").trim();
      if (!/product|service|feature|solution|plan|pricing|packages|shop|menu|course|program/i.test(text)) continue;
      if (text.length < 3 || text.length > 60 || GENERIC_BRAND_HEADING.test(text)) continue;
      const key = text.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ name: text, description: "", url: String(page.url || "") || null });
      if (out.length >= 8) return out;
    }
  }
  return out;
}

async function extractBrandDetails(
  rawUrl: string,
  options: { maxPages: number; maxDurationMs: number },
  onProgress?: (progress: CrawlProgress) => void
) {
  const root = await assertPublicHttpUrl(rawUrl);
  const started = Date.now();
  const maxPages = Math.max(1, Math.min(MAX_BRAND_PAGES, options.maxPages));

  onProgress?.({ pages_found: 1, pages_processed: 0, current_page: root.toString(), percent: 5, message: `Reading ${root.hostname}` });
  const home = await scrapeOne(root.toString(), 20_000, 200, true);
  const pages: Array<Record<string, any>> = [home];

  const candidates = new Map<string, { url: string; score: number }>();
  for (const link of ((home.links as Array<{url?: string; text?: string}> | undefined) ?? [])) {
    if (!link.url || !isSameSite(link.url, root)) continue;
    const normalized = normalizeCrawlUrl(link.url);
    if (!normalized || normalized === normalizeCrawlUrl(root.toString())) continue;
    const score = brandLinkScore(link.text ?? "", normalized);
    if (score < 1) continue;
    const existing = candidates.get(normalized);
    if (!existing || score > existing.score) candidates.set(normalized, { url: normalized, score });
  }

  const secondaries = [...candidates.values()].sort((a, b) => b.score - a.score).slice(0, Math.max(0, maxPages - 1));
  if (secondaries.length && Date.now() - started < options.maxDurationMs) {
    onProgress?.({ pages_found: pages.length + secondaries.length, pages_processed: pages.length, current_page: secondaries[0].url, percent: 20, message: `Reading ${secondaries.length} more page${secondaries.length === 1 ? "" : "s"} in parallel` });
    const settled = await Promise.allSettled(secondaries.map((candidate) => scrapeOne(candidate.url, 8_000, 80, true)));
    for (const item of settled) {
      if (item.status === "fulfilled") pages.push(item.value);
      else if (item.reason) { /* one weak secondary page must not fail the brand read */ }
    }
  }
  onProgress?.({ pages_found: pages.length, pages_processed: pages.length, current_page: null, percent: 95, message: "Brand details ready" });

  const brandOf = (page: Record<string, any>) => (page.brand ?? {}) as Record<string, any>;
  const pick = (getter: (page: Record<string, any>) => unknown): string => {
    for (const page of pages) {
      const value = getter(page);
      if (typeof value === "string" && value.trim()) return value.trim();
    }
    return "";
  };
  const stringList = (values: unknown[]): string[] =>
    Array.from(new Set(values.filter((value): value is string => typeof value === "string" && value.trim().length > 0).map(value => value.trim())));

  const name = pick(page => brandOf(page).name) || root.hostname.replace(/^www\./, "");
  const description = pick(page => brandOf(page).description);
  const tagline = pick(page => brandOf(page).tagline) || description;
  const aboutPage = pages.find(page => classifyPage(String(page.url || ""), String(page.title || ""), String(page.description || ""), (page.headings || []) as Array<{text: string}>) === "about");
  const aboutText = String(aboutPage?.body_text || home.body_text || "").slice(0, 6_000);
  const colors = stringList(pages.flatMap(page => Array.isArray(brandOf(page).colors) ? brandOf(page).colors : [])).slice(0, 6);
  const fonts = stringList(pages.flatMap(page => Array.isArray(brandOf(page).fonts) ? brandOf(page).fonts : [])).slice(0, 4);
  const nav = stringList(pages.flatMap(page => Array.isArray(brandOf(page).nav) ? brandOf(page).nav : [])).slice(0, 20);
  const emails = stringList(pages.flatMap(page => Array.isArray(brandOf(page).emails) ? brandOf(page).emails : [])).slice(0, 5);
  const phones = stringList(pages.flatMap(page => Array.isArray(brandOf(page).phones) ? brandOf(page).phones : [])).slice(0, 5);
  const social: Record<string, string> = {};
  for (const page of pages) {
    const links = brandOf(page).social_links;
    if (!links || typeof links !== "object") continue;
    for (const [key, value] of Object.entries(links as Record<string, unknown>)) {
      if (!social[key] && typeof value === "string" && value.startsWith("http")) social[key] = value;
    }
  }
  const images = pages.flatMap(page => Array.isArray(page.images) ? page.images : [])
    .filter((image): image is {src: string; alt?: string} => Boolean(image && typeof image === "object" && typeof (image as any).src === "string"))
    .slice(0, 30)
    .map(image => ({ src: image.src, alt: image.alt ?? "" }));

  const structured = summarizeStructuredData(pages as Array<Record<string, any>>);
  const offerings = brandOfferingsFromPages(pages);
  for (const entity of structured.entities) {
    const types = Array.isArray(entity.types) ? entity.types as string[] : [];
    const entityName = typeof entity.name === "string" ? entity.name.trim() : "";
    if (!entityName || !types.some(type => /Product|Service|Offer/i.test(type))) continue;
    if (offerings.some(offer => offer.name.toLowerCase() === entityName.toLowerCase())) continue;
    offerings.push({
      name: entityName.slice(0, 80),
      description: typeof entity.description === "string" ? entity.description.slice(0, 220) : "",
      url: typeof entity.url === "string" ? entity.url : null
    });
    if (offerings.length >= 8) break;
  }

  const headings = (home.headings as Array<{level: number; text: string}> | undefined ?? []).slice(0, 40);
  const absUrl = (value: unknown, base: string): string | null => {
    if (typeof value !== "string" || !value.trim()) return null;
    try {
      const resolved = new URL(value, base);
      return resolved.protocol === "http:" || resolved.protocol === "https:" ? resolved.toString() : null;
    } catch {
      return null;
    }
  };
  const absFromPages = (getter: (page: Record<string, any>) => unknown): string | null => {
    for (const page of pages) {
      const value = getter(page);
      const resolved = absUrl(value, String(page.url || home.url));
      if (resolved) return resolved;
    }
    return null;
  };
  const absoluteImages = images
    .map((image) => ({ ...image, src: absUrl(image.src, root.toString()) ?? "" }))
    .filter((image) => image.src);
  let confidence = 0.35;
  if (name && name.toLowerCase() !== root.hostname.replace(/^www\./, "").toLowerCase()) confidence += 0.15;
  if (description) confidence += 0.15;
  if (colors.length) confidence += 0.1;
  if (fonts.length) confidence += 0.05;
  if (offerings.length) confidence += 0.1;
  if (pages.length > 1) confidence += 0.1;
  confidence = Math.min(0.98, Number(confidence.toFixed(2)));

  onProgress?.({ pages_found: pages.length, pages_processed: pages.length, current_page: null, percent: 100, message: "Brand details ready" });

  return {
    schema_version: "1.0",
    url: String(home.url || root.toString()),
    requested_url: root.toString(),
    fetched_at: new Date().toISOString(),
    brand: {
      name,
      title: String(home.title || ""),
      description,
      tagline,
      one_liner: tagline || description,
      language: pick(page => brandOf(page).language),
      logo_url: absFromPages(page => brandOf(page).logo_url),
      og_image: absFromPages(page => brandOf(page).og_image),
      theme_color: pick(page => brandOf(page).theme_color) || null,
      colors,
      fonts,
      social_links: social,
      emails,
      phones,
      nav,
      headings,
      offerings: offerings.map(offer => ({ ...offer, url: offer.url ? absUrl(offer.url, root.toString()) : null })),
      about_text: aboutText,
      images: absoluteImages,
      structured_data: structured,
      confidence,
      pages_read: pages.length
    },
    pages: pages.map(page => ({
      url: String(page.url || ""),
      title: String(page.title || ""),
      description: String(page.description || ""),
      page_type: classifyPage(String(page.url || ""), String(page.title || ""), String(page.description || ""), (page.headings || []) as Array<{text: string}>),
      headings: (Array.isArray(page.headings) ? page.headings : []).slice(0, 30),
      body_text: String(page.body_text || "").slice(0, 12_000),
      images: (Array.isArray(page.images) ? page.images : []).slice(0, 30)
    }))
  };
}

function pruneBrandJobs() {
  const finished = [...brandJobs.values()]
    .filter(job => job.status === "completed" || job.status === "failed")
    .sort((a, b) => b.updated_at.localeCompare(a.updated_at));
  for (const job of finished.slice(MAX_BRAND_JOB_HISTORY)) brandJobs.delete(job.id);
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

app.post<{ Body: { url?: string; maxPages?: number; maxDurationMs?: number } }>("/brand", async (request, reply) => {
  const body = request.body || {};
  const url = body.url;
  if (typeof url !== "string" || url.length > 2048) {
    return reply.code(400).send({ error: "Provide a URL string up to 2048 characters." });
  }
  try { await assertPublicHttpUrl(url); }
  catch (err) { return reply.code(400).send({ error: err instanceof Error ? err.message : "Invalid URL" }); }

  const maxPages = Math.max(1, Math.min(MAX_BRAND_PAGES, Number.isFinite(body.maxPages) ? Math.floor(body.maxPages!) : MAX_BRAND_PAGES));
  const maxDurationMs = Math.max(8_000, Math.min(MAX_BRAND_DURATION_MS, Number.isFinite(body.maxDurationMs) ? Math.floor(body.maxDurationMs!) : MAX_BRAND_DURATION_MS));
  const activeBrands = [...brandJobs.values()].filter(job => job.status === "queued" || job.status === "running").length;
  if (activeBrands >= MAX_BRAND_JOBS || queue.size + queue.pending >= 10) {
    return reply.code(429).send({ error: "Brand read capacity is full; retry after a current job finishes." });
  }

  const id = randomUUID();
  const now = new Date().toISOString();
  const job: BrandJob = {
    id, status: "queued", created_at: now, updated_at: now,
    request: { url, max_pages: maxPages, max_duration_ms: maxDurationMs }
  };
  brandJobs.set(id, job);
  pruneBrandJobs();
  void queue.add(async () => {
    job.status = "running";
    job.updated_at = new Date().toISOString();
    try {
      job.result = await extractBrandDetails(url, { maxPages, maxDurationMs }, (progress) => {
        job.progress = progress;
        job.updated_at = new Date().toISOString();
      });
      job.status = "completed";
    } catch (err) {
      job.error = err instanceof Error ? err.message : String(err);
      job.status = "failed";
    } finally {
      job.updated_at = new Date().toISOString();
      pruneBrandJobs();
    }
  }).catch(err => {
    job.status = "failed";
    job.error = err instanceof Error ? err.message : String(err);
    job.updated_at = new Date().toISOString();
    pruneBrandJobs();
  });

  return reply.code(202).send({
    job_id: id,
    status: job.status,
    status_url: `/brand/${id}`,
    poll_interval_ms: 1000,
    limits: job.request,
    note: "Brand details are read from the homepage plus a few key pages, with bounded page and duration limits. Poll status_url until status is completed or failed."
  });
});

app.get<{ Params: { id: string } }>("/brand/:id", async (request, reply) => {
  const job = brandJobs.get(request.params.id);
  if (!job) return reply.code(404).send({ error: "Brand job not found or expired. Jobs are stored in memory and may be lost after a service restart." });
  return reply.send({
    job_id: job.id,
    status: job.status,
    created_at: job.created_at,
    updated_at: job.updated_at,
    request: job.request,
    ...(job.progress ? { progress: job.progress } : {}),
    ...(job.error ? { error: job.error } : {}),
    ...(job.result ? { result: job.result } : {})
  });
});

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
      job.result = await crawlWebsite(url, { maxPages, maxDepth, maxDurationMs, maxTotalChars }, (progress) => {
        job.progress = progress;
        job.updated_at = new Date().toISOString();
      });
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
    ...(job.progress ? { progress: job.progress } : {}),
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
