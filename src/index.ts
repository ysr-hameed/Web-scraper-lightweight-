import Fastify from "fastify";
import { chromium, type Browser, type BrowserContext, type Page, type Route } from "playwright";
import PQueue from "p-queue";
import dns from "node:dns/promises";
import net from "node:net";

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? "127.0.0.1";
const MAX_JOBS = Number(process.env.MAX_JOBS ?? 50);
const NAV_TIMEOUT_MS = Number(process.env.NAV_TIMEOUT_MS ?? 20_000);
const READY_TIMEOUT_MS = Number(process.env.READY_TIMEOUT_MS ?? 8_000);
const MAX_TEXT_CHARS = Number(process.env.MAX_TEXT_CHARS ?? 80_000);
const MAX_LINKS = Number(process.env.MAX_LINKS ?? 500);
const MAX_RESPONSE_BYTES = Number(process.env.MAX_RESPONSE_BYTES ?? 8 * 1024 * 1024);

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

async function scrapeOne(rawUrl: string) {
  const initialUrl = await assertPublicHttpUrl(rawUrl);
  const b = await getBrowser();
  let context: BrowserContext | undefined;
  let page: Page | undefined;
  const started = Date.now();
  let navigationStatus: number | null = null;
  let navigationError: string | null = null;
  const blocked: Record<string, number> = {};

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
      clone?.querySelectorAll("script,style,noscript,svg,canvas,iframe,nav,footer,header,[role=navigation],[aria-hidden=true]")
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
    }, { maxText: MAX_TEXT_CHARS, maxLinks: MAX_LINKS });

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

app.get("/health", async () => ({
  ok: true,
  queue_size: queue.size,
  queue_pending: queue.pending,
  browser_connected: Boolean(browser?.isConnected()),
  browser_started_at: browserStartedAt,
  completed_jobs: completedJobs
}));

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
