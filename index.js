import express from "express";
import cors from "cors";
import { chromium } from "playwright";
import { execSync } from "child_process";
import pg from "pg";

const { Pool } = pg;
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── Database ────────────────────────────────────────────────────────────────

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL environment variable is required.");
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// O Neon (serverless) derruba conexões ociosas de tempos em tempos (autosuspend/reciclagem
// do pooler). Sem este handler, um erro num cliente idle do pool vira uma exceção não
// tratada que derruba o processo inteiro. Aqui só logamos e seguimos — o pg cria uma
// conexão nova automaticamente na próxima query.
pool.on("error", (err) => {
  console.error("[PG POOL] erro em conexão ociosa (ignorado, processo continua):", err.message);
});

// Rede de segurança geral: qualquer erro assíncrono que escape dos try/catch normais
// (ex: uma Promise rejeitada sem .catch) também não deve derrubar o serviço inteiro.
process.on("unhandledRejection", (err) => {
  console.error("[UNHANDLED REJECTION] (ignorado, processo continua):", err);
});
process.on("uncaughtException", (err) => {
  console.error("[UNCAUGHT EXCEPTION] (ignorado, processo continua):", err);
});

async function query(sql, params = []) {
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

// Garante que toda URL salva tenha esquema (http/https). Sem isso, o Playwright falha
// ao tentar navegar ("Cannot navigate to invalid URL") e, em links renderizados no
// front-end (<a href="...">), o navegador interpreta como caminho relativo e abre
// dentro do próprio domínio do app (ex: "Cannot GET /dominio.com").
function normalizeUrl(url) {
  if (!url) return url;
  const trimmed = url.trim();
  if (!trimmed) return trimmed;
  return /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
}

async function initDb() {
  await query(`
    CREATE TABLE IF NOT EXISTS pages (
      slug          TEXT PRIMARY KEY,
      nome          TEXT NOT NULL,
      url           TEXT NOT NULL,
      tipo          TEXT NOT NULL DEFAULT 'pagina',
      instagram_url TEXT,
      geo           TEXT,
      nicho         TEXT,
      created_at    TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS scrape_history (
      id           SERIAL PRIMARY KEY,
      slug         TEXT NOT NULL,
      ads_count    INTEGER NOT NULL,
      slot         SMALLINT,
      collected_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_scrape_history_slug ON scrape_history(slug)
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS scrape_latest (
      slug         TEXT PRIMARY KEY,
      ads_count    INTEGER NOT NULL,
      collected_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  // Migrações: garante colunas novas em banco antigo (seguro rodar sempre)
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS tipo TEXT NOT NULL DEFAULT 'pagina'`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS inicial_count INTEGER`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS instagram_url TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS geo TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS nicho TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS funil TEXT`);

  // FIX (fila travada / starvation do cron): coluna que registra a última tentativa
  // de coleta de cada página, sucesso OU falha. Sem isso, uma página com falha
  // persistente nunca sai da lista de "pendentes" do slot (porque falha não grava em
  // scrape_history) e, sem ORDER BY, a query do /api/cron/tick devolvia sempre as
  // mesmas 5 páginas quebradas em todo tick — monopolizando o LIMIT 5 e impedindo
  // qualquer outra página do slot de ser tentada. Ver uso em processBatch() e na
  // query de /api/cron/tick.
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_status TEXT`);

  await query(`
    CREATE TABLE IF NOT EXISTS funnel_nodes (
      id         SERIAL PRIMARY KEY,
      slug       TEXT NOT NULL REFERENCES pages(slug) ON DELETE CASCADE,
      tipo       TEXT NOT NULL CHECK (tipo IN ('advertorial','tsl','vsl','quiz','whatsapp','checkout')),
      rotulo     TEXT NOT NULL,
      url        TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS funnel_edges (
      id           SERIAL PRIMARY KEY,
      from_node_id INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
      to_node_id   INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
      created_at   TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`CREATE INDEX IF NOT EXISTS idx_funnel_nodes_slug ON funnel_nodes(slug)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_funnel_edges_from ON funnel_edges(from_node_id)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_funnel_edges_to ON funnel_edges(to_node_id)`);

  // Migração: amplia o CHECK de funnel_nodes.tipo para incluir 'ads' e 'presell'.
  // Em vez de depender de adivinhar o nome do constraint (que o Postgres gera
  // automaticamente), o bloco DO $$ abaixo PROCURA dinamicamente qual é o CHECK
  // constraint da coluna `tipo` e o remove, seja qual for o nome — depois recria
  // com um nome fixo e conhecido (`funnel_nodes_tipo_check`). Isso é 100% seguro
  // de rodar em todo restart: se não encontrar nenhum constraint, não faz nada;
  // se encontrar, remove e recria com a lista ampliada.
  await query(`
    DO $$
    DECLARE
      c_name text;
    BEGIN
      SELECT conname INTO c_name
      FROM pg_constraint
      WHERE conrelid = 'funnel_nodes'::regclass
        AND contype = 'c'
        AND pg_get_constraintdef(oid) ILIKE '%tipo%';
      IF c_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE funnel_nodes DROP CONSTRAINT %I', c_name);
      END IF;
    END $$;
  `);
  await query(`
    ALTER TABLE funnel_nodes ADD CONSTRAINT funnel_nodes_tipo_check
    CHECK (tipo IN ('ads','advertorial','presell','tsl','vsl','quiz','whatsapp','checkout'))
  `);

  console.log("[DB] Tables ready.");
}


// ─── Helpers ─────────────────────────────────────────────────────────────────

function toSlug(nome) {
  return nome
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function getChromiumPath() {
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    return process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }
  try {
    return execSync("which chromium || which chromium-browser || which google-chrome", {
      encoding: "utf8",
    }).trim().split("\n")[0];
  } catch {
    return undefined;
  }
}

// ─── Scraper ─────────────────────────────────────────────────────────────────

function getBrowserLaunchArgs() {
  return [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process",
    "--window-size=1366,768",
  ];
}

async function createStealthContext(browser) {
  const context = await browser.newContext({
    locale: "pt-BR",
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    viewport: { width: 1366, height: 768 },
    extraHTTPHeaders: {
      "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
      "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
    },
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, "languages", { get: () => ["pt-BR", "pt", "en-US", "en"] });
    window.chrome = { runtime: {} };
  });

  // Injeta cookies de sessão do Facebook se configurados
  const fbCookiesRaw = process.env.FB_COOKIES;
  if (fbCookiesRaw) {
    try {
      const raw = JSON.parse(fbCookiesRaw);
      // Suporta formato Cookie-Editor (array de objetos) e formato Netscape simplificado
      const cookies = raw.map((c) => ({
        name: c.name,
        value: c.value,
        domain: c.domain || ".facebook.com",
        path: c.path || "/",
        httpOnly: c.httpOnly ?? false,
        secure: c.secure ?? true,
        sameSite: c.sameSite === "no_restriction" ? "None"
                : c.sameSite === "lax" ? "Lax"
                : c.sameSite === "strict" ? "Strict"
                : "None",
        ...(c.expirationDate ? { expires: Math.floor(c.expirationDate) } : {}),
      }));
      await context.addCookies(cookies);
      console.log(`[AUTH] ${cookies.length} cookies do Facebook injetados no contexto.`);
    } catch (err) {
      console.warn(`[AUTH] Falha ao parsear FB_COOKIES: ${err.message} — continuando sem autenticação.`);
    }
  } else {
    console.warn("[AUTH] FB_COOKIES não definido — Playwright rodará sem sessão (pode falhar na Meta).");
  }

  return context;
}

async function extractCount(page) {
  return await page.evaluate(() => {
    const bodyText = document.body ? document.body.innerText : "";
    let m = bodyText.match(/(?:~\s*)?([\d.,]+)\s*(?:resultados|results)/i);
    if (m) {
      const n = parseInt(m[1].replace(/[,.]/g, ""), 10);
      if (!Number.isNaN(n)) return n;
    }

    const elements = Array.from(document.querySelectorAll("div, span, h1, h2, h3, p, strong, b"));
    for (const el of elements) {
      const txt = el.innerText || "";
      if (txt.length < 60 && /(?:~\s*)?[\d.,]+\s*(?:resultados|results)/i.test(txt)) {
        const match = txt.match(/(?:~\s*)?([\d.,]+)\s*(?:resultados|results)/i);
        if (match) {
          const n = parseInt(match[1].replace(/[,.]/g, ""), 10);
          if (!Number.isNaN(n)) return n;
        }
      }
    }

    const docText = document.documentElement ? document.documentElement.innerText : "";
    m = docText.match(/(?:~\s*)?([\d.,]+)\s*(?:resultados|results)/i);
    if (m) {
      const n = parseInt(m[1].replace(/[,.]/g, ""), 10);
      if (!Number.isNaN(n)) return n;
    }

    return null;
  }).catch(() => null);
}

async function waitForCounter(page, maxWaitMs = 18000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const count = await extractCount(page);
    if (count !== null) return count;
    await page.evaluate(() => window.scrollBy(0, 100)).catch(() => {});
    await page.waitForTimeout(1000);
  }
  return null;
}

function cleanMetaUrl(rawUrl) {
  try {
    const u = new URL(rawUrl);
    if (u.hostname.includes("facebook.com") && u.searchParams.has("view_all_page_id") && u.searchParams.has("id")) {
      u.searchParams.delete("id");
      return u.toString();
    }
  } catch {}
  return rawUrl;
}

async function scrapeWithContext(context, url) {
  const page = await context.newPage();
  try {
    // Bloqueia APENAS imagens e mídias pesadas — NUNCA bloqueia CSS nem scripts
    await page.route("**/*", (route) => {
      const type = route.request().resourceType();
      if (["image", "media"].includes(type)) {
        route.abort();
      } else {
        route.continue();
      }
    });

    const targetUrl = cleanMetaUrl(url);
    await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: 35000 });

    let n = await waitForCounter(page, 18000);
    if (n !== null) return n;

    // Se o contador não apareceu e a URL foi limpa, tenta a original também
    if (targetUrl !== url) {
      console.log(`[SCRAPE] tentando URL alternativa: ${url}`);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 35000 });
      n = await waitForCounter(page, 12000);
      if (n !== null) return n;
    }

    // Se ainda não encontrou, checa se tem redirect para outra URL que NÃO SEJA _fb_noscript
    const html = await page.content();
    const m = html.match(/<meta[^>]+http-equiv=["']refresh["'][^>]*content=["'][^"']*url\s*=\s*([^"'>]+)["']/i);
    if (m && !m[1].includes("_fb_noscript")) {
      const target = m[1].trim().replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&#39;/g, "'");
      const nextUrl = new URL(target, page.url()).toString();
      console.log(`[SCRAPE] seguindo redirect válido: ${nextUrl}`);
      await page.goto(nextUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
      n = await waitForCounter(page, 12000);
      if (n !== null) return n;
    }

    // Log de diagnóstico
    const pageTitle = await page.title().catch(() => "");
    const bodySnippet = await page.evaluate(() => {
      return (document.body ? document.body.innerText.slice(0, 300) : "").replace(/\s+/g, " ").trim();
    }).catch(() => "");
    console.warn(`[SCRAPE-DIAG] Falha na extração. Title: "${pageTitle}" | Conteúdo visto: "${bodySnippet}"`);

    return null;
  } finally {
    await page.close();
  }
}

async function scrapeAdCount(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: getBrowserLaunchArgs(),
    });
    try {
      const context = await createStealthContext(browser);
      const count = await scrapeWithContext(context, url);
      if (count !== null) {
        console.log(`[SCRAPE] attempt=${attempt} count=${count}`);
        return count;
      }
      console.warn(`[SCRAPE] attempt=${attempt} — count not found, retrying...`);
    } catch (err) {
      console.error(`[SCRAPE] attempt=${attempt} error: ${err.message}`);
    } finally {
      await browser.close();
    }
    if (attempt < retries) await new Promise((r) => setTimeout(r, 4000));
  }
  console.error(`[SCRAPE] all ${retries} attempts failed, returning null`);
  return null;
}

// Dedupe considera slot — só bloqueia duplicata do MESMO slot
async function saveCount(slug, count, slot) {
  console.log(`[SAVECOUNT] slug=${slug} slot=${slot}`);
  const { rows: recent } = await query(
    `SELECT id FROM scrape_history
     WHERE slug = $1
       AND slot IS NOT DISTINCT FROM $2
       AND collected_at >= NOW() - INTERVAL '60 seconds'
     LIMIT 1`,
    [slug, slot]
  );

  await query(
    `INSERT INTO scrape_latest (slug, ads_count, collected_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (slug) DO UPDATE
       SET ads_count    = EXCLUDED.ads_count,
           collected_at = EXCLUDED.collected_at`,
    [slug, count]
  );

  if (recent.length === 0) {
    await query("INSERT INTO scrape_history (slug, ads_count, slot) VALUES ($1, $2, $3)", [slug, count, slot]);
    console.log(`[HISTORY] slug=${slug} slot=${slot} count=${count} saved`);
  } else {
    console.log(`[HISTORY] slug=${slug} slot=${slot} skipped duplicate`);
  }
}

// Captura inicial no momento do cadastro (individual)
async function captureInicial(slug, url) {
  try {
    const count = await scrapeAdCount(url, 2);
    if (count === null) {
      console.warn(`[DESCOBERTA] slug=${slug} falhou, nada gravado`);
      return null;
    }
    await query(
      `UPDATE pages SET inicial_count = COALESCE(inicial_count, $2) WHERE slug = $1`,
      [slug, count]
    );
    await query(
      `INSERT INTO scrape_latest (slug, ads_count, collected_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (slug) DO UPDATE
         SET ads_count    = EXCLUDED.ads_count,
             collected_at = EXCLUDED.collected_at`,
      [slug, count]
    );
    console.log(`[DESCOBERTA] slug=${slug} inicial=${count} capturado no cadastro`);
    return count;
  } catch (err) {
    console.error(`[DESCOBERTA] falha ao capturar inicial de slug=${slug}: ${err.message}`);
    return null;
  }
}

// Versão de captureInicial que reutiliza browser já aberto (para lotes)
async function captureInicialWithContext(context, slug, url) {
  let count = null;
  for (let attempt = 1; attempt <= 2 && count === null; attempt++) {
    try {
      count = await scrapeWithContext(context, url);
    } catch (err) {
      console.error(`[LOTE] slug=${slug} attempt=${attempt} error: ${err.message}`);
    }
  }
  if (count === null) return null;
  const final = count;
  await query(`UPDATE pages SET inicial_count = COALESCE(inicial_count, $2) WHERE slug = $1`, [slug, final]);
  await query(
    `INSERT INTO scrape_latest (slug, ads_count, collected_at) VALUES ($1, $2, NOW())
     ON CONFLICT (slug) DO UPDATE SET ads_count = EXCLUDED.ads_count, collected_at = EXCLUDED.collected_at`,
    [slug, final]
  );
  return final;
}

// Processa lote em background (fire-and-forget)
async function runLote(itens) {
  if (isRunning) {
    console.warn("[LOTE] abortado — já existe uma coleta em andamento (cron ou outro lote)");
    loteStatus.erros.push("Abortado: já havia uma coleta (cron ou outro lote) em andamento. Tente de novo em alguns minutos.");
    loteStatus.emAndamento = false;
    return;
  }
  isRunning = true;
  loteStatus = {
    emAndamento: true,
    total: itens.length,
    concluidos: 0,
    atual: null,
    erros: [],
    iniciadoEm: new Date().toISOString(),
    finalizadoEm: null,
  };
  console.log(`[LOTE] ===== iniciado — ${itens.length} itens =====`);

  let browser;
  try {
    browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: getBrowserLaunchArgs(),
    });
    const context = await createStealthContext(browser);

    for (const item of itens) {
      loteStatus.atual = item.nome;
      const slug = toSlug(item.nome);
      if (!slug) {
        loteStatus.erros.push(`"${item.nome}" — nome inválido, ignorado`);
        loteStatus.concluidos++;
        continue;
      }
      try {
        await query(
          `INSERT INTO pages (slug, nome, url, tipo, instagram_url)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (slug) DO UPDATE SET nome=$2, url=$3, tipo=$4,
             instagram_url = COALESCE(EXCLUDED.instagram_url, pages.instagram_url)`,
          [slug, item.nome, item.url, item.tipo, item.instagram_url || null]
        );
        await captureInicialWithContext(context, slug, item.url);
        console.log(`[LOTE] slug=${slug} cadastrado e capturado (${loteStatus.concluidos + 1}/${itens.length})`);
      } catch (err) {
        console.error(`[LOTE] erro no item slug=${slug}: ${err.message}`);
        loteStatus.erros.push(`"${item.nome}" — erro: ${err.message}`);
      }
      loteStatus.concluidos++;
    }
  } catch (err) {
    console.error(`[LOTE] erro fatal: ${err.message}`);
    loteStatus.erros.push(`Erro fatal: ${err.message}`);
  } finally {
    if (browser) await browser.close();
    isRunning = false;
    loteStatus.emAndamento = false;
    loteStatus.atual = null;
    loteStatus.finalizadoEm = new Date().toISOString();
    console.log(`[LOTE] ===== finalizado — ${loteStatus.concluidos}/${loteStatus.total} processados, ${loteStatus.erros.length} erros =====`);
  }
}

async function mirrorToSheet(rows) {
  const url = process.env.SHEET_WEBHOOK_URL;
  if (!url) return;
  try {
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ collected_at: new Date().toISOString(), rows }),
    });
    console.log("[SHEET] mirrored to backup webhook");
  } catch (err) {
    console.error(`[SHEET] mirror failed: ${err.message}`);
  }
}

let isRunning = false;
let blockedUntil = 0;

let loteStatus = {
  emAndamento: false,
  total: 0,
  concluidos: 0,
  atual: null,
  erros: [],
  iniciadoEm: null,
  finalizadoEm: null,
};

// Parser de lote — aceita 3 formatos:
//   Nome | URL
//   tipo | Nome | URL
//   Nome | URL | https://instagram.com/...   (Instagram no final — opcional)
//   tipo | Nome | URL | https://instagram.com/...
function parseLoteInput(texto) {
  const linhas = texto.split("\n").map((l) => l.trim()).filter(Boolean);
  const itens = [];
  for (const linha of linhas) {
    const partes = linha.split("|").map((s) => s.trim()).filter(Boolean);
    let nome, url, tipoForcado = null, instagram_url = null;

    if (partes.length >= 2) {
      const possivelTipo = partes[0].toLowerCase();
      if (possivelTipo === "dominio" || possivelTipo === "pagina") {
        // tipo | Nome | URL [| Instagram]
        tipoForcado = possivelTipo;
        nome = partes[1];
        // Verifica se o último campo é uma URL do Instagram
        const ultimo = partes[partes.length - 1];
        if (partes.length >= 4 && (ultimo.includes("instagram.com") || ultimo.startsWith("https://www.instagram"))) {
          instagram_url = ultimo;
          url = partes.slice(2, partes.length - 1).join("|");
        } else {
          url = partes.slice(2).join("|");
        }
      } else {
        // Nome | URL [| Instagram]
        nome = partes[0];
        const ultimo = partes[partes.length - 1];
        if (partes.length >= 3 && (ultimo.includes("instagram.com") || ultimo.startsWith("https://www.instagram"))) {
          instagram_url = ultimo;
          url = partes.slice(1, partes.length - 1).join("|");
        } else {
          url = partes.slice(1).join("|");
        }
      }
    } else {
      continue; // linha sem "|" ou vazia — ignora
    }

    if (!nome || !url) continue;
    const tipo = tipoForcado || (url.includes("view_all_page_id=") ? "pagina" : "dominio");
    itens.push({ nome, url, tipo, instagram_url });
  }
  return itens;
}

function getCurrentSlot() {
  const now = new Date();
  const hour = now.getUTCHours();
  if (hour >= 1 && hour < 6) return 22;
  if (hour >= 6 && hour < 15) return 3;
  return 12;
}

async function processBatch(pages, slot) {
  let browser;
  let context;
  const results = [];

  async function launchBrowser() {
    if (browser) await browser.close();
    browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: getBrowserLaunchArgs(),
    });
    context = await createStealthContext(browser);
  }

  try {
    await launchBrowser(); // Início limpo
    
    for (let i = 0; i < pages.length; i++) {
      const p = pages[i];

      // Sanity check: URL sem esquema (http/https) nunca vai carregar — nem tenta,
      // pra não desperdiçar as 2 tentativas nem confundir o motivo da falha no log.
      let count = null;
      let falhaMotivo = null;
      if (!/^https?:\/\//i.test(p.url || "")) {
        falhaMotivo = `URL inválida (falta http/https): "${p.url}"`;
        console.error(`[BATCH] slug=${p.slug} ${falhaMotivo}`);
      } else {
        for (let attempt = 1; attempt <= 2 && count === null; attempt++) {
          try {
            count = await scrapeWithContext(context, p.url);
          } catch (err) {
            falhaMotivo = err.message;
            console.error(`[BATCH] slug=${p.slug} attempt=${attempt} error: ${err.message}`);
          }
        }
      }

      // FIX (fila travada / starvation do cron): registra a tentativa (sucesso OU
      // falha) em pages.last_attempt_at. A query de /api/cron/tick agora ordena por
      // esse campo (ASC NULLS FIRST), então uma página que acabou de falhar vai para
      // o FIM da fila de pendentes do slot, dando vez às demais no próximo tick — em
      // vez de a mesma página quebrada monopolizar o LIMIT 5 em todo ciclo.
      await query(`UPDATE pages SET last_attempt_at = NOW(), last_status = $2 WHERE slug = $1`, [p.slug, count === null ? "falha_scraping" : "ok"]);

      // Coleta falhou de verdade — NÃO salva 0 (isso viraria um dado falso no histórico).
      // Só loga e pula o slug; será tentado de novo no próximo tick, mesmo slot, dentro
      // da janela de 8h.
      if (count === null) {
        console.warn(`[BATCH] slug=${p.slug} FALHA na coleta (${falhaMotivo || "motivo desconhecido"}) — pulado, histórico preservado (sem gravar 0 falso)`);
        results.push({ slug: p.slug, nome: p.nome, count: null, falha: falhaMotivo || "falha desconhecida" });
        await new Promise(r => setTimeout(r, 1500));
        continue;
      }

      const final = count;

      try {
        if (slot !== null && slot !== undefined) {
          await saveCount(p.slug, final, slot);
        } else {
          await query(
            `INSERT INTO scrape_latest (slug, ads_count, collected_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (slug) DO UPDATE
               SET ads_count = EXCLUDED.ads_count, collected_at = EXCLUDED.collected_at`,
            [p.slug, final]
          );
          console.log(`[LATEST] slug=${p.slug} count=${final} (manual)`);
        }
        results.push({ slug: p.slug, nome: p.nome, count: final });
      } catch (dbErr) {
        console.error(`[BATCH] slug=${p.slug} count=${final} COLETOU MAS FALHOU AO GRAVAR NO BANCO: ${dbErr.message}`);
        results.push({ slug: p.slug, nome: p.nome, count: null, dbError: true });
      }

      // Delay tático entre páginas
      await new Promise(r => setTimeout(r, 1500));
    }
  } catch (err) {
    console.error(`[BATCH] fatal error: ${err.message}`);
  } finally {
    if (browser) await browser.close();
  }

  const resultsOk = results.filter((r) => r.count !== null);
  if (resultsOk.length > 0) {
    await mirrorToSheet(resultsOk);
  }
  return results;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

app.get("/api/healthz", (_req, res) => res.json({ status: "ok", ts: new Date().toISOString() }));

app.get("/api/cron/tick", async (req, res) => {
  res.json({ status: "alive", message: "Tick received" });
  
  if (isRunning) return;
  if (Date.now() < blockedUntil) return;
  isRunning = true;
  
  try {
    const slot = getCurrentSlot();
    // FIX (fila travada / starvation): ORDER BY p.last_attempt_at ASC NULLS FIRST
    // garante rotação justa entre as páginas pendentes do slot. Sem isso, como a
    // query não tinha ordenação explícita, o Postgres devolvia consistentemente as
    // mesmas linhas (praticamente a ordem física da tabela) — então páginas que
    // falham sempre (ex: bloqueio da Meta numa URL específica) nunca saíam do topo
    // do resultado e ocupavam o LIMIT 5 inteiro em TODO tick, travando a coleta de
    // qualquer outra página pendente daquele slot. Agora, toda página tentada
    // (sucesso ou falha) vai pro fim da fila, e páginas nunca tentadas (NULL) vêm
    // primeiro.
    const { rows: pages } = await query(`
      SELECT p.slug, p.nome, p.url
      FROM pages p
      WHERE NOT EXISTS (
        SELECT 1 FROM scrape_history sh 
        WHERE sh.slug = p.slug 
          AND sh.slot = $1 
          AND sh.collected_at >= NOW() - INTERVAL '8 hours'
      )
      ORDER BY p.last_attempt_at ASC NULLS FIRST
      LIMIT 5;
    `, [slot]);

    if (pages.length === 0) {
      isRunning = false;
      return;
    }

    console.log(`[TICK] Iniciando lote de ${pages.length} paginas para o slot ${slot}...`);
    const results = await processBatch(pages, slot);
    const metaSlugs = new Set(pages.filter(p => /facebook\.com\/ads\/library/.test(p.url)).map(p => p.slug));
    const metaRes = results.filter(r => metaSlugs.has(r.slug));
    if (metaRes.length >= 3 && metaRes.every(r => r.count === null && !r.dbError)) {
      blockedUntil = Date.now() + 90 * 60 * 1000;
      console.warn(`[TICK] ${metaRes.length}/${metaRes.length} páginas da Meta falharam — cooldown de 90 min (até ${new Date(blockedUntil).toISOString()})`);
    }
    console.log(`[TICK] Lote do slot ${slot} finalizado.`);
  } catch (err) {
    console.error("[TICK] Erro geral:", err);
  } finally {
    isRunning = false;
  }
});

app.post("/api/salvar", async (req, res) => {
  const { nome, url: urlRaw, tipo, instagram_url, geo, nicho, funil, ads_count_inicial } = req.body;
  if (!nome || !urlRaw) return res.status(400).json({ error: "Fields 'nome' and 'url' are required." });
  const url = normalizeUrl(urlRaw);
  const slug = toSlug(nome);
  if (!slug) return res.status(400).json({ error: "Could not generate a valid slug." });
  const tipoFinal = tipo === "dominio" ? "dominio" : "pagina";
  await query(
    `INSERT INTO pages (slug, nome, url, tipo, instagram_url, geo, nicho, funil)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (slug) DO UPDATE
       SET nome=$2, url=$3, tipo=$4,
           instagram_url=COALESCE(EXCLUDED.instagram_url, pages.instagram_url),
           geo=COALESCE(EXCLUDED.geo, pages.geo),
           nicho=COALESCE(EXCLUDED.nicho, pages.nicho),
           funil=COALESCE(EXCLUDED.funil, pages.funil)`,
    [slug, nome, url, tipoFinal, instagram_url || null, geo || null, nicho || null, funil || null]
  );
  console.log(`[SALVAR] registered slug=${slug} tipo=${tipoFinal}`);

  let inicial = ads_count_inicial;
  if (inicial !== undefined && inicial !== null) {
    const countNum = parseInt(inicial, 10) || 0;
    await query(
      `UPDATE pages SET inicial_count = COALESCE(inicial_count, $2) WHERE slug = $1`,
      [slug, countNum]
    );
    await query(
      `INSERT INTO scrape_latest (slug, ads_count, collected_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (slug) DO UPDATE
         SET ads_count    = EXCLUDED.ads_count,
             collected_at = EXCLUDED.collected_at`,
      [slug, countNum]
    );
    await query(
      `INSERT INTO scrape_history (slug, ads_count, slot) VALUES ($1, $2, NULL)`,
      [slug, countNum]
    );
    inicial = countNum;
    console.log(`[DESCOBERTA] slug=${slug} inicial=${countNum} salvo via Extensão (Sem Playwright)`);
  } else {
    inicial = await captureInicial(slug, url);
  }

  res.json({ slug, tipo: tipoFinal, inicial, coletarPath: `/api/coletar/${slug}` });
});

app.get("/api/coletar/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query("SELECT * FROM pages WHERE slug = $1 LIMIT 1", [slug]);
  const row = rows[0];
  if (!row) return res.status(404).type("text/plain").send(`Page '${slug}' not registered.`);
  try {
    const count = await scrapeAdCount(row.url);
    if (count === null) return res.status(502).type("text/plain").send("FALHA");
    res.type("text/plain").send(String(count));
    await query(
      `INSERT INTO scrape_latest (slug, ads_count, collected_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (slug) DO UPDATE
         SET ads_count    = EXCLUDED.ads_count,
             collected_at = EXCLUDED.collected_at`,
      [slug, count]
    );
    console.log(`[LATEST] slug=${slug} count=${count} (manual via /api/coletar — histórico preservado)`);
  } catch (err) {
    console.error(`[COLETAR] error slug=${slug}: ${err.message}`);
    res.status(500).type("text/plain").send("FALHA");
  }
});

app.get("/api/coletar-tudo", async (_req, res) => {
  if (isRunning) {
    return res.status(409).json({ status: "busy", message: "Já existe uma coleta em andamento (cron ou lote). Tente em alguns minutos." });
  }
  isRunning = true;
  res.json({ status: "started" });

  (async () => {
    try {
      const { rows: pages } = await query(`SELECT slug, nome, url FROM pages`);
      if (pages.length === 0) {
        console.log("[RUN] coleta-tudo: nenhuma página cadastrada");
        return;
      }

      const CHUNK_SIZE = 5; // mesmo tamanho de lote usado pelo cron — navegador é reiniciado a cada lote
      console.log(`[RUN] coleta-tudo manual iniciada — ${pages.length} páginas, em lotes de ${CHUNK_SIZE}`);

      let processadas = 0;
      for (let i = 0; i < pages.length; i += CHUNK_SIZE) {
        const lote = pages.slice(i, i + CHUNK_SIZE);
        await processBatch(lote, null);
        processadas += lote.length;
        console.log(`[RUN] coleta-tudo progresso: ${processadas}/${pages.length}`);
      }

      console.log(`[RUN] coleta-tudo manual finalizada — ${pages.length} páginas`);
    } catch (e) {
      console.error("[RUN] manual error:", e.message);
    } finally {
      isRunning = false;
    }
  })();
});

app.get("/api/historico/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT id, slug, ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at DESC`,
    [slug]
  );
  res.json(rows);
});

app.get("/api/resumo/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at ASC`,
    [slug]
  );
  if (rows.length === 0) return res.json({ slug, message: "No data yet." });
  const counts = rows.map((r) => r.ads_count);
  const min = Math.min(...counts), max = Math.max(...counts);
  const avg = Math.round(counts.reduce((a, b) => a + b, 0) / counts.length);
  const first = counts[0], last = counts[counts.length - 1];
  const trend = last > first ? "crescendo" : last < first ? "caindo" : "estável";
  res.json({ slug, total_coletas: rows.length, min, max, avg, trend, first, last });
});

app.get("/api/status", async (_req, res) => {
  const { rows: pages } = await query("SELECT slug, nome, url FROM pages");
  const result = await Promise.all(pages.map(async (p) => {
    const { rows } = await query(
      `SELECT ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at DESC LIMIT 1`,
      [p.slug]
    );
    const latest = rows[0];
    return { slug: p.slug, nome: p.nome, url: p.url, ads_ativos: latest?.ads_count ?? null, ultima_coleta: latest?.collected_at ?? null };
  }));
  res.json(result);
});

app.get("/api/paginas", async (_req, res) => {
  const { rows } = await query("SELECT slug, nome, url, tipo, instagram_url, geo, nicho, funil FROM pages");
  res.json(rows);
});

// ─── Admin ───────────────────────────────────────────────────────────────────

app.get("/admin", async (_req, res) => {
  const { rows: pages } = await query(
    "SELECT slug, nome, url, tipo, instagram_url, geo, nicho, funil, created_at FROM pages ORDER BY tipo, created_at DESC"
  );

  // JSON de cada item, embutido no atributo data-item, usado pelo JS para preencher o formulário ao clicar em Editar
  function escAttr(str) {
    return String(str ?? "").replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  const lista = pages.map(p => `
    <tr>
      <td><span class="badge ${p.tipo === "dominio" ? "b-dom" : "b-pag"}">${p.tipo === "dominio" ? "🌐 Domínio" : "📡 Biblioteca"}</span></td>
      <td class="nome-cell">
        <div class="nome">${p.nome}</div>
        <div class="meta-badges">
          ${p.geo ? `<span class="meta-tag">🌍 ${p.geo}</span>` : ""}
          ${p.nicho ? `<span class="meta-tag">🏷️ ${p.nicho}</span>` : ""}
          ${p.funil ? `<span class="meta-tag">🎯 ${p.funil}</span>` : ""}
          ${p.instagram_url ? `<a href="${p.instagram_url}" target="_blank" class="ig-tag">
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="display:inline;vertical-align:middle;margin-right:3px"><rect width="24" height="24" rx="6" fill="url(#ig_admin)"/><circle cx="12" cy="12" r="4.5" stroke="white" stroke-width="1.8" fill="none"/><circle cx="17" cy="7" r="1.2" fill="white"/><rect x="3" y="3" width="18" height="18" rx="5" stroke="white" stroke-width="1.8" fill="none"/><defs><linearGradient id="ig_admin" x1="0" y1="24" x2="24" y2="0"><stop offset="0%" stop-color="#f09433"/><stop offset="25%" stop-color="#e6683c"/><stop offset="50%" stop-color="#dc2743"/><stop offset="75%" stop-color="#cc2366"/><stop offset="100%" stop-color="#bc1888"/></linearGradient></defs></svg>Instagram</a>` : ""}
        </div>
      </td>
      <td><a href="${p.url}" target="_blank" class="url-link">Ver na Meta ↗</a></td>
      <td>${new Date(p.created_at).toLocaleDateString("pt-BR")}</td>
      <td style="white-space:nowrap">
        <button type="button" class="btn-edit"
          data-slug="${escAttr(p.slug)}"
          data-nome="${escAttr(p.nome)}"
          data-url="${escAttr(p.url)}"
          data-tipo="${escAttr(p.tipo)}"
          data-instagram="${escAttr(p.instagram_url)}"
          data-geo="${escAttr(p.geo)}"
          data-nicho="${escAttr(p.nicho)}"
          data-funil="${escAttr(p.funil)}"
          onclick="editarItem(this)">✏️ Editar</button>
        <a href="/admin/funis/${p.slug}" class="btn-funis">🔀 Funis</a>
        <form method="POST" action="/admin/remover" style="display:inline" onsubmit="return confirm('Remover ${p.nome}?')">
          <input type="hidden" name="slug" value="${p.slug}">
          <button type="submit" class="btn-del">Remover</button>
        </form>
      </td>
    </tr>`).join("");

  const msgOk = (() => {
    const q = res.req?.query || {};
    if (q.ok === "1") return '<div class="msg ok">✅ Rastreamento cadastrado com sucesso.</div>';
    if (q.ok === "editado") return '<div class="msg ok">✏️ Rastreamento atualizado com sucesso.</div>';
    if (q.ok === "removido") return '<div class="msg ok">🗑️ Rastreamento removido.</div>';
    if (q.erro === "campos-obrigatorios") return '<div class="msg err">⚠️ Nome e URL são obrigatórios.</div>';
    if (q.erro === "nome-invalido") return '<div class="msg err">⚠️ Nome inválido.</div>';
    if (q.erro === "lote-vazio") return '<div class="msg err">⚠️ Nenhum item enviado no lote.</div>';
    if (q.erro === "lote-invalido") return '<div class="msg err">⚠️ Nenhuma linha válida encontrada no lote.</div>';
    if (q.erro === "lote-em-andamento") return '<div class="msg err">⚠️ Já existe um lote em andamento. Aguarde terminar.</div>';
    if (q.erro === "erro-interno") return '<div class="msg err">⚠️ Erro interno. Tente novamente.</div>';
    return "";
  })();

  res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lowticket Monitor — Admin</title>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:1000px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:28px;padding-bottom:16px;border-bottom:1px solid var(--border)}
.hdr h1{font-size:18px;font-weight:700}
.hdr a{margin-left:auto;font-size:13px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:7px 16px;border-radius:8px}
.hdr a:hover{background:var(--accent);color:#fff}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:22px 24px;margin-bottom:20px}
.card h2{font-size:14px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:18px}
.form-row{display:grid;grid-template-columns:160px 1fr 1fr;gap:12px;align-items:end;margin-bottom:12px}
.form-row-2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.form-row-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:12px}
@media(max-width:700px){.form-row,.form-row-2,.form-row-3{grid-template-columns:1fr}}
.field{display:flex;flex-direction:column;gap:6px}
label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.label-optional{font-size:10px;color:var(--muted);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px;opacity:.7}
input,select{background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:14px;padding:10px 14px;outline:none;transition:border-color .2s}
input:focus,select:focus{border-color:var(--accent)}
input::placeholder{color:var(--muted)}
.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:600;padding:10px 22px;cursor:pointer;transition:opacity .2s}
.btn:hover{opacity:.85}
.btn-del{background:transparent;color:var(--down);border:1px solid var(--down);border-radius:6px;font-family:'Space Grotesk',sans-serif;font-size:11px;padding:4px 10px;cursor:pointer;transition:all .2s;margin-left:6px}
.btn-edit{background:transparent;color:var(--accent);border:1px solid var(--accent);border-radius:6px;font-family:'Space Grotesk',sans-serif;font-size:11px;padding:4px 10px;cursor:pointer;transition:all .2s}
.btn-edit:hover{background:var(--accent);color:#fff}
.btn-funis{display:inline-block;background:transparent;color:#34d399;border:1px solid #34d399;border-radius:6px;font-family:'Space Grotesk',sans-serif;font-size:11px;padding:4px 10px;cursor:pointer;transition:all .2s;text-decoration:none;margin-left:6px}
.btn-funis:hover{background:#34d399;color:#0a0a14}
.btn-del:hover{background:var(--down);color:#fff}
.tip{font-size:12px;color:var(--muted);margin-top:14px;line-height:1.6;background:#0f0f1e;border-radius:8px;padding:12px 14px;border-left:3px solid var(--accent)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.6px;padding:10px 14px;text-align:left;border-bottom:1px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:middle}
.nome{font-weight:600;color:#fff}
.nome-cell{vertical-align:middle}
.meta-badges{display:flex;flex-wrap:wrap;gap:5px;margin-top:5px}
.meta-tag{font-size:10px;background:rgba(124,111,255,.12);color:#a78bfa;padding:2px 7px;border-radius:5px;font-weight:500}
.ig-tag{font-size:10px;background:rgba(220,39,67,.12);color:#fb7185;padding:2px 7px;border-radius:5px;font-weight:500;text-decoration:none;display:inline-flex;align-items:center;gap:3px}
.ig-tag:hover{background:rgba(220,39,67,.25)}
.url-link{color:var(--accent);font-size:12px;text-decoration:none;font-family:'Space Mono',monospace}
.url-link:hover{text-decoration:underline}
.badge{display:inline-block;padding:3px 9px;border-radius:6px;font-size:11px;font-weight:600}
.b-dom{background:rgba(124,111,255,.15);color:#a78bfa}
.b-pag{background:rgba(52,211,153,.12);color:#34d399}
.msg{padding:12px 16px;border-radius:8px;font-size:13px;margin-bottom:18px}
.msg.ok{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.25)}
.msg.err{background:rgba(251,113,133,.12);color:#fb7185;border:1px solid rgba(251,113,133,.25)}
.empty{color:var(--muted);font-size:13px;text-align:center;padding:24px}
.divider{border:none;border-top:1px solid var(--border);margin:16px 0}
</style>
</head>
<body>
<div class="hdr">
  <h1>⚙️ Admin — Lowticket Monitor</h1>
  <div style="margin-left:auto;display:flex;gap:8px;flex-wrap:wrap">
    <a href="/dashboard" style="font-size:13px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:7px 16px;border-radius:8px">← Ver Dashboard</a>
    <a href="/funis" style="font-size:13px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:7px 16px;border-radius:8px">🔀 Ver Mapa de Funis</a>
  </div>
</div>

${msgOk}

<div class="card" id="form-card">
  <h2 id="form-title">➕ Cadastrar novo rastreamento</h2>
  <form method="POST" action="/admin/salvar" id="mainForm">
    <input type="hidden" name="original_slug" id="originalSlug" value="">
    <div class="form-row">
      <div class="field">
        <label>Tipo</label>
        <select name="tipo" id="tipoSelect" onchange="atualizarDica()">
          <option value="pagina">📡 Biblioteca (página)</option>
          <option value="dominio">🌐 Domínio (URL)</option>
        </select>
      </div>
      <div class="field">
        <label>Nome</label>
        <input type="text" name="nome" id="nomeInput" placeholder="Ex: FlowForce Max ou FLOWFORCE.COM" required>
      </div>
      <div class="field">
        <label>URL da Meta Ad Library</label>
        <input type="url" name="url" id="urlInput" placeholder="https://www.facebook.com/ads/library/..." required>
      </div>
    </div>

    <hr class="divider">
    <div style="font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Informações adicionais <span style="font-weight:400;text-transform:none;letter-spacing:0;opacity:.6">(opcionais)</span></div>

    <div class="form-row-3">
      <div class="field">
        <label>Instagram <span class="label-optional">opcional</span></label>
        <input type="url" name="instagram_url" id="instagramInput" placeholder="https://www.instagram.com/perfil">
      </div>
      <div class="field">
        <label>Geo <span class="label-optional">opcional</span></label>
        <input type="text" name="geo" id="geoInput" placeholder="Ex: US, BR, UK">
      </div>
      <div class="field">
        <label>Nicho <span class="label-optional">opcional</span></label>
        <input type="text" name="nicho" id="nichoInput" placeholder="Ex: Próstata, Weight Loss, ED">
      </div>
    </div>

    <div style="display:flex;gap:10px;margin-top:4px">
      <button type="submit" class="btn" id="submitBtn">Cadastrar</button>
      <button type="button" class="btn" id="cancelBtn" style="display:none;background:transparent;border:1px solid var(--border);color:var(--text2)" onclick="cancelarEdicao()">Cancelar edição</button>
    </div>

    <div class="tip" id="dica">
      💡 <strong>Biblioteca:</strong> Cole a URL da página do anunciante na Meta Ad Library com filtro "Anúncios ativos".<br>
      Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&ad_type=all&id=XXXXXXXXX</code>
    </div>
  </form>
</div>

<div class="card">
  <h2>📦 Cadastro em lote</h2>
  <form method="POST" action="/admin/lote">
    <div class="field">
      <label>Uma linha por item</label>
      <textarea name="itens" rows="7"
        placeholder="FlowForce Max | https://www.facebook.com/ads/library/?view_all_page_id=123456 | https://instagram.com/flowforcemax&#10;FLOWFORCE.COM | https://www.facebook.com/ads/library/?q=FLOWFORCE.COM...&#10;dominio | AnotherOffer | https://www.facebook.com/ads/library/?q=ANOTHEROFFER.COM | https://instagram.com/anotheroffer"
        style="background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Mono',monospace;font-size:12px;padding:12px 14px;outline:none;resize:vertical;width:100%"></textarea>
    </div>
    <button type="submit" class="btn" style="margin-top:12px">Cadastrar lote</button>
    <div class="tip">
      💡 Formatos aceitos por linha:<br>
      <code>Nome | URL da Meta Ad Library</code><br>
      <code>Nome | URL da Meta Ad Library | https://instagram.com/perfil</code><br>
      <code>tipo | Nome | URL | https://instagram.com/perfil</code><br><br>
      O tipo (Biblioteca ou Domínio) é detectado automaticamente pela URL. O Instagram é opcional — basta omitir.<br>
      Geo e Nicho só podem ser preenchidos após o cadastro, editando o item individualmente no admin.<br>
      Cada item leva ~15-20s pra processar. A página não precisa ficar aberta.
    </div>
  </form>
  <div id="lote-progresso" style="display:none;margin-top:16px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:14px 16px">
    <div id="lote-texto" style="font-size:13px;color:var(--text2)"></div>
    <div style="background:var(--border);border-radius:6px;height:8px;margin-top:10px;overflow:hidden">
      <div id="lote-barra" style="background:var(--accent);height:100%;width:0%;transition:width .3s"></div>
    </div>
    <div id="lote-erros" style="font-size:12px;color:var(--down);margin-top:10px"></div>
  </div>
</div>

<div class="card">
  <h2>📋 Rastreamentos cadastrados (${pages.length})</h2>
  ${pages.length === 0 ? '<div class="empty">Nenhum rastreamento cadastrado ainda.</div>' : `
  <table>
    <thead><tr><th>Tipo</th><th>Nome / Metadados</th><th>Link Meta</th><th>Cadastrado</th><th></th></tr></thead>
    <tbody>${lista}</tbody>
  </table>`}
</div>

<script>
function atualizarDica(){
  const tipo=document.getElementById('tipoSelect').value;
  const dica=document.getElementById('dica');
  const url=document.getElementById('urlInput');
  if(tipo==='dominio'){
    dica.innerHTML='💡 <strong>Domínio:</strong> Cole a URL de busca por palavra-chave/domínio na Meta Ad Library.<br>Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&q=SEUDOMINIO.COM&search_type=keyword_unordered</code>';
    url.placeholder='https://www.facebook.com/ads/library/?active_status=active&q=SEUDOMINIO.COM...';
  }else{
    dica.innerHTML='💡 <strong>Biblioteca:</strong> Cole a URL da página do anunciante na Meta Ad Library com filtro "Anúncios ativos".<br>Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&ad_type=all&id=XXXXXXXXX</code>';
    url.placeholder='https://www.facebook.com/ads/library/?active_status=active&id=...';
  }
}

function editarItem(btn){
  document.getElementById('originalSlug').value=btn.dataset.slug;
  document.getElementById('nomeInput').value=btn.dataset.nome;
  document.getElementById('urlInput').value=btn.dataset.url;
  document.getElementById('tipoSelect').value=btn.dataset.tipo;
  document.getElementById('instagramInput').value=btn.dataset.instagram;
  document.getElementById('geoInput').value=btn.dataset.geo;
  document.getElementById('nichoInput').value=btn.dataset.nicho;
  document.getElementById('form-title').textContent='✏️ Editando: '+btn.dataset.nome;
  document.getElementById('submitBtn').textContent='Salvar alterações';
  document.getElementById('cancelBtn').style.display='inline-block';
  atualizarDica();
  document.getElementById('form-card').scrollIntoView({behavior:'smooth',block:'start'});
}

function cancelarEdicao(){
  document.getElementById('mainForm').reset();
  document.getElementById('originalSlug').value='';
  document.getElementById('form-title').textContent='➕ Cadastrar novo rastreamento';
  document.getElementById('submitBtn').textContent='Cadastrar';
  document.getElementById('cancelBtn').style.display='none';
  atualizarDica();
}

(function iniciarPollingLote(){
  const params=new URLSearchParams(window.location.search);
  const painel=document.getElementById('lote-progresso');
  const texto=document.getElementById('lote-texto');
  const barra=document.getElementById('lote-barra');
  const errosEl=document.getElementById('lote-erros');
  if(!painel)return;
  async function checarStatus(){
    try{
      const r=await fetch('/api/lote/status');
      const s=await r.json();
      if(!s.emAndamento&&!s.total)return;
      painel.style.display='block';
      const pct=s.total?Math.round((s.concluidos/s.total)*100):0;
      barra.style.width=pct+'%';
      if(s.emAndamento){
        texto.textContent='Processando '+s.concluidos+' de '+s.total+'... atual: '+(s.atual||'—');
        setTimeout(checarStatus,3000);
      }else{
        texto.textContent='Lote finalizado — '+s.concluidos+' de '+s.total+' itens processados.';
        if(s.erros&&s.erros.length){errosEl.innerHTML=s.erros.map(e=>'⚠️ '+e).join('<br>');}
      }
    }catch(e){console.error('Falha ao consultar status do lote',e);}
  }
  if(params.get('lote')==='iniciado'){checarStatus();}else{checarStatus();}
})();
</script>
</body>
</html>`);
});

app.post("/admin/salvar", async (req, res) => {
  const { nome, url, tipo, instagram_url, geo, nicho, funil, original_slug } = req.body;
  if (!nome || !url) return res.redirect("/admin?erro=campos-obrigatorios");
  const tipoFinal = tipo === "dominio" ? "dominio" : "pagina";

  // Modo edição: atualiza o registro existente pelo slug original — o slug NUNCA muda,
  // mesmo que o nome de exibição mude, para preservar o vínculo com scrape_history/scrape_latest.
  if (original_slug && original_slug.trim()) {
    try {
      const { rowCount } = await query(
        `UPDATE pages SET nome=$1, url=$2, tipo=$3, instagram_url=$4, geo=$5, nicho=$6, funil=$7 WHERE slug=$8`,
        [nome, url, tipoFinal, instagram_url || null, geo || null, nicho || null, funil || null, original_slug.trim()]
      );
      if (rowCount === 0) {
        console.warn(`[ADMIN] edição falhou — slug=${original_slug} não encontrado`);
        return res.redirect("/admin?erro=erro-interno");
      }
      console.log(`[ADMIN] editou slug=${original_slug}`);
      return res.redirect("/admin?ok=editado");
    } catch (err) {
      console.error("[ADMIN] erro ao editar:", err.message);
      return res.redirect("/admin?erro=erro-interno");
    }
  }

  // Modo cadastro (novo item)
  const slug = toSlug(nome);
  if (!slug) return res.redirect("/admin?erro=nome-invalido");
  try {
    await query(
      `INSERT INTO pages (slug, nome, url, tipo, instagram_url, geo, nicho, funil)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (slug) DO UPDATE
         SET nome=$2, url=$3, tipo=$4,
             instagram_url=COALESCE(EXCLUDED.instagram_url, pages.instagram_url),
             geo=COALESCE(EXCLUDED.geo, pages.geo),
             nicho=COALESCE(EXCLUDED.nicho, pages.nicho),
             funil=COALESCE(EXCLUDED.funil, pages.funil)`,
      [slug, nome, url, tipoFinal, instagram_url || null, geo || null, nicho || null, funil || null]
    );
    console.log(`[ADMIN] cadastrou slug=${slug} tipo=${tipoFinal}`);
    await captureInicial(slug, url);
    res.redirect("/admin?ok=1");
  } catch (err) {
    console.error("[ADMIN] erro:", err.message);
    res.redirect("/admin?erro=erro-interno");
  }
});

app.post("/admin/remover", async (req, res) => {
  const { slug } = req.body;
  if (!slug) return res.redirect("/admin");
  await query("DELETE FROM pages WHERE slug=$1", [slug]);
  await query("DELETE FROM scrape_history WHERE slug=$1", [slug]);
  await query("DELETE FROM scrape_latest WHERE slug=$1", [slug]);
  console.log(`[ADMIN] removeu slug=${slug}`);
  res.redirect("/admin?ok=removido");
});

app.post("/admin/lote", async (req, res) => {
  const { itens: textoItens } = req.body;
  if (!textoItens || !textoItens.trim()) return res.redirect("/admin?erro=lote-vazio");
  const itens = parseLoteInput(textoItens);
  if (!itens.length) return res.redirect("/admin?erro=lote-invalido");
  if (loteStatus.emAndamento) return res.redirect("/admin?erro=lote-em-andamento");
  res.redirect("/admin?lote=iniciado");
  runLote(itens).catch((err) => console.error("[LOTE] erro não tratado:", err.message));
});

app.get("/api/lote/status", (_req, res) => {
  res.json(loteStatus);
});

// ─── Funis (modelo de grafo: nós + conexões) ────────────────────────────────

const TIPO_INFO = {
  ads:         { icon: "📢", label: "ADS" },
  advertorial: { icon: "📄", label: "Advertorial" },
  presell:     { icon: "🧲", label: "Presell" },
  tsl:         { icon: "📝", label: "TSL" },
  vsl:         { icon: "🎬", label: "VSL" },
  quiz:        { icon: "🧩", label: "Quiz" },
  whatsapp:    { icon: "💬", label: "WhatsApp" },
  checkout:    { icon: "💳", label: "Checkout" },
};
const TIPOS_ORDEM = ["ads", "advertorial", "presell", "tsl", "vsl", "quiz", "whatsapp", "checkout"];

// Computa todos os caminhos (raiz → folha) de um grafo de nós/conexões.
// Raiz = nó sem conexão de entrada. Folha = nó sem conexão de saída.
// Guarda contra ciclos interrompendo o caminho se o nó já apareceu nele.
function computarCaminhos(nodes, edges) {
  const nodesById = {};
  nodes.forEach(n => { nodesById[n.id] = n; });

  const adjOut = {};
  const adjIn = {};
  const adjUndir = {};
  nodes.forEach(n => {
    adjOut[n.id] = [];
    adjIn[n.id] = [];
    adjUndir[n.id] = [];
  });
  edges.forEach(e => {
    if (adjOut[e.from_node_id] && adjIn[e.to_node_id]) {
      adjOut[e.from_node_id].push(e.to_node_id);
      adjIn[e.to_node_id].push(e.from_node_id);
      adjUndir[e.from_node_id].push(e.to_node_id);
      adjUndir[e.to_node_id].push(e.from_node_id);
    }
  });

  const visited = new Set();
  const componentes = [];
  nodes.forEach(n => {
    if (visited.has(n.id)) return;
    if (adjUndir[n.id].length === 0) return;
    const comp = [];
    const queue = [n.id];
    visited.add(n.id);
    while (queue.length > 0) {
      const curr = queue.shift();
      comp.push(curr);
      adjUndir[curr].forEach(nxt => {
        if (!visited.has(nxt)) {
          visited.add(nxt);
          queue.push(nxt);
        }
      });
    }
    if (comp.length >= 2) componentes.push(comp);
  });

  const funisMapeados = [];
  componentes.forEach(comp => {
    const compSet = new Set(comp);
    let raizes = comp.filter(id => adjIn[id].filter(x => compSet.has(x)).length === 0);
    if (raizes.length === 0) raizes = [comp[0]];

    const levels = [];
    const assigned = new Set();
    let currentLevel = [...raizes];
    currentLevel.forEach(id => assigned.add(id));

    while (currentLevel.length > 0) {
      levels.push(currentLevel.map(id => nodesById[id]).filter(Boolean));
      const nextLevelSet = new Set();
      currentLevel.forEach(id => {
        adjOut[id].forEach(nxt => {
          if (compSet.has(nxt) && !assigned.has(nxt)) {
            nextLevelSet.add(nxt);
          }
        });
      });
      currentLevel = Array.from(nextLevelSet);
      currentLevel.forEach(id => assigned.add(id));
    }

    const remaining = comp.filter(id => !assigned.has(id)).map(id => nodesById[id]).filter(Boolean);
    if (remaining.length > 0) {
      if (levels.length > 0) levels[levels.length - 1].push(...remaining);
      else levels.push(remaining);
    }

    function buildTree(id, treeVisited = new Set()) {
      if (treeVisited.has(id)) return null;
      treeVisited.add(id);
      const node = nodesById[id];
      if (!node) return null;
      const childrenIds = (adjOut[id] || []).filter(nxt => compSet.has(nxt));
      const children = childrenIds
        .map(cid => buildTree(cid, new Set(treeVisited)))
        .filter(Boolean);
      return { node, children };
    }

    const rootTrees = raizes.map(rid => buildTree(rid)).filter(Boolean);
    const mainTree = rootTrees.length === 1 ? rootTrees[0] : { node: null, children: rootTrees };

    funisMapeados.push({
      tree: mainTree,
      levels: levels,
      allNodes: comp.map(id => nodesById[id]).filter(Boolean)
    });
  });

  return funisMapeados;
}

async function getNodesEdges(slug) {
  const { rows: nodes } = await query(
    `SELECT id, tipo, rotulo, url FROM funnel_nodes WHERE slug=$1 ORDER BY created_at ASC`, [slug]
  );
  let edges = [];
  if (nodes.length) {
    const ids = nodes.map(n => n.id);
    const { rows } = await query(
      `SELECT id, from_node_id, to_node_id FROM funnel_edges WHERE from_node_id = ANY($1) OR to_node_id = ANY($1)`,
      [ids]
    );
    edges = rows;
  }
  return { nodes, edges };
}

function renderChip(node) {
  const info = TIPO_INFO[node.tipo] || { icon: "🔗", label: node.tipo };
  return `<a href="${node.url}" target="_blank" rel="noopener" class="chip" title="Abrir ${node.rotulo} em nova guia">
    <span class="chip-icon">${info.icon}</span>
    <span class="chip-label">${node.rotulo}</span>
  </a>`;
}

function renderNotionLinearConnector() {
  return `<span class="flow-notion-arrow" style="display:inline-flex;align-items:center;margin:0 6px;flex-shrink:0">
    <svg width="24" height="12" viewBox="0 0 24 12" fill="none">
      <line x1="0" y1="6" x2="18" y2="6" stroke="#7c6fff" stroke-width="2" stroke-linecap="round"/>
      <path d="M15 2L20 6L15 10" stroke="#7c6fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  </span>`;
}

function renderNotionBranchConnector() {
  return `<span class="flow-branch-arrow" style="display:inline-flex;align-items:center;margin-right:6px;flex-shrink:0">
    <svg width="10" height="12" viewBox="0 0 10 12" fill="none">
      <path d="M1 2L6 6L1 10" stroke="#a78bfa" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  </span>`;
}

function renderTreeNode(treeNode) {
  if (!treeNode) return "";
  if (!treeNode.node && treeNode.children) {
    return treeNode.children.map(renderTreeNode).join("");
  }
  const chipHtml = renderChip(treeNode.node);
  if (!treeNode.children || treeNode.children.length === 0) {
    return `<div class="flow-tree-node">${chipHtml}</div>`;
  }
  if (treeNode.children.length === 1) {
    return `<div class="flow-tree-node">${chipHtml}<div class="flow-linear-branch">${renderNotionLinearConnector()}${renderTreeNode(treeNode.children[0])}</div></div>`;
  }
  const branchesHtml = treeNode.children.map(child => `
    <div class="flow-tree-branch">
      ${renderNotionBranchConnector()}
      <div class="flow-branch-content">
        ${renderTreeNode(child)}
      </div>
    </div>
  `).join("");

  return `<div class="flow-tree-node">${chipHtml}<div class="flow-tree-branches">${branchesHtml}</div></div>`;
}

function renderCaminho(funilItem, idx = 0, isEditMode = false, explicitSlug = "") {
  let tree, levels, allNodes;
  if (funilItem && funilItem.tree) {
    tree = funilItem.tree;
    levels = funilItem.levels;
    allNodes = funilItem.allNodes;
  } else if (Array.isArray(funilItem)) {
    levels = Array.isArray(funilItem[0]) ? funilItem : funilItem.map(n => [n]);
    allNodes = levels.flat();
    tree = { node: allNodes[0], children: [] };
  } else {
    return "";
  }

  const slug = explicitSlug || allNodes[0]?.slug || "";
  const label = allNodes.map(n => n.rotulo).join(' \u2192 ');
  const levelsJson = JSON.stringify(levels.map(lvl => lvl.map(n => n.id)));
  const allIdsJson = JSON.stringify(allNodes.map(n => n.id));

  const treeHtml = renderTreeNode(tree);

  if (!isEditMode) {
    return `<div class="caminho-row flow-row-container" style="display:flex;align-items:center;overflow-x:auto;padding:14px 16px">
      ${treeHtml}
    </div>`;
  }

  return `<div class="caminho-row flow-row-container" style="display:flex;align-items:center;justify-content:space-between;gap:16px;overflow-x:auto;padding:14px 16px">
    <div style="display:flex;align-items:center">
      ${treeHtml}
    </div>
    <div style="display:flex;align-items:center;gap:8px;margin-left:auto;flex-shrink:0">
      <button type="button" class="btn-edit-sm" onclick='editarFunil(${levelsJson})' title="Editar este funil no construtor">\u270F\uFE0F Editar</button>
      <form id="form-rem-caminho-${idx}" method="POST" action="/admin/funis/remover-caminho" style="display:none">
        <input type="hidden" name="slug" value="${slug}">
        <input type="hidden" name="funil_node_ids" value='${allIdsJson}'>
      </form>
      <button type="button" class="btn-del-sm" onclick="abrirModalRemover('caminho', ${idx}, 'Excluir funil mapeado', '${label.replace(/'/g, "\\'")}')">\u2715 Excluir</button>
    </div>
  </div>`;
}

// Página de gerenciamento de nós/conexões de um player
app.get("/admin/funis/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows: pages } = await query("SELECT nome, url FROM pages WHERE slug=$1 LIMIT 1", [slug]);
  if (!pages.length) return res.status(404).send("Player não encontrado.");
  const nomePage = pages[0].nome;

  const { nodes, edges } = await getNodesEdges(slug);
  const nodesById = {};
  nodes.forEach(n => { nodesById[n.id] = n; });

  const optionsNodes = nodes.map(n => {
    const info = TIPO_INFO[n.tipo] || { icon: "🔗", label: n.tipo };
    return `<option value="${n.id}">${info.icon} ${n.rotulo} (${info.label})</option>`;
  }).join("");

  const optionsTipos = TIPOS_ORDEM.map(t => `<option value="${t}">${TIPO_INFO[t].icon} ${TIPO_INFO[t].label}</option>`).join("");

  const listaNodes = nodes.length ? nodes.map(n => {
    const info = TIPO_INFO[n.tipo] || { icon: "🔗", label: n.tipo };
    return `<div class="node-row">
      <span class="node-tipo">${info.icon} ${info.label}</span>
      <span class="node-rotulo">${n.rotulo}</span>
      <a href="${n.url}" target="_blank" class="node-url">${n.url.length > 45 ? n.url.slice(0,45)+'...' : n.url}</a>
      <form id="form-rem-node-${n.id}" method="POST" action="/admin/funis/remover-node" style="display:none">
        <input type="hidden" name="node_id" value="${n.id}">
        <input type="hidden" name="slug" value="${slug}">
      </form>
      <button type="button" class="btn-del-sm" onclick="abrirModalRemover('node', ${n.id}, 'Remover etapa', '${n.rotulo.replace(/'/g, "\\'")}')">✕</button>
    </div>`;
  }).join("") : '<div class="empty-hint-sm">Nenhuma etapa cadastrada ainda. Crie a primeira acima.</div>';

  const listaEdges = edges.length ? edges.map(e => {
    const de = nodesById[e.from_node_id], para = nodesById[e.to_node_id];
    if (!de || !para) return "";
    const label = de.rotulo + ' \u2192 ' + para.rotulo;
    return `<div class="edge-row">
      ${renderChip(de)}<span class="chip-arrow">→</span>${renderChip(para)}
      <form id="form-rem-${e.id}" method="POST" action="/admin/funis/remover-edge" style="display:none">
        <input type="hidden" name="edge_id" value="${e.id}">
        <input type="hidden" name="slug" value="${slug}">
      </form>
      <button type="button" class="btn-del-sm" style="margin-left:auto"
        onclick="abrirModalRemover(${e.id},'${label.replace(/'/g, "\\'")}')">&#x2715;</button>
    </div>`;
  }).join("") : '<div class="empty-hint-sm">Nenhuma conexão criada ainda.</div>';

  const caminhos = computarCaminhos(nodes, edges);
  const previaCaminhos = caminhos.length
    ? caminhos.map((c, idx) => renderCaminho(c, idx, true, slug)).join("")
    : '<div class="empty-hint-sm">Cadastre etapas e conecte-as para ver os funis mapeados aqui.</div>';

  const msgOk = (() => {
    const q = req.query;
    if (q.ok === "node-add") return '<div class="msg ok">✅ Etapa criada.</div>';
    if (q.ok === "node-rem") return '<div class="msg ok">🗑️ Etapa removida.</div>';
    if (q.ok === "edge-add") return '<div class="msg ok">✅ Conexão criada.</div>';
    if (q.ok === "edge-rem") return '<div class="msg ok">🗑️ Conexão removida.</div>';
    if (q.erro === "sem-etapas") return '<div class="msg err">⚠️ Cadastre pelo menos 2 etapas antes de conectar.</div>';
    if (q.erro === "mesma-etapa") return '<div class="msg err">⚠️ Uma etapa não pode se conectar a ela mesma.</div>';
    return "";
  })();

  res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Funil — ${nomePage}</title>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:900px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.hdr h1{font-size:17px;font-weight:700}
.hdr-sub{font-size:12px;color:var(--muted);margin-top:3px}
.hdr-nav{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
.hdr-nav a{font-size:12px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:6px 14px;border-radius:8px;white-space:nowrap}
.hdr-nav a:hover{background:var(--accent);color:#fff}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:20px 22px;margin-bottom:18px}
.card h2{font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:16px}
.field{display:flex;flex-direction:column;gap:6px}
label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
input,select{background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:13px;padding:9px 13px;outline:none;transition:border-color .2s}
input:focus,select:focus{border-color:var(--accent)}
input::placeholder{color:var(--muted)}
.form-row{display:grid;grid-template-columns:180px 160px 1fr auto;gap:10px;align-items:end}
.form-row-edge{display:grid;grid-template-columns:1fr auto 1fr auto;gap:10px;align-items:end}
.chain-builder{display:flex;align-items:flex-end;flex-wrap:wrap;gap:8px}
.chain-elo{display:flex;flex-direction:column;gap:6px;min-width:165px;flex:1}
.elo-selects{display:flex;flex-direction:column;gap:6px}
.btn-elo-bifurcar{background:transparent;color:#a78bfa;border:1px dashed rgba(167,139,250,0.35);border-radius:6px;font-size:11px;font-weight:600;padding:5px 8px;cursor:pointer;margin-top:2px;width:100%;transition:all .15s}
.btn-elo-bifurcar:hover{background:rgba(167,139,250,0.12);border-color:#a78bfa;color:#fff}
.funil-seta-apple{display:inline-flex;align-items:center;color:#a78bfa;margin:0 5px;opacity:0.85}
.funil-seta-apple svg{display:block;filter:drop-shadow(0 0 4px rgba(167,139,250,0.4))}
.chain-arrow{color:var(--muted);font-size:16px;padding-bottom:10px;flex-shrink:0}
.btn-chain-add{background:transparent;color:var(--accent);border:1px solid var(--accent);border-radius:6px;font-size:18px;font-weight:700;width:32px;height:36px;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;line-height:1;transition:all .15s;padding:0;margin-bottom:1px}
.btn-chain-add:hover{background:var(--accent);color:#fff}
.btn-chain-rem{background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:12px;width:32px;height:36px;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;line-height:1;transition:all .15s;padding:0;margin-bottom:1px}
.btn-chain-rem:hover{color:var(--down);border-color:var(--down)}
@media(max-width:700px){.form-row,.form-row-edge{grid-template-columns:1fr}.chain-builder{flex-direction:column}}
.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;font-family:'Space Grotesk',sans-serif;font-size:13px;font-weight:600;padding:9px 20px;cursor:pointer;white-space:nowrap}
.btn:hover{opacity:.85}
.btn-del-sm{background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:11px;padding:5px 10px;cursor:pointer;line-height:1.4;transition:all .15s}
.btn-del-sm:hover{color:var(--down);border-color:var(--down);background:rgba(251,113,133,.08)}
.btn-edit-sm{background:rgba(167,139,250,.12);color:#a78bfa;border:1px solid rgba(167,139,250,.3);border-radius:6px;font-size:11px;padding:5px 10px;cursor:pointer;line-height:1.4;transition:all .15s;font-weight:600}
.btn-edit-sm:hover{background:var(--accent);color:#fff}
.btn-insert-mid{background:transparent;color:var(--accent);border:1px dashed var(--accent);border-radius:5px;font-size:11px;font-weight:700;padding:2px 7px;cursor:pointer;line-height:1.2;transition:all .15s}
.btn-insert-mid:hover{background:var(--accent);color:#fff}
.node-row{display:flex;align-items:center;gap:10px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:9px 12px;margin-bottom:8px;flex-wrap:wrap}
.node-tipo{font-size:11px;font-weight:600;color:#a78bfa;background:rgba(167,139,250,.12);padding:3px 9px;border-radius:5px;white-space:nowrap}
.node-rotulo{font-size:13px;font-weight:700;color:#fff}
.node-url{font-size:11px;color:var(--accent);text-decoration:none;font-family:'Space Mono',monospace;margin-left:auto}
.node-url:hover{text-decoration:underline}
.edge-row{display:flex;align-items:center;gap:6px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:9px 12px;margin-bottom:8px;flex-wrap:wrap}
.chip{display:inline-flex;align-items:center;gap:5px;background:var(--surface);border:1px solid var(--border);border-radius:7px;padding:5px 10px;text-decoration:none;font-size:12px;font-weight:600;color:#fff}
.chip:hover{border-color:var(--accent)}
.chip-icon{font-size:13px}
.chip-arrow{color:var(--muted);font-size:15px;font-weight:700;margin:0 2px}
.caminho-row{display:flex;align-items:center;flex-wrap:wrap;gap:2px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:8px}
.flow-tree-node{display:inline-flex;align-items:center}
.flow-linear-branch{display:inline-flex;align-items:center}
.flow-tree-branches{display:flex;flex-direction:column;justify-content:center;gap:12px;position:relative;margin-left:8px;padding-left:18px}
.flow-tree-branches::before{content:'';position:absolute;left:0;top:16px;bottom:16px;width:2px;background:rgba(167,139,250,0.45);border-radius:2px}
.flow-tree-branch{display:inline-flex;align-items:center;position:relative}
.flow-tree-branch::before{content:'';position:absolute;left:-18px;top:50%;width:12px;height:2px;background:rgba(167,139,250,0.45)}
.flow-branch-content{display:inline-flex;align-items:center}
.flow-row-container{min-height:54px}
.empty-hint-sm{color:var(--muted);font-size:12px;text-align:center;padding:16px;border:1px dashed var(--border);border-radius:8px}
.msg{padding:11px 14px;border-radius:8px;font-size:13px;margin-bottom:16px}
.msg.ok{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.25)}
.msg.err{background:rgba(251,113,133,.12);color:#fb7185;border:1px solid rgba(251,113,133,.25)}
.divider{border:none;border-top:1px solid var(--border);margin:14px 0}
/* Modal Apple-style */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);display:flex;align-items:center;justify-content:center;z-index:9999;opacity:0;pointer-events:none;transition:opacity .2s ease}
.modal-overlay.open{opacity:1;pointer-events:all}
.modal-card{background:#1c1c2e;border:1px solid rgba(255,255,255,.1);border-radius:20px;padding:30px 28px 24px;max-width:340px;width:calc(100% - 40px);box-shadow:0 40px 100px rgba(0,0,0,.7),0 0 0 1px rgba(255,255,255,.06);transform:scale(.92) translateY(12px);transition:transform .28s cubic-bezier(.34,1.4,.64,1),opacity .22s ease;opacity:0;text-align:center}
.modal-overlay.open .modal-card{transform:scale(1) translateY(0);opacity:1}
.modal-icon{font-size:38px;margin-bottom:14px;line-height:1}
.modal-title{font-size:16px;font-weight:700;color:#fff;margin-bottom:8px;letter-spacing:-.2px}
.modal-desc{font-size:13px;color:#8888aa;margin-bottom:24px;line-height:1.6;word-break:break-all}
.modal-actions{display:flex;gap:10px}
.modal-btn-cancel{flex:1;background:rgba(255,255,255,.07);color:#e0e0f0;border:1px solid rgba(255,255,255,.1);border-radius:12px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:600;padding:12px 0;cursor:pointer;transition:background .15s}
.modal-btn-cancel:hover{background:rgba(255,255,255,.13)}
.modal-btn-confirm{flex:1;background:#fb7185;color:#fff;border:none;border-radius:12px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:700;padding:12px 0;cursor:pointer;transition:background .15s,transform .1s}
.modal-btn-confirm:hover{background:#f43f5e}
.modal-btn-confirm:active{transform:scale(.97)}
</style>
</head>
<body>
<!-- Modal Apple-style universal -->
<div class="modal-overlay" id="modal-remover-apple" onclick="fecharModalAppleOverlay(event)">
  <div class="modal-card">
    <div class="modal-icon">🗑️</div>
    <h3 class="modal-title" id="modal-apple-title">Confirmar exclusão</h3>
    <p class="modal-desc" id="modal-apple-desc"></p>
    <div class="modal-actions">
      <button class="modal-btn-cancel" onclick="fecharModalApple()">Cancelar</button>
      <button class="modal-btn-confirm" onclick="confirmarRemoverApple()">Excluir</button>
    </div>
  </div>
</div>
<div class="hdr">
  <div>
    <h1>🔀 Funil — ${nomePage}</h1>
    <div class="hdr-sub">Mapeamento de etapas e conexões</div>
  </div>
  <div class="hdr-nav">
    <a href="/admin">⚙️ Admin</a>
    <a href="/dashboard">📊 Dashboard</a>
    <a href="/funis">🔀 Ver Mapa de Funis</a>
  </div>
</div>

${msgOk}

<div class="card">
  <h2>➕ Nova etapa</h2>
  <form method="POST" action="/admin/funis/add-node">
    <input type="hidden" name="slug" value="${slug}">
    <div class="form-row">
      <div class="field"><label>Tipo</label><select name="tipo">${optionsTipos}</select></div>
      <div class="field"><label>Rótulo</label><input type="text" name="rotulo" placeholder="Ex: TSL2, Checkout1" required></div>
      <div class="field"><label>URL</label><input type="url" name="url" placeholder="https://..." required></div>
      <button type="submit" class="btn">Adicionar</button>
    </div>
  </form>
</div>

<div class="card">
  <h2>📋 Etapas cadastradas (${nodes.length})</h2>
  ${listaNodes}
</div>

<div class="card" id="card-conectar-etapas">
  <h2>🔗 Conectar etapas</h2>
  ${nodes.length < 2
    ? '<div class="empty-hint-sm">Cadastre pelo menos 2 etapas para poder conectá-las.</div>'
    : `<form method="POST" action="/admin/funis/add-edge" id="form-add-edge">
        <input type="hidden" name="slug" value="${slug}">
        <div style="font-size:11px;color:var(--muted);margin-bottom:12px;line-height:1.5">
          Monte a sequência completa do funil. Cada etapa conecta à próxima em cadeia.
        </div>
        <input type="hidden" name="chain_steps_json" id="chain_steps_json">
        <div class="chain-builder" id="chain-builder">
          <div class="chain-elo">
            <label>Etapa 1</label>
            <div class="elo-selects">
              <select name="chain_ids[]">${optionsNodes}</select>
            </div>
            <button type="button" class="btn-elo-bifurcar" onclick="bifurcarElo(this)" title="Adicionar bifurcação nesta etapa">🔀 Bifurcar</button>
          </div>
          <div class="chain-arrow" id="chain-arrow-1">→</div>
          <div class="chain-elo" id="chain-elo-2">
            <label>Etapa 2</label>
            <div class="elo-selects">
              <select name="chain_ids[]">${optionsNodes}</select>
            </div>
            <button type="button" class="btn-elo-bifurcar" onclick="bifurcarElo(this)" title="Adicionar bifurcação nesta etapa">🔀 Bifurcar</button>
          </div>
          <button type="button" class="btn-chain-add" id="btn-chain-add" onclick="adicionarElo()" title="Adicionar próxima etapa na cadeia">+</button>
        </div>
        <div style="margin-top:14px;display:flex;justify-content:flex-end">
          <button type="submit" class="btn">Conectar Cadeia</button>
        </div>
      </form>`}
</div>

<div class="card">
  <h2>✨ FUNIS MAPEADOS (${caminhos.length})</h2>
  ${previaCaminhos}
</div>

<script>
var _chainCount=2;
var _optionsHtml=(function(){
  var sel=document.querySelector('#chain-builder select');
  return sel?sel.innerHTML:'';
})();
function adicionarElo(val){
  _chainCount++;
  var builder=document.getElementById('chain-builder');
  var addBtn=document.getElementById('btn-chain-add');
  var arrow=document.createElement('div');
  arrow.className='chain-arrow';
  arrow.textContent='\u2192';
  var elo=document.createElement('div');
  elo.className='chain-elo';
  elo.id='chain-elo-'+_chainCount;
  var lbl=document.createElement('label');
  lbl.textContent='Etapa '+_chainCount;
  var selWrap=document.createElement('div');
  selWrap.className='elo-selects';
  var sel=document.createElement('select');
  sel.name='chain_ids[]';
  sel.innerHTML=_optionsHtml;
  if(val) sel.value=val;
  selWrap.appendChild(sel);
  var bifBtn=document.createElement('button');
  bifBtn.type='button';
  bifBtn.className='btn-elo-bifurcar';
  bifBtn.textContent='🔀 Bifurcar';
  bifBtn.title='Adicionar bifurcação nesta etapa';
  bifBtn.onclick=function(){bifurcarElo(bifBtn);};
  var remBtn=document.createElement('button');
  remBtn.type='button';
  remBtn.className='btn-chain-rem';
  remBtn.textContent='\u2715';
  remBtn.title='Remover esta etapa';
  remBtn.onclick=function(){arrow.remove();elo.remove();renumerarCadeia();};
  elo.appendChild(lbl);
  elo.appendChild(selWrap);
  elo.appendChild(bifBtn);
  builder.insertBefore(arrow,addBtn);
  builder.insertBefore(elo,addBtn);
  builder.insertBefore(remBtn,addBtn);
}
function bifurcarElo(btnEl){
  var elo=btnEl.closest('.chain-elo');
  var wrap=elo.querySelector('.elo-selects');
  var row=document.createElement('div');
  row.style.cssText='display:flex;align-items:center;gap:4px;margin-top:4px';
  var sel=document.createElement('select');
  sel.name='chain_ids[]';
  sel.innerHTML=_optionsHtml;
  var rm=document.createElement('button');
  rm.type='button';
  rm.className='btn-chain-rem';
  rm.style.padding='2px 6px';
  rm.textContent='\u2715';
  rm.onclick=function(){row.remove();};
  row.appendChild(sel);
  row.appendChild(rm);
  wrap.appendChild(row);
}
function renumerarCadeia(){
  var elos=document.querySelectorAll('#chain-builder .chain-elo label');
  elos.forEach(function(lbl, i){ lbl.textContent='Etapa '+(i+1); });
}
function editarFunil(levels){
  var builder=document.getElementById('chain-builder');
  if(!builder) return;
  if(!Array.isArray(levels[0])) levels = levels.map(function(id){ return [id]; });
  
  builder.innerHTML = '';
  _chainCount = 0;
  levels.forEach(function(stepIds, idx){
    if(idx > 0){
      var arrow=document.createElement('div');
      arrow.className='chain-arrow';
      arrow.textContent='\u2192';
      builder.appendChild(arrow);
    }
    _chainCount++;
    var elo=document.createElement('div');
    elo.className='chain-elo';
    elo.id='chain-elo-'+_chainCount;
    var lbl=document.createElement('label');
    lbl.textContent='Etapa '+_chainCount;
    var selWrap=document.createElement('div');
    selWrap.className='elo-selects';
    
    stepIds.forEach(function(nodeId, sIdx){
      var row=document.createElement('div');
      row.style.cssText=sIdx===0 ? '' : 'display:flex;align-items:center;gap:4px;margin-top:4px';
      var sel=document.createElement('select');
      sel.name='chain_ids[]';
      sel.innerHTML=_optionsHtml;
      sel.value=nodeId;
      if(sIdx===0){
        selWrap.appendChild(sel);
      } else {
        var rm=document.createElement('button');
        rm.type='button';
        rm.className='btn-chain-rem';
        rm.style.padding='2px 6px';
        rm.textContent='\u2715';
        rm.onclick=function(){row.remove();};
        row.appendChild(sel);
        row.appendChild(rm);
        selWrap.appendChild(row);
      }
    });

    var bifBtn=document.createElement('button');
    bifBtn.type='button';
    bifBtn.className='btn-elo-bifurcar';
    bifBtn.textContent='🔀 Bifurcar';
    bifBtn.onclick=function(){bifurcarElo(bifBtn);};

    elo.appendChild(lbl);
    elo.appendChild(selWrap);
    elo.appendChild(bifBtn);

    if(_chainCount > 2){
      var remEloBtn=document.createElement('button');
      remEloBtn.type='button';
      remEloBtn.className='btn-chain-rem';
      remEloBtn.textContent='\u2715';
      remEloBtn.title='Remover esta etapa';
      remEloBtn.onclick=function(){elo.previousSibling?.remove();elo.remove();renumerarCadeia();};
      elo.appendChild(remEloBtn);
    }

    builder.appendChild(elo);
  });

  var addBtn=document.createElement('button');
  addBtn.type='button';
  addBtn.className='btn-chain-add';
  addBtn.id='btn-chain-add';
  addBtn.onclick=function(){adicionarElo();};
  addBtn.textContent='+';
  addBtn.title='Adicionar próxima etapa na cadeia';
  builder.appendChild(addBtn);

  var card=document.getElementById('card-conectar-etapas');
  if(card){
    card.scrollIntoView({behavior:'smooth',block:'center'});
    card.style.transition='box-shadow 0.4s ease, border-color 0.4s ease';
    card.style.borderColor='var(--accent)';
    card.style.boxShadow='0 0 0 2px rgba(167,139,250,0.4)';
    setTimeout(function(){ card.style.borderColor=''; card.style.boxShadow=''; }, 1500);
  }
}
function editarCaminho(ids){ editarFunil(ids); }

/* ── Modal Apple-style para remover ── */
var _itemToRemove=null;
function abrirModalRemover(tipo,id,titulo,descricao){
  _itemToRemove={tipo:tipo, id:id};
  document.getElementById('modal-apple-title').textContent=titulo;
  document.getElementById('modal-apple-desc').textContent=descricao;
  var m=document.getElementById('modal-remover-apple');
  m.classList.add('open');
  document.body.style.overflow='hidden';
}
function fecharModalApple(){
  var m=document.getElementById('modal-remover-apple');
  m.classList.remove('open');
  document.body.style.overflow='';
}
function fecharModalAppleOverlay(e){
  if(e.target===document.getElementById('modal-remover-apple')) fecharModalApple();
}
function confirmarRemoverApple(){
  if(!_itemToRemove) return;
  sessionStorage.setItem('funil_scroll_y', window.scrollY);
  var frm = document.getElementById('form-rem-'+_itemToRemove.tipo+'-'+_itemToRemove.id);
  if(frm) frm.submit();
}
/* Restaurar posição do scroll após redirect sem pular para o topo */
function restoreScrollPosition(){
  var y=sessionStorage.getItem('funil_scroll_y');
  if(y!==null){
    window.scrollTo({top:parseInt(y,10),behavior:'instant'});
    sessionStorage.removeItem('funil_scroll_y');
  }
}
restoreScrollPosition();
window.addEventListener('DOMContentLoaded', function(){
  restoreScrollPosition();
  var frm = document.getElementById('form-add-edge');
  if(frm){
    frm.addEventListener('submit', function(){
      var elos = document.querySelectorAll('#chain-builder .chain-elo');
      var steps = [];
      elos.forEach(function(elo){
        var selects = elo.querySelectorAll('select');
        var ids = [];
        selects.forEach(function(s){
          if(s.value && ids.indexOf(s.value) === -1) ids.push(s.value);
        });
        if(ids.length > 0) steps.push(ids);
      });
      var hidden = document.getElementById('chain_steps_json');
      if(hidden) hidden.value = JSON.stringify(steps);
    });
  }
});
</script>
</body>
</html>`);
});

app.post("/admin/funis/add-node", async (req, res) => {
  const { slug, tipo, rotulo, url } = req.body;
  if (!slug || !tipo || !rotulo || !url) return res.redirect(`/admin/funis/${slug || ""}`);
  if (!TIPOS_ORDEM.includes(tipo)) return res.redirect(`/admin/funis/${slug}`);
  await query(`INSERT INTO funnel_nodes (slug, tipo, rotulo, url) VALUES ($1,$2,$3,$4)`, [slug, tipo, rotulo, url]);
  console.log(`[FUNIS] add-node slug=${slug} tipo=${tipo} rotulo=${rotulo}`);
  res.redirect(`/admin/funis/${slug}?ok=node-add`);
});

app.post("/admin/funis/remover-node", async (req, res) => {
  const { node_id, slug } = req.body;
  if (!node_id) return res.redirect("/admin");
  await query(`DELETE FROM funnel_nodes WHERE id=$1`, [node_id]);
  console.log(`[FUNIS] remover-node node_id=${node_id}`);
  res.redirect(`/admin/funis/${slug}?ok=node-rem`);
});

app.post("/admin/funis/add-edge", async (req, res) => {
  const { slug, chain_steps_json } = req.body;
  if (!slug) return res.redirect("/admin");

  let steps = [];
  try {
    steps = JSON.parse(chain_steps_json || "[]");
  } catch (e) {}

  if (!steps.length && req.body.chain_ids) {
    let chain = req.body.chain_ids;
    if (!Array.isArray(chain)) chain = chain ? [chain] : [];
    steps = chain.map(id => [String(id)]).filter(arr => arr[0]);
  }

  if (steps.length < 2) return res.redirect(`/admin/funis/${slug}?erro=sem-etapas`);

  for (let i = 0; i < steps.length - 1; i++) {
    const fromNodes = steps[i];
    const toNodes = steps[i + 1];
    for (const from of fromNodes) {
      for (const to of toNodes) {
        if (from === to) continue;
        const { rows: exist } = await query(
          `SELECT id FROM funnel_edges WHERE from_node_id=$1 AND to_node_id=$2 LIMIT 1`,
          [from, to]
        );
        if (!exist.length) {
          await query(`INSERT INTO funnel_edges (from_node_id, to_node_id) VALUES ($1,$2)`, [from, to]);
          console.log(`[FUNIS] add-edge ${from} -> ${to}`);
        }
      }
    }
  }
  res.redirect(`/admin/funis/${slug}?ok=edge-add`);
});

app.post("/admin/funis/remover-edge", async (req, res) => {
  const { edge_id, slug } = req.body;
  if (!edge_id) return res.redirect("/admin");
  await query(`DELETE FROM funnel_edges WHERE id=$1`, [edge_id]);
  console.log(`[FUNIS] remover-edge edge_id=${edge_id}`);
  res.redirect(`/admin/funis/${slug}?ok=edge-rem`);
});

app.post("/admin/funis/remover-caminho", async (req, res) => {
  const { slug, funil_node_ids } = req.body;
  let ids = [];
  try {
    ids = JSON.parse(funil_node_ids || "[]");
  } catch (e) {}

  if (!ids.length && req.body.chain_ids) {
    let chain = req.body.chain_ids;
    if (!Array.isArray(chain)) chain = chain ? [chain] : [];
    ids = chain.map(String).filter(Boolean);
  }

  if (!slug || ids.length < 2) return res.redirect(`/admin/funis/${slug || ""}`);

  for (const from of ids) {
    for (const to of ids) {
      if (from === to) continue;
      await query(`DELETE FROM funnel_edges WHERE from_node_id=$1 AND to_node_id=$2`, [from, to]);
    }
  }
  console.log(`[FUNIS] remover-caminho funil unificado:`, ids);
  res.redirect(`/admin/funis/${slug}?ok=edge-rem`);
});

app.post("/api/funis/salvar-node", async (req, res) => {
  const { slug, tipo, rotulo, url: urlRaw, checkout_url: checkoutRaw } = req.body;
  if (!slug || !tipo || !rotulo || !urlRaw) {
    return res.status(400).json({ error: "Missing required fields" });
  }
  const url = normalizeUrl(urlRaw);
  const checkout_url = checkoutRaw ? normalizeUrl(checkoutRaw) : checkoutRaw;

  try {
    // 1. Resolve ou cria o nó da Landing Page
    let landingNodeId;
    const { rows: existingLanding } = await query(
      "SELECT id FROM funnel_nodes WHERE slug = $1 AND url = $2 LIMIT 1",
      [slug, url]
    );

    if (existingLanding.length > 0) {
      landingNodeId = existingLanding[0].id;
      // Atualiza o tipo e rótulo caso tenham mudado
      await query(
        "UPDATE funnel_nodes SET tipo = $1, rotulo = $2 WHERE id = $3",
        [tipo, rotulo, landingNodeId]
      );
    } else {
      const { rows: newLanding } = await query(
        "INSERT INTO funnel_nodes (slug, tipo, rotulo, url) VALUES ($1, $2, $3, $4) RETURNING id",
        [slug, tipo, rotulo, url]
      );
      landingNodeId = newLanding[0].id;
    }

    // 2. Se houver checkout preenchido, resolve o nó do checkout e conecta
    if (checkout_url && checkout_url.trim()) {
      let checkoutNodeId;
      const cleanCheckout = checkout_url.trim();

      const { rows: existingCheckout } = await query(
        "SELECT id FROM funnel_nodes WHERE slug = $1 AND url = $2 LIMIT 1",
        [slug, cleanCheckout]
      );

      if (existingCheckout.length > 0) {
        checkoutNodeId = existingCheckout[0].id;
      } else {
        const { rows: newCheckout } = await query(
          "INSERT INTO funnel_nodes (slug, tipo, rotulo, url) VALUES ($1, 'checkout', 'Checkout', $2) RETURNING id",
          [slug, cleanCheckout]
        );
        checkoutNodeId = newCheckout[0].id;
      }

      // 3. Cria a conexão (edge) entre a Landing Page e o Checkout se não existir
      const { rows: existingEdge } = await query(
        "SELECT id FROM funnel_edges WHERE from_node_id = $1 AND to_node_id = $2 LIMIT 1",
        [landingNodeId, checkoutNodeId]
      );

      if (existingEdge.length === 0) {
        await query(
          "INSERT INTO funnel_edges (from_node_id, to_node_id) VALUES ($1, $2)",
          [landingNodeId, checkoutNodeId]
        );
        console.log(`[FUNIL] Conectou nó ${landingNodeId} ao checkout ${checkoutNodeId}`);
      }
    }

    res.json({ success: true, landingNodeId });
  } catch (err) {
    console.error("[API] Error saving funnel node:", err.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Página de visão geral — mapa de funis de todos os players
app.get("/funis", async (_req, res) => {
  const { rows: pages } = await query(
    "SELECT slug, nome, url, tipo FROM pages ORDER BY created_at DESC"
  );

  const { rows: allNodes } = await query(`SELECT id, slug, tipo, rotulo, url FROM funnel_nodes`);
  const { rows: allEdges } = await query(`SELECT id, from_node_id, to_node_id FROM funnel_edges`);

  const nodesBySlug = {};
  allNodes.forEach(n => { (nodesBySlug[n.slug] ||= []).push(n); });

  const nodeIdToSlug = {};
  allNodes.forEach(n => { nodeIdToSlug[n.id] = n.slug; });
  const edgesBySlug = {};
  allEdges.forEach(e => {
    const s = nodeIdToSlug[e.from_node_id];
    if (s) (edgesBySlug[s] ||= []).push(e);
  });

  const comMapa = [];
  const semMapa = [];
  for (const p of pages) {
    const nodes = nodesBySlug[p.slug] || [];
    if (nodes.length === 0) { semMapa.push(p); continue; }
    const edges = edgesBySlug[p.slug] || [];
    const caminhos = computarCaminhos(nodes, edges);
    comMapa.push({ ...p, caminhos });
  }

  const cardsHtml = comMapa.map(p => `
    <div class="player-card">
      <div class="player-hdr">
        <span class="player-tipo-badge ${p.tipo === "dominio" ? "b-dom" : "b-pag"}">${p.tipo === "dominio" ? "🌐" : "📡"}</span>
        <a href="${p.url}" target="_blank" rel="noopener" class="player-nome">${p.nome}</a>
        <a href="/admin/funis/${p.slug}" class="player-edit-link">✏️ Editar</a>
      </div>
      <div class="player-caminhos">
        ${p.caminhos.map((c, idx) => renderCaminho(c, idx, false)).join("")}
      </div>
    </div>`).join("");

  const semMapaHtml = semMapa.length ? `
    <div class="card" style="margin-top:8px">
      <h2>💤 Sem funil mapeado ainda (${semMapa.length})</h2>
      <div class="sem-mapa-list">
        ${semMapa.map(p => `<a href="/admin/funis/${p.slug}" class="sem-mapa-item">${p.tipo === "dominio" ? "🌐" : "📡"} ${p.nome}</a>`).join("")}
      </div>
    </div>` : "";

  res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mapa de Funis — Lowticket Monitor</title>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:1100px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.hdr h1{font-size:19px;font-weight:700}
.hdr-sub{font-size:12px;color:var(--muted);margin-top:3px}
.hdr-nav{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
.hdr-nav a{font-size:12px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:6px 14px;border-radius:8px;white-space:nowrap}
.hdr-nav a:hover{background:var(--accent);color:#fff}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:20px 22px;margin-bottom:18px}
.card h2{font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:14px}
.player-card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:18px 20px;margin-bottom:14px}
.player-hdr{display:flex;align-items:center;gap:10px;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border)}
.player-tipo-badge{font-size:15px}
.player-nome{font-size:15px;font-weight:700;color:#fff;text-decoration:none}
.player-nome:hover{color:var(--accent)}
.player-edit-link{margin-left:auto;font-size:11px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:4px 10px;border-radius:6px;white-space:nowrap}
.player-edit-link:hover{background:var(--accent);color:#fff}
.player-caminhos{display:flex;flex-direction:column;gap:8px}
.caminho-row{display:flex;align-items:center;flex-wrap:wrap;gap:2px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:10px 12px}
.flow-tree-node{display:inline-flex;align-items:center}
.flow-linear-branch{display:inline-flex;align-items:center}
.flow-tree-branches{display:flex;flex-direction:column;justify-content:center;gap:12px;position:relative;margin-left:8px;padding-left:18px}
.flow-tree-branches::before{content:'';position:absolute;left:0;top:16px;bottom:16px;width:2px;background:rgba(167,139,250,0.45);border-radius:2px}
.flow-tree-branch{display:inline-flex;align-items:center;position:relative}
.flow-tree-branch::before{content:'';position:absolute;left:-18px;top:50%;width:12px;height:2px;background:rgba(167,139,250,0.45)}
.flow-branch-content{display:inline-flex;align-items:center}
.flow-row-container{min-height:54px}
.chip{display:inline-flex;align-items:center;gap:5px;background:var(--surface);border:1px solid var(--border);border-radius:7px;padding:5px 10px;text-decoration:none;font-size:12px;font-weight:600;color:#fff}
.chip:hover{border-color:var(--accent)}
.chip-icon{font-size:13px}
.chip-arrow{color:var(--muted);font-size:15px;font-weight:700;margin:0 2px}
.empty-state{color:var(--muted);font-size:13px;text-align:center;padding:32px;border:1px dashed var(--border);border-radius:12px}
.sem-mapa-list{display:flex;flex-wrap:wrap;gap:8px}
.sem-mapa-item{font-size:12px;color:var(--muted);text-decoration:none;background:#0f0f1e;border:1px solid var(--border);border-radius:7px;padding:6px 12px}
.sem-mapa-item:hover{color:var(--accent);border-color:var(--accent)}
@media(max-width:640px){.hdr-nav{width:100%}.hdr-nav a{flex:1;text-align:center}}
</style>
</head>
<body>
<div class="hdr">
  <div>
    <h1>🔀 Mapa de Funis</h1>
    <div class="hdr-sub">Visão geral de todos os caminhos mapeados por biblioteca/domínio</div>
  </div>
  <div class="hdr-nav">
    <a href="/admin">⚙️ Admin</a>
    <a href="/dashboard">📊 Dashboard</a>
  </div>
</div>

${comMapa.length === 0
  ? '<div class="empty-state">Nenhum funil mapeado ainda. Vá em Admin → 🔀 Funis num player para começar.</div>'
  : cardsHtml}

${semMapaHtml}

</body>
</html>`);
});

// ─── Dashboard ───────────────────────────────────────────────────────────────

// SVG da logo do Instagram (inline, sem dependência externa)
const IG_SVG = `<svg width="18" height="18" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="ig" x1="0" y1="24" x2="24" y2="0"><stop offset="0%" stop-color="#f09433"/><stop offset="25%" stop-color="#e6683c"/><stop offset="50%" stop-color="#dc2743"/><stop offset="75%" stop-color="#cc2366"/><stop offset="100%" stop-color="#bc1888"/></linearGradient></defs><rect width="24" height="24" rx="6" fill="url(#ig)"/><rect x="3" y="3" width="18" height="18" rx="5" stroke="white" stroke-width="1.8" fill="none"/><circle cx="12" cy="12" r="4.5" stroke="white" stroke-width="1.8" fill="none"/><circle cx="17.5" cy="6.5" r="1.2" fill="white"/></svg>`;

app.get("/dashboard", async (_req, res) => {
  try {
    const { rows: allPages } = await query(
      "SELECT slug, nome, url, tipo, created_at, inicial_count, instagram_url, geo, nicho, funil, last_attempt_at, last_status FROM pages"
    );

    const BR_OFFSET_MS = 3 * 60 * 60 * 1000;
    function toBrDate(utcNaiveTimestamp) {
      return new Date(new Date(utcNaiveTimestamp).getTime() - BR_OFFSET_MS);
    }

    async function processarGrupo(pagesDoGrupo) {
      const ultimaLeitura = {};
      const primeiraData = {};
      const paginas = {};
      const mon = {};
      const meta = {}; // instagram_url, geo, nicho, funil por nome

      for (const p of pagesDoGrupo) {
        const { rows: hist } = await query(
          `SELECT ads_count, slot, collected_at
           FROM scrape_history WHERE slug=$1 ORDER BY collected_at ASC`,
          [p.slug]
        );

        const { rows: latest } = await query(
          `SELECT ads_count, collected_at FROM scrape_latest WHERE slug = $1 LIMIT 1`,
          [p.slug]
        );

        const latestRow = latest[0];
        const temDado = hist.length > 0 || !!latestRow;
        if (!temDado) continue;

        ultimaLeitura[p.nome] = {
          ads:          latestRow ? latestRow.ads_count : hist[hist.length - 1].ads_count,
          url:          p.url,
          ultimaColeta: latestRow
            ? new Date(latestRow.collected_at).toISOString()
            : (hist.length ? new Date(hist[hist.length - 1].collected_at).toISOString() : null),
          tentativa:    p.last_attempt_at ? new Date(p.last_attempt_at).toISOString() : null,
          status:       p.last_status || null,
        };

        primeiraData[p.nome] = toBrDate(p.created_at).toISOString().slice(0, 10);

        mon[p.nome] = {
          ini: p.inicial_count ?? (hist.length ? hist[0].ads_count : (latestRow ? latestRow.ads_count : 0))
        };

        // Metadados para a dashboard
        meta[p.nome] = {
          instagram_url: p.instagram_url || null,
          geo:           p.geo || null,
          nicho:         p.nicho || null,
          funil:         p.funil || null,
        };

        paginas[p.nome] = {};
        for (const h of hist) {
          const brDt = toBrDate(h.collected_at);
          const dk = brDt.toISOString().slice(0, 10);
          const slot = (h.slot !== null && h.slot !== undefined)
            ? Number(h.slot)
            : [3, 12, 22].reduce((b, s) => Math.abs(brDt.getUTCHours() - s) < Math.abs(brDt.getUTCHours() - b) ? s : b, 3);
          if (!paginas[p.nome][dk]) paginas[p.nome][dk] = {};
          paginas[p.nome][dk][slot] = h.ads_count;
        }
      }

      const slugs = pagesDoGrupo.map(p => p.slug);
      let histMap = {}, histDates = [];
      if (slugs.length) {
        const { rows: histAll } = await query(`
          SELECT p.nome, sh.ads_count, sh.slot, sh.collected_at
          FROM scrape_history sh
          JOIN pages p ON p.slug = sh.slug
          WHERE sh.slug = ANY($1) AND sh.collected_at >= NOW() - INTERVAL '60 days'
          ORDER BY sh.collected_at DESC
        `, [slugs]);
        for (const r of histAll) {
          const nome = r.nome;
          const brDt = toBrDate(r.collected_at);
          const dk = brDt.toISOString().slice(0, 10);
          const slot = (r.slot !== null && r.slot !== undefined)
            ? Number(r.slot)
            : [3, 12, 22].reduce((b, s) => Math.abs(brDt.getUTCHours() - s) < Math.abs(brDt.getUTCHours() - b) ? s : b, 3);
          if (!histMap[nome]) histMap[nome] = {};
          if (!histMap[nome][dk]) histMap[nome][dk] = {};
          if (histMap[nome][dk][slot] === undefined) histMap[nome][dk][slot] = r.ads_count;
        }
        histDates = [...new Set(histAll.map(r => toBrDate(r.collected_at).toISOString().slice(0, 10)))]
          .sort((a, b) => b.localeCompare(a));
      }
      const histLibs = Object.keys(paginas).sort((a, b) => (ultimaLeitura[b]?.ads || 0) - (ultimaLeitura[a]?.ads || 0));

      return {
        geral: { pags: paginas, ultima: ultimaLeitura, primeira: primeiraData, mon, meta },
        hist:  { map: histMap, dates: histDates, libs: histLibs },
        count: Object.keys(paginas).length,
      };
    }

    const grupoPaginas  = await processarGrupo(allPages.filter(p => p.tipo !== "dominio"));
    const grupoDominios = await processarGrupo(allPages.filter(p => p.tipo === "dominio"));

    const dados       = JSON.stringify(grupoPaginas.geral);
    const histDados   = JSON.stringify(grupoPaginas.hist);
    const dadosDom    = JSON.stringify(grupoDominios.geral);
    const histDadosDom = JSON.stringify(grupoDominios.hist);

    // "📢 Mapeamento ADS": lê todo nó tipo='ads', conectado ou não, agrupado por página.
    // Não depende de computarCaminhos() (que exige componente com 2+ nós) — um ADS
    // cadastrado sozinho, sem conexão nenhuma, já aparece aqui.
    const { rows: adsRows } = await query(`
      SELECT fn.id, fn.rotulo, fn.url AS ad_url, p.slug, p.nome, p.tipo
      FROM funnel_nodes fn
      JOIN pages p ON p.slug = fn.slug
      WHERE fn.tipo = 'ads'
      ORDER BY p.nome, fn.created_at ASC
    `);

    const adsBySlug = {};
    adsRows.forEach(r => {
      (adsBySlug[r.slug] ||= { nome: r.nome, tipo: r.tipo, itens: [] })
        .itens.push({ id: r.id, rotulo: r.rotulo, url: r.ad_url });
    });
    const adsPaginas = Object.values(adsBySlug).sort((a, b) => a.nome.localeCompare(b.nome));

    const adsCardsHtml = adsPaginas.map(pg => `
      <div class="player-card">
        <div class="player-hdr">
          <span class="player-tipo-badge ${pg.tipo === "dominio" ? "b-dom" : "b-pag"}">${pg.tipo === "dominio" ? "🌐" : "📡"}</span>
          <span class="player-nome">${pg.nome}</span>
          <span class="ads-count-badge">📢 ${pg.itens.length} ADS</span>
        </div>
        <div class="ads-chip-row">
          ${pg.itens.map(a => `<a href="${a.url}" target="_blank" rel="noopener" class="chip" title="${a.rotulo}"><span class="chip-icon">📢</span><span class="chip-label">${a.rotulo}</span></a>`).join("")}
        </div>
      </div>
    `).join("");

    // Serializa o SVG do Instagram para uso seguro dentro do template literal JS
    const IG_SVG_ESC = IG_SVG.replace(/`/g, "\\`").replace(/\$/g, "\\$");

    res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lowticket Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"><\/script>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--surface2:#171728;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--up2:#10b981;--down:#fb7185;--flat:#8888aa;--hot:#a78bfa}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',system-ui,sans-serif;padding:18px;max-width:1600px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.hdr h1{font-size:19px;font-weight:700;color:#fff;letter-spacing:.2px}
.hdr-sub{font-size:12px;color:var(--text2);margin-top:3px;font-family:'Space Mono',monospace}
.hdr-live{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:12px;color:var(--text2);background:var(--surface);border:1px solid var(--border);padding:7px 14px;border-radius:8px}
.hdr-admin-btn{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:6px 14px;border-radius:8px;transition:all .15s;white-space:nowrap}
.hdr-admin-btn:hover{background:var(--accent);color:#fff}
.dot{width:8px;height:8px;border-radius:50%;background:var(--up);box-shadow:0 0 8px var(--up)}
.section-label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin:0 0 12px 2px;display:flex;align-items:center;gap:8px}
.scaling-strip{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;margin-bottom:26px}
.scale-card{background:linear-gradient(135deg,#1a1530,#14122a);border:1px solid #2e2658;border-radius:14px;padding:16px 18px;position:relative;overflow:hidden;cursor:pointer;transition:all 0.25s cubic-bezier(0.16,1,0.3,1)}
.scale-card:hover{border-color:var(--accent);box-shadow:0 8px 24px -6px rgba(124,111,255,0.35);transform:translateY(-2px)}
.scale-card:active{transform:scale(0.982)}
.scale-arrow{display:inline-block;transition:transform 0.25s ease, color 0.25s ease;font-weight:700;margin-left:4px}
.scale-card:hover .scale-arrow{transform:translate(2px,-2px);color:var(--accent)}
.scale-card-toast{position:absolute;inset:0;background:rgba(18,18,31,0.92);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);display:flex;align-items:center;justify-content:center;gap:8px;color:#34d399;font-weight:700;font-size:14px;border-radius:14px;opacity:0;transform:scale(0.92);pointer-events:none;transition:all 0.25s cubic-bezier(0.16,1,0.3,1);z-index:10}
.scale-card-toast.show{opacity:1;transform:scale(1)}
.scale-card-rank{position:absolute;top:12px;right:14px;font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;display:flex;align-items:center}
.scale-card-name{font-size:13px;font-weight:600;color:#fff;margin-bottom:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:34px}
.scale-card-val{font-size:30px;font-weight:700;color:#fff;font-family:'Space Mono',monospace;line-height:1}
.scale-card-meta{display:flex;align-items:center;gap:10px;margin-top:8px;font-size:12px}
.scale-pct{font-weight:600;font-family:'Space Mono',monospace}
.scale-spark{margin-top:10px;height:32px;position:relative}
.empty-hint{background:var(--surface);border:1px dashed var(--border);border-radius:12px;padding:22px;text-align:center;color:var(--muted);font-size:13px;margin-bottom:26px}
.lib-link{color:var(--text);text-decoration:none;border-bottom:1px solid var(--border);padding-bottom:1px;transition:color .15s,border-color .15s}
.lib-link:hover{color:var(--accent);border-color:var(--accent)}
.accordion-wrap{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.accordion-btn{width:100%;display:flex;align-items:center;gap:10px;padding:14px 18px;background:transparent;border:none;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:13px;font-weight:600;cursor:pointer;text-align:left;transition:background .15s}
.accordion-btn:hover{background:var(--surface2)}
.acc-meta{font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;margin-left:4px}
.acc-icon{margin-left:auto;font-size:12px;color:var(--muted);transition:transform .25s;flex-shrink:0}
.acc-icon.open{transform:rotate(180deg)}
.accordion-body{display:none;padding:0 18px 16px;overflow-x:auto;-webkit-overflow-scrolling:touch}
.accordion-body.open{display:block}
.grid-charts{display:grid;grid-template-columns:380px 1fr;gap:14px;margin-bottom:26px}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:16px 18px}
.panel-title{font-size:12px;font-weight:600;color:var(--text2);text-transform:uppercase;letter-spacing:.6px;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.rosca-wrap{display:flex;gap:16px;align-items:center}
.rosca-canvas{width:150px;height:150px;position:relative;flex-shrink:0}
.legend{display:flex;flex-direction:column;gap:7px;flex:1;min-width:0}
.leg-item{display:flex;align-items:center;gap:9px}
.leg-dot{width:10px;height:10px;border-radius:3px;flex-shrink:0}
.leg-name{font-size:12px;color:var(--text);flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.leg-val{font-size:12px;font-weight:600;color:#fff;font-family:'Space Mono',monospace}
.leg-pct{font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;width:32px;text-align:right}
.chart-box{height:240px;position:relative}
.hist-box{height:300px;position:relative}
.tbl-panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:var(--surface2);color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.6px;padding:11px 16px;text-align:left;font-weight:600}
td{padding:11px 16px;border-top:1px solid var(--border);color:var(--text2);white-space:nowrap}
tbody tr:hover td{background:var(--surface2)}
.t-name{font-weight:600;color:#fff;white-space:normal}
.t-name-main{display:block;font-weight:600;color:#fff;margin-bottom:4px}
.t-meta-badges{display:flex;flex-wrap:wrap;gap:4px;margin-top:3px}
.t-geo-badge{font-size:10px;background:rgba(34,211,238,.1);color:#22d3ee;padding:2px 7px;border-radius:5px;font-weight:500;white-space:nowrap}
.t-nicho-badge{font-size:10px;background:rgba(167,139,250,.12);color:#a78bfa;padding:2px 7px;border-radius:5px;font-weight:500;white-space:nowrap}
.t-funil-badge{font-size:10px;background:rgba(251,191,36,.12);color:#fbbf24;padding:2px 7px;border-radius:5px;font-weight:500;white-space:nowrap}
.badge{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:7px;font-size:11px;font-weight:600;font-family:'Space Grotesk'}
.b-up{background:rgba(52,211,153,.13);color:#34d399}
.b-hot{background:rgba(167,139,250,.15);color:#a78bfa}
.b-down{background:rgba(251,113,133,.13);color:#fb7185}
.b-flat{background:rgba(136,136,170,.12);color:#9999b8}
.b-off{background:rgba(120,120,140,.1);color:#777}
.scalebar-bg{width:80px;height:5px;background:var(--border);border-radius:3px;display:inline-block;vertical-align:middle;margin-right:8px}
.scalebar{height:5px;border-radius:3px;display:block}
.spark3{font-family:'Space Mono',monospace;font-size:13px}
.ig-cell{text-align:center}
.ig-link{display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;border-radius:8px;transition:background .15s}
.ig-link:hover{background:rgba(220,39,67,.15)}
.ig-none{color:var(--muted);font-size:13px}
.mono{font-family:'Space Mono',monospace}
.hist-tbl thead th{white-space:nowrap}
.hist-tbl td{font-family:'Space Mono',monospace;font-size:12px;text-align:center}
.hist-tbl td.lib-name{text-align:left;font-family:'Space Grotesk',sans-serif;font-weight:600;color:#fff;white-space:nowrap}
.hist-tbl td.date-col{color:var(--muted);text-align:left;white-space:nowrap}
.hist-slot{display:inline-block;min-width:42px;text-align:right}
.hist-slot.empty{color:var(--border)}
.tbl-scroll-x{overflow-x:auto;-webkit-overflow-scrolling:touch}
.group-title{font-size:15px;font-weight:700;color:#fff;letter-spacing:.4px;margin:0 0 16px 2px;padding-bottom:10px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px}
.player-card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:18px 20px;margin-bottom:14px}
.player-hdr{display:flex;align-items:center;gap:10px;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border)}
.player-tipo-badge{font-size:15px;padding:2px 6px;border-radius:6px}
.player-nome{font-size:15px;font-weight:700;color:#fff;text-decoration:none}
.b-dom{background:rgba(124,111,255,.15);color:#a78bfa}
.b-pag{background:rgba(52,211,153,.12);color:#34d399}
.chip{display:inline-flex;align-items:center;gap:5px;background:var(--surface2);border:1px solid var(--border);border-radius:7px;padding:5px 10px;text-decoration:none;font-size:12px;font-weight:600;color:#fff}
.chip:hover{border-color:var(--accent)}
.chip-icon{font-size:13px}
.chip-label{white-space:nowrap}
.ads-count-badge{margin-left:auto;font-size:11px;font-weight:600;color:#a78bfa;background:rgba(167,139,250,.12);padding:3px 10px;border-radius:7px;white-space:nowrap}
.ads-chip-row{display:flex;flex-wrap:wrap;gap:8px}
@media(max-width:1100px){.grid-charts{grid-template-columns:1fr}.rosca-wrap{flex-direction:column}}
@media(max-width:768px){
  body{padding:12px}
  .hdr{flex-wrap:wrap;gap:8px}
  .hdr-live{margin-left:0;width:100%}
  .hdr-admin-btn{width:100%;justify-content:center;box-sizing:border-box}
  .hdr h1{font-size:15px}
  .hdr-sub{font-size:10px}
  .scaling-strip{grid-template-columns:1fr 1fr}
  .scale-card-val{font-size:24px}
  .grid-charts{grid-template-columns:1fr}
  .rosca-wrap{flex-direction:column;align-items:flex-start}
  .rosca-canvas{width:120px;height:120px}
  .hist-box{height:220px}
  .tbl-panel table thead{display:none}
  .tbl-panel table tbody tr{display:block;background:var(--surface2);border:1px solid var(--border);border-radius:10px;margin-bottom:10px;padding:12px 14px}
  .tbl-panel table tbody td{display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-top:none;white-space:normal;font-size:12px}
  .tbl-panel table tbody td::before{content:attr(data-label);font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-right:10px;flex-shrink:0}
  .tbl-panel table tbody td.t-name{font-size:14px;font-weight:700;color:#fff;border-bottom:1px solid var(--border);padding-bottom:8px;margin-bottom:4px}
  .tbl-panel table tbody td.t-name::before{display:none}
  .scalebar-bg{width:50px}
}
@media(max-width:480px){.scaling-strip{grid-template-columns:1fr}}
</style>
</head>
<body>

<div class="hdr">
  <div>
    <h1>📊 Lowticket Monitor</h1>
    <div class="hdr-sub" id="upd"></div>
  </div>
  <div style="margin-left:auto;display:flex;flex-direction:column;align-items:flex-end;gap:8px">
    <div class="hdr-live" style="margin-left:0"><span class="dot"></span><span id="livecount"></span></div>
    <div style="display:flex;gap:8px">
      <a href="/admin" class="hdr-admin-btn">⚙️ Ir para Admin</a>
      <a href="/funis" class="hdr-admin-btn">🔀 Ver Mapa de Funis</a>
    </div>
  </div>
</div>

<div class="group-title">📡 BIBLIOTECAS — rastreio por página</div>

<div class="section-label">🚀 Escalando agora — bibliotecas em ascensão</div>
<div class="scaling-strip" id="pag_scaling"></div>
<div class="empty-hint" id="pag_scaling-empty" style="display:none">Nenhum item em ascensão no período. Conforme as coletas acumulam, o que cresce aparece aqui.</div>

<div class="grid-charts">
  <div class="panel">
    <div class="panel-title">🍩 Distribuição atual</div>
    <div class="rosca-wrap">
      <div class="rosca-canvas"><canvas id="pag_cRosca"></canvas></div>
      <div class="legend" id="pag_legend"></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-title">📈 Evolução histórica — média diária <span style="color:var(--down);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px">● dia de descoberta</span></div>
    <div class="hist-box"><canvas id="pag_cHist"></canvas></div>
  </div>
</div>

<div class="section-label">📋 Resumo completo</div>
<div class="tbl-panel">
  <table>
    <thead><tr>
      <th>#</th><th>Bibliotecas</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Última Checagem</th>
      <th>Δ Total</th><th>Tendência</th><th>Participação</th><th>3 dias</th><th>Instagram</th>
    </tr></thead>
    <tbody id="pag_tbody"></tbody>
  </table>
</div>

<div style="height:36px"></div>

<div class="group-title">🌐 DOMÍNIOS — rastreio por URL</div>

<div class="section-label">🚀 Escalando agora — domínios em ascensão</div>
<div class="scaling-strip" id="dom_scaling"></div>
<div class="empty-hint" id="dom_scaling-empty" style="display:none">Nenhum item em ascensão no período. Conforme as coletas acumulam, o que cresce aparece aqui.</div>

<div class="grid-charts">
  <div class="panel">
    <div class="panel-title">🍩 Distribuição atual</div>
    <div class="rosca-wrap">
      <div class="rosca-canvas"><canvas id="dom_cRosca"></canvas></div>
      <div class="legend" id="dom_legend"></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-title">📈 Evolução histórica — média diária <span style="color:var(--down);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px">● dia de descoberta</span></div>
    <div class="hist-box"><canvas id="dom_cHist"></canvas></div>
  </div>
</div>

<div class="section-label">📋 Resumo completo</div>
<div class="tbl-panel">
  <table>
    <thead><tr>
      <th>#</th><th>Domínios</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Última Checagem</th>
      <th>Δ Total</th><th>Tendência</th><th>Participação</th><th>3 dias</th><th>Instagram</th>
    </tr></thead>
    <tbody id="dom_tbody"></tbody>
  </table>
</div>

<div style="height:36px"></div>

<div class="group-title">🔀 Mapeamento de Funis</div>
<div class="empty-hint" style="display:flex;align-items:center;justify-content:center;gap:14px">
  <span>O mapeamento completo de funis agora tem uma página dedicada.</span>
  <a href="/funis" style="font-size:13px;font-weight:600;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:8px 18px;border-radius:8px;white-space:nowrap">🔀 Abrir Mapa de Funis</a>
</div>

<div style="height:36px"></div>

<div class="group-title">📢 Mapeamento ADS</div>
${adsPaginas.length === 0
  ? '<div class="empty-hint">Nenhum ADS mapeado ainda. Cadastre em Admin → 🔀 Funis → Nova Etapa (tipo ADS) — não precisa conectar a nada.</div>'
  : `<div class="player-caminhos" style="margin-bottom:26px">${adsCardsHtml}</div>`}

<div style="height:36px"></div>

<div class="group-title">📅 Histórico de Coletas</div>


<div class="accordion-wrap">
  <button class="accordion-btn" onclick="toggleAccordion('pag_hist-section','pag_acc-icon')">
    <span>📡 Histórico de Coletas — Bibliotecas · 03h · 12h · 22h</span>
    <span class="acc-meta" id="pag_acc-meta"></span>
    <span class="acc-icon" id="pag_acc-icon">▼</span>
  </button>
  <div class="accordion-body" id="pag_hist-section">Carregando histórico...</div>
</div>

<div class="accordion-wrap" style="margin-top:12px">
  <button class="accordion-btn" onclick="toggleAccordion('dom_hist-section','dom_acc-icon')">
    <span>🌐 Histórico de Coletas — Domínios · 03h · 12h · 22h</span>
    <span class="acc-meta" id="dom_acc-meta"></span>
    <span class="acc-icon" id="dom_acc-icon">▼</span>
  </button>
  <div class="accordion-body" id="dom_hist-section">Carregando histórico...</div>
</div>

<script>
const IG_SVG=\`${IG_SVG_ESC}\`;

document.getElementById("upd").textContent="Atualizado "+new Date().toLocaleString("pt-BR")+"  ·  coletas 03h · 12h · 22h";

function toggleAccordion(bodyId,iconId){
  const body=document.getElementById(bodyId);
  const icon=document.getElementById(iconId);
  body.classList.toggle('open');
  icon.classList.toggle('open');
}

function render(D,HD,P){
const COR=["#7c6fff","#34d399","#fb7185","#fbbf24","#22d3ee","#a78bfa","#f97316","#4ade80","#ec4899","#38bdf8","#facc15","#2dd4bf","#fb923c","#a3e635","#e879f9","#60a5fa"];
function med(s){const v=Object.values(s).filter(x=>!isNaN(x));return v.length?Math.round(v.reduce((a,b)=>a+b,0)/v.length):null}
function fd(dk){const[y,m,d]=dk.split("-");return d+"/"+m}
function fdFull(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y}
const{pags,ultima,primeira,mon,meta}=D;
const LP=Object.keys(pags).sort();
const dSet=new Set();LP.forEach(p=>Object.keys(pags[p]).forEach(d=>dSet.add(d)));
const datas=Array.from(dSet).sort();

function serie(pag){return datas.map(dk=>pags[pag]?.[dk]?med(pags[pag][dk]):null)}
function slope(pag){
  const s=serie(pag).filter(v=>v!==null);
  if(s.length<2)return 0;
  const recent=s.slice(-3);
  return recent[recent.length-1]-recent[0];
}
function info(pag){
  const at=ultima[pag]?.ads??0;
  const ini=mon[pag]?.ini??at;
  const vn=at-ini;
  const pct=ini>0?Math.round(((at-ini)/ini)*100):0;
  const sl=slope(pag);
  let cls,label,color;
  if(at===0){cls="b-off";label="Inativo";color=getComputedStyle(document.documentElement).getPropertyValue('--muted')}
  else if(pct>50){cls="b-hot";label="Escalando forte";color="#a78bfa"}
  else if(sl>0&&pct>5){cls="b-up";label="Crescendo";color="#34d399"}
  else if(sl<0&&pct<-5){cls="b-down";label="Cortando";color="#fb7185"}
  else if(vn===0){cls="b-flat";label="Estável";color="#9999b8"}
  else if(vn>0){cls="b-up";label="Subindo";color="#34d399"}
  else{cls="b-down";label="Caindo";color="#fb7185"}
  return{at,ini,vn,pct,sl,cls,label,color};
}

const porAds=[...LP].sort((a,b)=>(ultima[b]?.ads||0)-(ultima[a]?.ads||0));
const maxAds=ultima[porAds[0]]?.ads||1;

const escalando=LP.map(p=>({p,...info(p)}))
  .filter(x=>x.at>0&&(x.label==="Escalando forte"||x.label==="Crescendo"||x.label==="Subindo")&&x.pct>0)
  .sort((a,b)=>b.pct-a.pct);

const strip=document.getElementById(P+"scaling");
if(escalando.length===0){
  document.getElementById(P+"scaling-empty").style.display="block";
}else{
  escalando.forEach((x,i)=>{
    const card=document.createElement("div");
    card.className="scale-card";
    const urlBib = ultima[x.p]?.url || "";
    card.title = "Clique para copiar o link e abrir a biblioteca de " + x.p;
    card.onclick = function() {
      if (urlBib) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(urlBib).catch(()=>{});
        } else {
          const ta = document.createElement("textarea");
          ta.value = urlBib;
          document.body.appendChild(ta);
          ta.select();
          try { document.execCommand("copy"); } catch(e){}
          document.body.removeChild(ta);
        }
        window.open(urlBib, "_blank", "noopener");
      }
      let oldToast = card.querySelector(".scale-card-toast");
      if (oldToast) oldToast.remove();
      const toast = document.createElement("div");
      toast.className = "scale-card-toast";
      toast.innerHTML = '<span style="font-size:16px">✓</span> Link copiado!';
      card.appendChild(toast);
      void toast.offsetWidth;
      toast.classList.add("show");
      setTimeout(()=>{
        toast.classList.remove("show");
        setTimeout(()=>{ toast.remove(); }, 300);
      }, 2000);
    };
    card.innerHTML='<div class="scale-card-rank">#'+(i+1)+' <span class="scale-arrow">↗</span></div>'
      +'<div class="scale-card-name">'+x.p+'</div>'
      +'<div class="scale-card-val">'+x.at.toLocaleString("pt-BR")+'</div>'
      +'<div class="scale-card-meta"><span class="scale-pct" style="color:'+x.color+'">'+(x.pct>=0?"+":"")+x.pct+'%</span>'
      +'<span style="color:var(--muted)">'+(x.vn>=0?"+":"")+x.vn+' ads desde início</span></div>'
      +'<div class="scale-spark"><canvas id="'+P+'spark'+i+'"></canvas></div>';
    strip.appendChild(card);
  });
}

const tbody=document.getElementById(P+"tbody");
porAds.forEach((pag,idx)=>{
  const x=info(pag);
  const did=primeira[pag]?fdFull(primeira[pag]):"—";
  const s=serie(pag).filter(v=>v!==null);
  const last3=s.slice(-3);
  const spark3=last3.length>=2?(last3[last3.length-1]>last3[0]?'<span style="color:#34d399">▲ sub</span>':last3[last3.length-1]<last3[0]?'<span style="color:#fb7185">▼ cai</span>':'<span style="color:#888">= est</span>'):"—";
  const partPct=Math.round((x.at/maxAds)*100);
  const corLib=COR[idx%COR.length];

  // Monta célula do nome com badges de geo e nicho
  const m=meta?.[pag]||{};
  const geoBadge=m.geo?'<span class="t-geo-badge">🌍 '+m.geo+'</span>':'';
  const nichoBadge=m.nicho?'<span class="t-nicho-badge">🏷️ '+m.nicho+'</span>':'';
  const funilBadge=m.funil?'<span class="t-funil-badge">🎯 '+m.funil+'</span>':'';
  const metaBadgesHtml=(geoBadge||nichoBadge||funilBadge)?'<div class="t-meta-badges">'+geoBadge+nichoBadge+funilBadge+'</div>':'';
  const nomeCell='<span class="t-name-main"><a href="'+(ultima[pag]?.url||'#')+'" target="_blank" rel="noopener" class="lib-link">'+pag+'</a></span>'+metaBadgesHtml;

  // Célula do Instagram
  const igCell=m.instagram_url
    ?'<a href="'+m.instagram_url+'" target="_blank" rel="noopener" class="ig-link" title="Ver Instagram">'+IG_SVG+'</a>'
    :'<span class="ig-none">—</span>';

  const tr=document.createElement("tr");
  tr.innerHTML=
    '<td class="mono" data-label="#" style="color:var(--muted)">'+(idx+1)+'</td>'
    +'<td class="t-name" data-label="Nome">'+nomeCell+'</td>'
    +'<td data-label="Descoberta" style="color:var(--muted)">'+did+'</td>'
    +'<td class="mono" data-label="Inicial">'+x.ini+'</td>'
    +'<td class="mono" data-label="Atual" style="color:#fff;font-weight:600">'+x.at+'</td>'
    +'<td data-label="Últ. Checagem" style="color:var(--muted);font-family:Space Mono,monospace;font-size:11px">'
    +(ultima[pag]?.ultimaColeta?new Date(ultima[pag].ultimaColeta).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}):'—')
    +(ultima[pag]?.status==='falha_scraping'&&ultima[pag]?.tentativa?'<div style="color:#fb7185;font-size:10px">tentou '+new Date(ultima[pag].tentativa).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})+' · falhou</div>':'')
    +'</td>'
    +'<td class="mono" data-label="Δ Total" style="color:'+(x.vn>0?"#34d399":x.vn<0?"#fb7185":"#888")+'">'+(x.vn>=0?"+":"")+x.vn+'</td>'
    +'<td data-label="Tendência"><span class="badge '+x.cls+'">'+x.label+'</span></td>'
    +'<td data-label="Participação"><span class="scalebar-bg"><span class="scalebar" style="width:'+partPct+'%;background:'+corLib+'"></span></span><span class="mono" style="font-size:11px;color:var(--muted)">'+partPct+'%</span></td>'
    +'<td class="spark3" data-label="3 dias">'+spark3+'</td>'
    +'<td class="ig-cell" data-label="Instagram">'+igCell+'</td>';
  tbody.appendChild(tr);
});

const ro=porAds.filter(p=>(ultima[p]?.ads||0)>0);
const totalRo=ro.reduce((s,p)=>s+(ultima[p]?.ads||0),0);
const legend=document.getElementById(P+"legend");
ro.forEach((p,i)=>{
  const at=ultima[p]?.ads||0;
  const pct=totalRo>0?Math.round((at/totalRo)*100):0;
  const it=document.createElement("div");it.className="leg-item";
  it.innerHTML='<span class="leg-dot" style="background:'+COR[porAds.indexOf(p)%COR.length]+'"></span>'
    +'<span class="leg-name" title="'+p+'">'+p+'</span>'
    +'<span class="leg-val">'+at.toLocaleString("pt-BR")+'</span>'
    +'<span class="leg-pct">'+pct+'%</span>';
  legend.appendChild(it);
});

new Chart(document.getElementById(P+"cRosca"),{
  type:"doughnut",
  data:{labels:ro,datasets:[{data:ro.map(p=>ultima[p]?.ads||0),backgroundColor:ro.map(p=>COR[porAds.indexOf(p)%COR.length]),borderWidth:2,borderColor:"#12121f"}]},
  options:{responsive:true,maintainAspectRatio:false,cutout:"62%",plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>" "+ctx.label+": "+ctx.parsed.toLocaleString("pt-BR")+" ads"}}}}
});

const LP_hist=porAds.slice(0,8);
new Chart(document.getElementById(P+"cHist"),{
  type:"line",
  data:{labels:datas.map(fd),datasets:LP_hist.map((p)=>{const didK=primeira[p]||null;const c=COR[porAds.indexOf(p)%COR.length];return{label:p,data:serie(p),borderColor:c,backgroundColor:"transparent",borderWidth:2,pointBackgroundColor:datas.map(dk=>dk===didK?"#fb7185":c),pointRadius:datas.map(dk=>dk===didK?5:2),pointHoverRadius:6,tension:.35,spanGaps:true};})},
  options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"bottom",labels:{color:"#b8b8d0",font:{size:11,family:"Space Grotesk"},padding:10,boxWidth:8,usePointStyle:true}},tooltip:{callbacks:{label:ctx=>" "+ctx.dataset.label+": "+(ctx.parsed.y??"—")+" ads"}}},scales:{x:{ticks:{color:"#7a7a98",font:{size:11}},grid:{color:"#1c1c30"}},y:{ticks:{color:"#7a7a98",font:{size:11}},grid:{color:"#1c1c30"},beginAtZero:false}}}
});

escalando.forEach((x,i)=>{
  const el=document.getElementById(P+"spark"+i);
  if(!el)return;
  new Chart(el,{type:"line",data:{labels:datas,datasets:[{data:serie(x.p),borderColor:x.color,backgroundColor:"transparent",borderWidth:2,pointRadius:0,tension:.4,spanGaps:true}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{enabled:false}},scales:{x:{display:false},y:{display:false}},elements:{line:{borderCapStyle:"round"}}}});
});

// ── Tabela histórica de coletas ──────────────────────────────────────────────
const histSection=document.getElementById(P+"hist-section");
if(!HD.dates.length||!HD.libs.length){
  histSection.innerHTML='<div class="empty-hint" style="margin:0">Nenhum dado histórico disponível ainda. Aguarde as próximas coletas automáticas.</div>';
}else{
  function fdH(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y.slice(2)}
  function slotCell(nome,dk,slot){
    const v=HD.map[nome]?.[dk]?.[slot];
    if(v===undefined||v===null)return '<span class="hist-slot empty">—</span>';
    return '<span class="hist-slot">'+v+'</span>';
  }
  let thead='<thead><tr>'
    +'<th style="text-align:left;white-space:nowrap">Biblioteca</th>'
    +'<th style="text-align:left;white-space:nowrap">Data</th>'
    +'<th style="text-align:center">03h</th>'
    +'<th style="text-align:center">12h</th>'
    +'<th style="text-align:center">22h</th>'
    +'</tr></thead>';
  let tbody2='<tbody>';
  let rowCount=0;
  for(const lib of HD.libs){
    let firstForLib=true;
    for(const dk of HD.dates){
      const slots=HD.map[lib]?.[dk];
      if(!slots)continue;
      const hasAny=slots[3]!==undefined||slots[12]!==undefined||slots[22]!==undefined;
      if(!hasAny)continue;
      tbody2+='<tr>'
        +'<td class="lib-name">'+(firstForLib?lib:'')+'</td>'
        +'<td class="date-col">'+fdH(dk)+'</td>'
        +'<td>'+slotCell(lib,dk,3)+'</td>'
        +'<td>'+slotCell(lib,dk,12)+'</td>'
        +'<td>'+slotCell(lib,dk,22)+'</td>'
        +'</tr>';
      firstForLib=false;
      rowCount++;
    }
    if(!firstForLib){
      tbody2+='<tr style="height:4px;background:var(--bg)"><td colspan="5"></td></tr>';
    }
  }
  tbody2+='</tbody>';
  histSection.innerHTML='<div class="tbl-scroll-x"><table class="hist-tbl">'+thead+tbody2+'</table></div>';
  const metaEl=document.getElementById(P+"acc-meta");
  if(metaEl)metaEl.textContent='('+rowCount+' registros)';
}
}

const D_DOM=__DADOS_DOM__;
const HD_DOM=__HIST_DOM__;
const D_PAG=__DADOS_PLACEHOLDER__;
const HD_PAG=__HIST_PLACEHOLDER__;

const totalLibs=Object.keys(D_DOM.pags).length+Object.keys(D_PAG.pags).length;
document.getElementById("livecount").textContent=Object.keys(D_DOM.pags).length+" domínios · "+Object.keys(D_PAG.pags).length+" Páginas/FanPage";

render(D_PAG,HD_PAG,"pag_");
render(D_DOM,HD_DOM,"dom_");

<\/script>
</body>
</html>`
      .replace("__DADOS_DOM__", dadosDom)
      .replace("__HIST_DOM__", histDadosDom)
      .replace("__DADOS_PLACEHOLDER__", dados)
      .replace("__HIST_PLACEHOLDER__", histDados));
  } catch (err) {
    res.status(500).send("Erro: " + err.message);
  }
});

// ─── Scheduler ───────────────────────────────────────────────────────────────

// O agendamento agora é feito externamente via rota GET /api/cron/tick (ex: UptimeRobot a cada 5 min)
console.log("[CRON] Usando arquitetura de Fila Assíncrona via /api/cron/tick");

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
initDb().then(() => {
  app.listen(PORT, () => console.log(`[SERVER] Running on port ${PORT}`));
});