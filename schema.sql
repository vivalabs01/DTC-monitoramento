-- ═══════════════════════════════════════════════════════════════════════════
-- schema.sql — DTC Monitor (Meta Ads Library Monitor)
-- ═══════════════════════════════════════════════════════════════════════════
-- Snapshot do schema tal como é criado/mantido por initDb() em index.js.
-- Reflete o estado ATUAL (colunas de ALTER TABLE já incorporadas nas
-- definições de CREATE TABLE abaixo, e o CHECK de funnel_nodes.tipo já
-- incluindo 'ads' e 'presell').
--
-- Este arquivo é referência/documentação e para provisionar um banco NOVO
-- do zero. Em produção, quem efetivamente cria/migra as tabelas é o
-- initDb() do index.js, rodando automaticamente a cada boot do processo —
-- rodar este arquivo manualmente não é necessário no dia a dia.
--
-- Ordem de criação respeita as foreign keys:
--   pages → funnel_nodes → funnel_edges
-- ═══════════════════════════════════════════════════════════════════════════

-- ─── pages ────────────────────────────────────────────────────────────────
-- Cada linha é uma "biblioteca" rastreada (Página/FanPage ou Domínio) na
-- Meta Ad Library.
CREATE TABLE IF NOT EXISTS pages (
  slug           TEXT PRIMARY KEY,
  nome           TEXT NOT NULL,
  url            TEXT NOT NULL,
  tipo           TEXT NOT NULL DEFAULT 'pagina',   -- 'pagina' | 'dominio'
  inicial_count  INTEGER,                          -- contagem no momento da descoberta
  instagram_url  TEXT,
  geo            TEXT,
  nicho          TEXT,
  funil          TEXT,
  brand          TEXT,
  created_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ─── scrape_history ──────────────────────────────────────────────────────
-- Série histórica de coletas, uma linha por (slug, slot, momento da coleta).
-- slot: 3 | 12 | 22 (horário UTC do tick) | NULL (coleta manual/avulsa).
CREATE TABLE IF NOT EXISTS scrape_history (
  id           SERIAL PRIMARY KEY,
  slug         TEXT NOT NULL,
  ads_count    INTEGER NOT NULL,
  slot         SMALLINT,
  collected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scrape_history_slug ON scrape_history(slug);

-- ─── scrape_latest ───────────────────────────────────────────────────────
-- Última contagem conhecida por slug (cache de leitura rápida pro dashboard).
CREATE TABLE IF NOT EXISTS scrape_latest (
  slug         TEXT PRIMARY KEY,
  ads_count    INTEGER NOT NULL,
  collected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ─── brands ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS brands (
  id           SERIAL PRIMARY KEY,
  nome         TEXT UNIQUE NOT NULL,
  site         TEXT,
  created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ─── funnel_nodes ────────────────────────────────────────────────────────
-- Nós do grafo de funil (mapeamento de funis). Cada nó pertence a uma page
-- (slug) e representa uma etapa: anúncio, advertorial, presell, TSL, VSL,
-- quiz, whatsapp ou checkout.
CREATE TABLE IF NOT EXISTS funnel_nodes (
  id         SERIAL PRIMARY KEY,
  slug       TEXT NOT NULL REFERENCES pages(slug) ON DELETE CASCADE,
  tipo       TEXT NOT NULL CHECK (tipo IN ('ads','advertorial','presell','tsl','vsl','quiz','whatsapp','checkout')),
  rotulo     TEXT NOT NULL,
  url        TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funnel_nodes_slug ON funnel_nodes(slug);

-- ─── funnel_edges ────────────────────────────────────────────────────────
-- Conexões nó → nó do grafo de funil (from_node_id "leva para" to_node_id).
CREATE TABLE IF NOT EXISTS funnel_edges (
  id           SERIAL PRIMARY KEY,
  from_node_id INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
  to_node_id   INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
  created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funnel_edges_from ON funnel_edges(from_node_id);
CREATE INDEX IF NOT EXISTS idx_funnel_edges_to   ON funnel_edges(to_node_id);

-- ═══════════════════════════════════════════════════════════════════════════
-- Fim. 6 tabelas: pages, scrape_history, scrape_latest, brands,
-- funnel_nodes, funnel_edges.
-- ═══════════════════════════════════════════════════════════════════════════