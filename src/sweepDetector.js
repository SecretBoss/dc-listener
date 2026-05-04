// NFT Sweep Detector
// Subscribes to OpenSea Stream API for the top 200 ETH collections by market cap
// and posts a Discord alert when a collection sees >= SWEEP_THRESHOLD sales
// within SWEEP_WINDOW_MS. No polling, no Alchemy CU usage.
//
// Required env vars:
//   OPENSEA_API_KEY            — for REST top-collections lookup + Stream auth
//   DISCORD_BOT_TOKEN          — already set, used to post via REST
//   SWEEP_ALERT_CHANNEL_ID     — Discord channel to post into
//
// Optional:
//   SWEEP_THRESHOLD            — sales count that triggers alert (default 10)
//   SWEEP_WINDOW_MS            — sliding window in ms (default 60000)
//   SWEEP_COOLDOWN_MS          — per-collection cooldown after alert (default 300000)
//   TOP_COLLECTIONS_COUNT      — how many top collections to track (default 200)
//   INGEST_URL / INGEST_TOKEN   — used to pull live /sweep config from backend

import { OpenSeaStreamClient, Network } from "@opensea/stream-js";
import { WebSocket } from "ws";

const {
  OPENSEA_API_KEY,
  DISCORD_BOT_TOKEN,
  SWEEP_ALERT_CHANNEL_ID,
  SWEEP_THRESHOLD = "10",
  SWEEP_WINDOW_MS = "60000",
  SWEEP_COOLDOWN_MS = "300000",
  TOP_COLLECTIONS_COUNT = "200",
  INGEST_URL,
  INGEST_TOKEN,
} = process.env;

// Mutable at runtime via setSweepConfig() so Discord commands can tune live.
let THRESHOLD = Number(SWEEP_THRESHOLD);
let WINDOW_MS = Number(SWEEP_WINDOW_MS);
let COOLDOWN_MS = Number(SWEEP_COOLDOWN_MS);
let PING_ROLE_ID = "0";
let ETH_USD = null;
const TOP_N = Number(TOP_COLLECTIONS_COUNT);

export function getSweepConfig() {
  return { threshold: THRESHOLD, windowMs: WINDOW_MS, cooldownMs: COOLDOWN_MS, pingRoleId: PING_ROLE_ID, ethUsd: ETH_USD, tracked: collectionMeta.size };
}

export function setSweepConfig({ threshold, windowMs, cooldownMs, pingRoleId, ethUsd } = {}) {
  if (Number.isFinite(threshold) && threshold > 0) THRESHOLD = Math.floor(threshold);
  if (Number.isFinite(windowMs) && windowMs >= 1000) WINDOW_MS = Math.floor(windowMs);
  if (Number.isFinite(cooldownMs) && cooldownMs >= 0) COOLDOWN_MS = Math.floor(cooldownMs);
  if (typeof pingRoleId === "string" && /^\d+$/.test(pingRoleId)) PING_ROLE_ID = pingRoleId;
  if (Number.isFinite(ethUsd) && ethUsd > 0) ETH_USD = ethUsd;
  return getSweepConfig();
}

async function refreshSweepConfig() {
  if (!INGEST_URL || !INGEST_TOKEN) return;
  try {
    const res = await fetch(INGEST_URL, { headers: { "x-ingest-token": INGEST_TOKEN } });
    if (!res.ok) {
      console.error(`[sweep config] HTTP ${res.status}: ${await res.text().catch(() => "")}`);
      return;
    }
    const cfg = await res.json();
    setSweepConfig({
      threshold: Number(cfg.sweep_threshold),
      windowMs: Number(cfg.sweep_window_ms),
      cooldownMs: Number(cfg.sweep_cooldown_ms),
      pingRoleId: String(cfg.sweep_ping_role_id ?? "0"),
      ethUsd: Number(cfg.eth_usd),
    });
  } catch (e) {
    console.error("[sweep config] refresh failed", e?.message ?? e);
  }
}

const sales = new Map(); // slug -> [{ ts, priceEth, image, name, slug }]
const lastAlertAt = new Map(); // slug -> ts
const collectionMeta = new Map(); // slug -> { name, image }

// Active stream clients across all shards. Kept at module scope so the
// daily refresh can tear them down and rebuild against the new top list.
let activeClients = [];

const SHARD_SIZE = Number(process.env.SWEEP_SHARD_SIZE ?? "40");
const JOIN_DELAY_MS = Number(process.env.SWEEP_JOIN_DELAY_MS ?? "100");

export async function startSweepDetector() {
  if (!OPENSEA_API_KEY || !DISCORD_BOT_TOKEN || !SWEEP_ALERT_CHANNEL_ID) {
    console.warn("[sweep] disabled — missing OPENSEA_API_KEY / DISCORD_BOT_TOKEN / SWEEP_ALERT_CHANNEL_ID");
    return;
  }

  await refreshSweepConfig();
  setInterval(refreshSweepConfig, 30_000);
  await subscribeTopCollections();

  // Refresh the top-N list every 24h. We tear down existing sockets and
  // rebuild against the freshly ordered list so newly trending collections
  // get tracked and dropped ones get released.
  setInterval(async () => {
    try {
      console.log("[sweep] daily refresh — re-fetching top collections by 1d volume");
      await subscribeTopCollections();
    } catch (e) {
      console.error("[sweep] refresh failed", e?.message ?? e);
    }
  }, 24 * 60 * 60 * 1000);
}

async function subscribeTopCollections() {
  const slugs = await fetchTopSlugs(TOP_N);
  if (slugs.length === 0) {
    console.error("[sweep] no slugs returned, skipping");
    return;
  }
  console.log(`[sweep] tracking ${slugs.length} collections, threshold=${THRESHOLD} in ${WINDOW_MS}ms`);

  // Tear down any existing clients before rebuilding.
  if (activeClients.length) {
    console.log(`[sweep] disconnecting ${activeClients.length} previous sockets`);
    for (const c of activeClients) {
      try { c?.disconnect?.(); } catch { /* ignore */ }
    }
    activeClients = [];
    // Reset per-collection state so stale slugs don't leak counters.
    sales.clear();
  }

  // OpenSea Stream caps channel joins per WebSocket connection (~50). To track
  // 200 collections we shard the slug list across multiple Stream clients —
  // each opens its own websocket — and stagger joins to avoid rate limits.
  const shards = [];
  for (let i = 0; i < slugs.length; i += SHARD_SIZE) {
    shards.push(slugs.slice(i, i + SHARD_SIZE));
  }
  console.log(`[sweep] sharding ${slugs.length} slugs into ${shards.length} sockets (size=${SHARD_SIZE})`);

  const sdkSockets = [];
  for (let s = 0; s < shards.length; s++) {
    const shard = shards[s];
    const client = new OpenSeaStreamClient({
      network: Network.MAINNET,
      token: OPENSEA_API_KEY,
      connectOptions: { transport: WebSocket },
      onError: (err) => console.error(`[sweep stream error shard ${s}]`, err?.message ?? err),
    });
    activeClients.push(client);
    const sdkSocket =
      client?.socket?.socket ?? client?.socket ?? client?._socket ?? null;
    sdkSockets.push(sdkSocket);

    for (let i = 0; i < shard.length; i++) {
      const slug = shard[i];
      await new Promise((r) => setTimeout(r, JOIN_DELAY_MS));
      try {
        client.onItemSold(slug, (event) => handleSale(slug, event));
      } catch (e) {
        console.error(`[sweep] subscribe failed for ${slug}`, e?.message ?? e);
      }
    }
  }

  setTimeout(() => {
    let joined = 0;
    let failed = 0;
    let pending = 0;
    const failedSlugs = [];
    for (const sdkSocket of sdkSockets) {
      const channels = sdkSocket?.channels ?? [];
      for (const ch of channels) {
        const topic = ch?.topic ?? "";
        if (!topic.startsWith("collection:")) continue;
        const slug = topic.slice("collection:".length);
        const state = typeof ch?.state === "string" ? ch.state : "unknown";
        if (state === "joined") joined++;
        else if (state === "errored" || state === "closed") {
          failed++;
          failedSlugs.push(slug);
        } else {
          pending++;
        }
      }
    }
    console.log(
      `[sweep] subscription summary — attempted=${slugs.length}, sockets=${sdkSockets.length}, joined=${joined}, failed=${failed}, pending=${pending}`,
    );
    if (failedSlugs.length) {
      console.log(`[sweep] failed slugs (${failedSlugs.length}): ${failedSlugs.join(", ")}`);
    }
  }, 60_000);
}

function handleSale(slug, event) {
  try {
    const payload = event?.payload ?? {};
    const item = payload.item ?? {};
    const sale = payload.sale_price ? BigInt(payload.sale_price) : 0n;
    const decimals = Number(payload.payment_token?.decimals ?? 18);
    const symbol = (payload.payment_token?.symbol ?? "").toUpperCase();

    // Only count ETH/WETH sales.
    if (!["ETH", "WETH"].includes(symbol)) return;

    // Sweep = ONE buyer scooping many items. Keep only buy-side events by
    // identifying the taker (buyer) wallet. WETH bid-fills have the buyer as
    // the maker — we treat those as sells (collection offers being accepted)
    // and ignore them here so we don't mistake "10 different sellers
    // accepting bids" for a sweep.
    const taker = String(payload.taker?.address ?? "").toLowerCase();
    if (!taker) return;
    if (symbol === "WETH") return; // WETH = bid acceptance, not a buy-side sweep

    const priceEth = Number(sale) / Math.pow(10, decimals);
    const now = Date.now();

    const meta = collectionMeta.get(slug) ?? {
      name: item?.metadata?.name?.split(" #")?.[0] ?? slug,
      image: item?.metadata?.image_url ?? null,
    };
    if (!collectionMeta.has(slug)) collectionMeta.set(slug, meta);

    const arr = sales.get(slug) ?? [];
    arr.push({ ts: now, priceEth, buyer: taker });
    const cutoff = now - WINDOW_MS;
    while (arr.length && arr[0].ts < cutoff) arr.shift();
    sales.set(slug, arr);

    if (arr.length < THRESHOLD) return;
    const last = lastAlertAt.get(slug) ?? 0;
    if (now - last < COOLDOWN_MS) return;

    lastAlertAt.set(slug, now);
    void postSweepAlert(slug, meta, arr.slice());
  } catch (e) {
    console.error("[sweep handleSale]", e?.message ?? e);
  }
}

function fmtUsd(eth) {
  if (!ETH_USD || !Number.isFinite(eth)) return "";
  const usd = eth * ETH_USD;
  const formatted = usd >= 1000
    ? `$${Math.round(usd).toLocaleString("en-US")}`
    : `$${usd.toFixed(2)}`;
  return ` (${formatted})`;
}

async function postSweepAlert(slug, meta, window) {
  const count = window.length;
  const totalEth = window.reduce((s, x) => s + x.priceEth, 0);
  const avgEth = totalEth / count;
  const minEth = Math.min(...window.map((x) => x.priceEth));
  const uniqueBuyers = new Set(window.map((x) => x.buyer).filter(Boolean)).size;
  const slugUrl = `https://opensea.io/collection/${slug}`;

  const embed = {
    title: `🧹 Sweep detected: ${meta.name}`,
    url: slugUrl,
    color: 0xf59e0b,
    description: `**${count}** buys in the last ${Math.round(WINDOW_MS / 1000)}s · ${uniqueBuyers} buyer${uniqueBuyers === 1 ? "" : "s"}`,
    fields: [
      { name: "Total Volume", value: `${totalEth.toFixed(3)} ETH${fmtUsd(totalEth)}`, inline: true },
      { name: "Avg Price", value: `${avgEth.toFixed(4)} ETH${fmtUsd(avgEth)}`, inline: true },
      { name: "Floor in Sweep", value: `${minEth.toFixed(4)} ETH${fmtUsd(minEth)}`, inline: true },
      { name: "Collection", value: `[View on OpenSea](${slugUrl})`, inline: false },
    ],
    timestamp: new Date().toISOString(),
  };
  if (meta.image) embed.thumbnail = { url: meta.image };

  const content = PING_ROLE_ID && PING_ROLE_ID !== "0" ? `<@&${PING_ROLE_ID}>` : undefined;
  const allowed_mentions = content ? { parse: [], roles: [PING_ROLE_ID] } : { parse: [] };

  try {
    const res = await fetch(
      `https://discord.com/api/v10/channels/${SWEEP_ALERT_CHANNEL_ID}/messages`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bot ${DISCORD_BOT_TOKEN}`,
        },
        body: JSON.stringify({ ...(content ? { content } : {}), embeds: [embed], allowed_mentions }),
      },
    );
    if (!res.ok) {
      const t = await res.text().catch(() => "");
      console.error(`[sweep post] HTTP ${res.status}: ${t}`);
    } else {
      console.log(`[sweep] alert posted for ${slug} (${count} sales, ${totalEth.toFixed(3)} ETH)`);
    }
  } catch (e) {
    console.error("[sweep post] network", e?.message ?? e);
  }
}

async function fetchTopSlugs(limit) {
  const slugs = [];
  let cursor = null;
  // OpenSea v2 returns up to 100 per page.
  while (slugs.length < limit) {
    const url = new URL("https://api.opensea.io/api/v2/collections");
    url.searchParams.set("chain", "ethereum");
    // Rank by last 24h trading volume so we always track what's actually moving.
    url.searchParams.set("order_by", "one_day_volume");
    url.searchParams.set("limit", "100");
    if (cursor) url.searchParams.set("next", cursor);
    const res = await fetch(url, { headers: { "x-api-key": OPENSEA_API_KEY, accept: "application/json" } });
    if (!res.ok) {
      console.error(`[sweep] top list HTTP ${res.status}: ${await res.text().catch(() => "")}`);
      break;
    }
    const json = await res.json();
    for (const c of json?.collections ?? []) {
      if (c?.collection) {
        slugs.push(c.collection);
        if (c?.name || c?.image_url) {
          collectionMeta.set(c.collection, { name: c.name ?? c.collection, image: c.image_url ?? null });
        }
      }
      if (slugs.length >= limit) break;
    }
    cursor = json?.next ?? null;
    if (!cursor) break;
  }
  return slugs.slice(0, limit);
}