// Penni Discord activity listener.
// Connects to the Discord Gateway 24/7 and forwards three signal types to the
// Penni backend's `ingest-discord-activity` endpoint:
//   - message       (one row per message sent)
//   - reaction      (one row per reaction added)
//   - voice_minute  (one row per minute a user is in a voice channel, ticked every 60s)
//
// We deliberately do NOT use the Supabase service_role key here. Instead we POST
// batches to a dedicated, scope-limited edge function authenticated with a shared
// secret token. If this token leaks, the worst an attacker can do is insert junk
// rows into discord_activity (auto-pruned after 14 days). They cannot read users
// or touch any other table.
//
// Required env vars (set in Railway Variables):
//   DISCORD_BOT_TOKEN          — bot token with Server Members + read access to messages/reactions/voice
//   GUILD_ID                   — 1388614650895138898  (Penni server)
//   INGEST_URL                 — https://jmrnfwrmowkgejufeprg.supabase.co/functions/v1/ingest-discord-activity
//   INGEST_TOKEN               — shared secret matching DISCORD_LISTENER_INGEST_TOKEN in the backend
//
// Optional:
//   FLUSH_INTERVAL_MS          — batched flush interval (default 5000)
//
// Discord intents required on the bot in the Developer Portal:
//   - GUILDS
//   - GUILD_MESSAGES        (just to receive the MESSAGE_CREATE event — content not needed)
//   - GUILD_MESSAGE_REACTIONS
//   - GUILD_VOICE_STATES

import { Client, GatewayIntentBits, Events } from "discord.js";
import { startSweepDetector } from "./sweepDetector.js";

const {
  DISCORD_BOT_TOKEN,
  GUILD_ID,
  INGEST_URL,
  INGEST_TOKEN,
  FLUSH_INTERVAL_MS = "5000",
} = process.env;

for (const [k, v] of Object.entries({ DISCORD_BOT_TOKEN, GUILD_ID, INGEST_URL, INGEST_TOKEN })) {
  if (!v) {
    console.error(`[fatal] missing env var: ${k}`);
    process.exit(1);
  }
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.GuildMessageReactions,
    GatewayIntentBits.GuildVoiceStates,
  ],
});

// -------- Batched writer --------
// Buffer events and flush every FLUSH_INTERVAL_MS to keep request volume sane.
// On crash we lose at most ~5s of activity, fine for a 7-day rolling stat.
const buffer = [];
function enqueue(row) {
  buffer.push(row);
}

async function flush() {
  if (buffer.length === 0) return;
  // Cap each request to 500 events (matches backend MAX_BATCH).
  const batch = buffer.splice(0, Math.min(500, buffer.length));
  try {
    const res = await fetch(INGEST_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-ingest-token": INGEST_TOKEN,
      },
      body: JSON.stringify({ events: batch }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      console.error(`[flush] HTTP ${res.status}: ${text}`);
      buffer.unshift(...batch); // retry next tick
      return;
    }
    const data = await res.json().catch(() => ({}));
    console.log(`[flush] sent ${batch.length} events (inserted ${data.inserted ?? "?"})`);
  } catch (err) {
    console.error("[flush] network error:", err.message);
    buffer.unshift(...batch);
  }
}
setInterval(flush, Number(FLUSH_INTERVAL_MS));

// -------- Voice tracking --------
// Discord doesn't tell us "minute ticks" — we track who's in voice and emit a
// voice_minute event every 60s for each non-bot, non-deafened, non-muted user.
const voiceUsers = new Map(); // discordId -> channelId

client.on(Events.VoiceStateUpdate, (_oldState, newState) => {
  const userId = newState.id;
  if (newState.member?.user?.bot) return;

  if (newState.channelId) {
    if (!newState.serverMute && !newState.serverDeaf) {
      voiceUsers.set(userId, newState.channelId);
    } else {
      voiceUsers.delete(userId);
    }
  } else {
    voiceUsers.delete(userId);
  }
});

setInterval(() => {
  for (const [discordId, channelId] of voiceUsers.entries()) {
    enqueue({
      discord_id: discordId,
      event_type: "voice_minute",
      weight: 1,
      channel_id: channelId,
    });
  }
}, 60_000);

// -------- Messages --------
client.on(Events.MessageCreate, (msg) => {
  if (msg.author?.bot) return;
  if (msg.guildId !== GUILD_ID) return;
  enqueue({
    discord_id: msg.author.id,
    event_type: "message",
    weight: 1,
    channel_id: msg.channelId,
  });
});

// -------- Reactions --------
client.on(Events.MessageReactionAdd, (reaction, user) => {
  if (user?.bot) return;
  enqueue({
    discord_id: user.id,
    event_type: "reaction",
    weight: 1,
    channel_id: reaction.message?.channelId ?? null,
  });
});

client.once(Events.ClientReady, (c) => {
  console.log(`[ready] logged in as ${c.user.tag}, watching guild ${GUILD_ID}`);
  console.log(`[ready] forwarding to ${INGEST_URL}`);
  startSweepDetector().catch((e) => console.error("[sweep] failed to start", e));
});

client.on("error", (err) => console.error("[discord error]", err));
process.on("unhandledRejection", (err) => console.error("[unhandledRejection]", err));

async function shutdown(signal) {
  console.log(`[shutdown] ${signal} received, flushing…`);
  try { await flush(); } catch (e) { console.error(e); }
  try { await client.destroy(); } catch { /* ignore */ }
  process.exit(0);
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

client.login(DISCORD_BOT_TOKEN);
