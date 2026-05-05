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

import { Client, GatewayIntentBits, Events, REST, Routes, SlashCommandBuilder, MessageFlags } from "discord.js";
import { startSweepDetector, setSweepConfig, getSweepConfig } from "./sweepDetector.js";

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

// Hardcoded: only these Discord user IDs may run /sweep, and only in this channel.
const SWEEP_ADMINS = new Set(["535883101302292480", "257150199003217920"]);
const SWEEP_COMMAND_CHANNEL_ID = "1500830957748621312";

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

// -------- Slash commands (/sweep ...) --------
const sweepCommand = new SlashCommandBuilder()
  .setName("sweep")
  .setDescription("Configure NFT sweep alert detector")
  .addSubcommand((s) => s.setName("status").setDescription("Show current sweep config"))
  .addSubcommand((s) =>
    s
      .setName("threshold")
      .setDescription("Set how many sales within the window trigger an alert")
      .addIntegerOption((o) =>
        o.setName("value").setDescription("Sales count (>=1)").setRequired(true).setMinValue(1).setMaxValue(1000),
      ),
  )
  .addSubcommand((s) =>
    s
      .setName("window")
      .setDescription("Set the sliding window (in seconds) used to count sales")
      .addIntegerOption((o) =>
        o.setName("seconds").setDescription("Window length in seconds (>=1)").setRequired(true).setMinValue(1).setMaxValue(86400),
      ),
  )
  .addSubcommand((s) =>
    s
      .setName("cooldown")
      .setDescription("Set the per-collection cooldown (in seconds) after an alert")
      .addIntegerOption((o) =>
        o.setName("seconds").setDescription("Cooldown in seconds (>=0)").setRequired(true).setMinValue(0).setMaxValue(86400),
      ),
  )
  .addSubcommand((s) =>
    s
      .setName("role")
      .setDescription("Set the role pinged on sweep alerts (use 0 to disable)")
      .addStringOption((o) =>
        o.setName("role_id").setDescription("Discord role ID, or 0 to disable").setRequired(true),
      ),
  );

async function registerSlashCommands(appId) {
  const rest = new REST({ version: "10" }).setToken(DISCORD_BOT_TOKEN);
  try {
    const commands = await rest.get(Routes.applicationGuildCommands(appId, GUILD_ID));
    const existing = Array.isArray(commands) ? commands.find((cmd) => cmd.name === "sweep") : null;
    if (existing?.id) {
      await rest.patch(Routes.applicationGuildCommand(appId, GUILD_ID, existing.id), { body: sweepCommand.toJSON() });
    } else {
      await rest.post(Routes.applicationGuildCommands(appId, GUILD_ID), { body: sweepCommand.toJSON() });
    }
    console.log("[slash] registered /sweep in guild", GUILD_ID);
  } catch (e) {
    console.error("[slash] register failed", e?.message ?? e);
  }
}

client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isChatInputCommand()) return;
  if (interaction.commandName !== "sweep") return;

  if (!SWEEP_ADMINS.has(interaction.user.id)) {
    await interaction.reply({ content: "⛔ not authorized", flags: MessageFlags.Ephemeral }).catch(() => {});
    return;
  }
  if (interaction.channelId !== SWEEP_COMMAND_CHANNEL_ID) {
    await interaction.reply({
      content: `⛔ this command can only be used in <#${SWEEP_COMMAND_CHANNEL_ID}>`,
      flags: MessageFlags.Ephemeral,
    }).catch(() => {});
    return;
  }

  try {
    const sub = interaction.options.getSubcommand();
    let cfg;
    if (sub === "status") cfg = getSweepConfig();
    else if (sub === "threshold") cfg = setSweepConfig({ threshold: interaction.options.getInteger("value", true) });
    else if (sub === "window") cfg = setSweepConfig({ windowMs: interaction.options.getInteger("seconds", true) * 1000 });
    else if (sub === "cooldown") cfg = setSweepConfig({ cooldownMs: interaction.options.getInteger("seconds", true) * 1000 });
    else if (sub === "role") cfg = setSweepConfig({ pingRoleId: interaction.options.getString("role_id", true) });
    else {
      await interaction.reply({ content: "unknown subcommand", flags: MessageFlags.Ephemeral });
      return;
    }
    const pingTxt = cfg.pingRoleId && cfg.pingRoleId !== "0" ? `<@&${cfg.pingRoleId}>` : "disabled";
    await interaction.reply({
      content: `🧹 sweep config — threshold=**${cfg.threshold}** sales / **${Math.round(cfg.windowMs / 1000)}s** window, cooldown=**${Math.round(cfg.cooldownMs / 1000)}s**, ping=${pingTxt}, tracking **${cfg.tracked}** collections`,
      allowed_mentions: { parse: [] },
    });
  } catch (e) {
    await interaction.reply({ content: `error: ${e?.message ?? e}`, flags: MessageFlags.Ephemeral }).catch(() => {});
  }
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
  registerSlashCommands(c.user.id).catch((e) => console.error("[slash] failed", e));
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