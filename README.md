# Penni Discord Activity Listener

A tiny 24/7 Node.js process that listens to the Penni Discord server and forwards
message / reaction / voice-minute events to the Penni backend's
`ingest-discord-activity` endpoint. The Penni website's raffle draw reads the
last 7 days of `discord_activity` to award the **Discord activity boost**
(50+ events = ×2 entries).

This is a **separate process** from the website because Supabase Edge Functions
can't hold a persistent websocket connection, which the Discord Gateway requires.

## Why a custom ingest endpoint?

We deliberately do NOT give this bot the Supabase service role key. Instead it
authenticates with a single-purpose token (`INGEST_TOKEN`) against an edge
function that can ONLY insert into `discord_activity`. If the Railway
environment is ever compromised, the worst an attacker can do is insert junk
rows that get auto-pruned after 14 days. They cannot read user data or touch
any other table.

---

## Deploy on Railway (5 minutes)

### 1. Push this folder to a new GitHub repo
Just this `discord-listener-bot/` folder, not the whole Penni repo.

```bash
cd discord-listener-bot
git init && git add . && git commit -m "init"
# create a new GitHub repo, then:
git remote add origin git@github.com:YOUR_ORG/penni-discord-listener.git
git push -u origin main
```

### 2. Create a Railway project
1. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
2. Pick the `penni-discord-listener` repo
3. Railway auto-detects the `Dockerfile` and starts building

### 3. Add the environment variables
In Railway → your service → **Variables**:

| Name | Value |
|---|---|
| `DISCORD_BOT_TOKEN` | Same token your existing Penni bot uses, **or** a new bot invited to the server |
| `GUILD_ID` | `1388614650895138898` |
| `INGEST_URL` | `https://jmrnfwrmowkgejufeprg.supabase.co/functions/v1/ingest-discord-activity` |
| `INGEST_TOKEN` | The value of `DISCORD_LISTENER_INGEST_TOKEN` you set in Lovable Cloud secrets |

> You generated `DISCORD_LISTENER_INGEST_TOKEN` when wiring this up. Use the
> exact same string here. To rotate later, update both places (Lovable Cloud
> secret + Railway variable) at the same time.

### 4. Enable the right Discord intents
In the [Developer Portal](https://discord.com/developers/applications) → your bot
→ **Bot** tab, enable:
- ✅ **Server Members Intent** (optional but recommended)
- ❌ **Message Content Intent** — NOT needed (we only count *that* a message happened)
- ❌ **Presence Intent** — NOT needed

### 5. Deploy
Railway redeploys automatically. Check the **Deployments → Logs** tab — you should see:
```
[ready] logged in as PenniBot#1234, watching guild 1388614650895138898
[ready] forwarding to https://jmrnfwrmowkgejufeprg.supabase.co/functions/v1/ingest-discord-activity
[flush] sent 12 events (inserted 12)
```

If you see `HTTP 401` in the logs, the `INGEST_TOKEN` doesn't match the secret
in Lovable Cloud — double-check both values.

---

## Cost
- Railway free tier: 500 execution hours/month + $5 of usage credit
- This bot uses ~256 MB RAM and almost zero CPU → fits in the free tier

## Enable the boost on the website
Once the bot is running and the **Activity Boosts** panel in the admin shows
"Listener connected", flip the **Discord activity boost** toggle ON.

## What gets logged
| Event | When | Weight |
|---|---|---|
| `message` | A non-bot user sends a message in the Penni guild | 1 |
| `reaction` | A non-bot user adds a reaction | 1 |
| `voice_minute` | Per minute a non-bot user is in a voice channel and not server-muted/deafened | 1 |

Old rows (>14 days) are pruned by the backend on a schedule.
