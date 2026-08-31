# TG Magnet Stremio

Paste a magnet link → downloads torrent → uploads to your Telegram channel → streams in Stremio.

Render only handles lightweight API calls. All file storage and streaming goes through Telegram CDN.

## Architecture

```
Magnet Link → [Render: one-time download+upload] → Telegram Channel (storage)
                                                         ↓
Stremio ← [Render: just metadata JSON] ← Telegram CDN (streaming)
```

## Prerequisites

1. **API_ID + API_HASH** → Go to [my.telegram.org](https://my.telegram.org) → API Development Tools → Create application
2. **BOT_TOKEN** → Message [@BotFather](https://t.me/BotFather) on Telegram → `/newbot` → follow prompts
3. **Telegram Channel** → Create a private channel → add your bot as **admin**
4. **Channel ID** → Forward a message from your channel to [@userinfobot](https://t.me/userinfobot) or [@RawDataBot](https://t.me/RawDataBot) — it will show the ID (looks like `-1001234567890`)
5. **Your User ID** → Message [@userinfobot](https://t.me/userinfobot) — it will show your numeric ID
6. **(Optional) TMDb API Key** → Go to [themoviedb.org/settings/api](https://www.themoviedb.org/settings/api) → Create API key (for auto-posters and IMDB matching in Stremio)

## Step 1: Generate Session String (one-time, on your own machine)

This lets the server upload files to your Telegram account and stream from it.

```bash
pip install pyrogram tgcrypto
python setup_session.py
```

It will ask for:
- Your phone number (with country code, e.g. `+919876543210`)
- The confirmation code Telegram sends you
- Your 2FA password (if enabled)

**Copy the long string it prints.** This is your `SESSION_STRING`.

## Step 2: Deploy on Render (Free)

1. **Push this repo to GitHub** (already done if you're reading this)
2. Go to [render.com](https://render.com) → Sign up / Log in
3. Click **New** → **Web Service** → **Connect** your GitHub repo
4. Set **Environment** to **Docker** (auto-detected from Dockerfile)
5. Add these **Environment Variables**:

   | Variable | Required | What to put |
   |----------|----------|-------------|
   | `API_ID` | Yes | Your API ID from my.telegram.org |
   | `API_HASH` | Yes | Your API hash from my.telegram.org |
   | `BOT_TOKEN` | Yes | From @BotFather |
   | `SESSION_STRING` | Yes | Output of `setup_session.py` |
   | `CHANNEL_ID` | Yes | Your channel ID (e.g. `-1001234567890`) |
   | `OWNER_ID` | Yes | Your Telegram numeric user ID |
   | `BASE_URL` | Yes | Your Render URL (e.g. `https://telestr.onrender.com`) |
   | `TMDB_API_KEY` | No | Your TMDb API key for posters |

6. Click **Create Free Web Service**
7. Wait for build to finish (~3-5 minutes)

## Step 3: Connect Stremio

1. Open your Render URL (e.g. `https://telestr.onrender.com`)
2. You'll see the dashboard with an **Addon URL** at the top
3. Copy that URL
4. Open **Stremio** → **Addons** → Paste the URL → **Install**
5. Your library (initially empty) appears under the addon

## Step 4: Add Content

**Option A: Web Dashboard**
- Open your Render URL
- Paste a magnet link in the input box
- Click "Add & Download"
- Wait for the progress bar to complete

**Option B: Telegram Bot**
- Send a magnet link to your bot in a private message
- Bot replies with a download ID
- Check the dashboard for progress

> Only the OWNER_ID user can send commands to the bot.

## How Streaming Works

1. You paste a magnet link (dashboard or bot)
2. Server downloads the torrent to temporary disk
3. Server uploads the file to your Telegram channel
4. Local temp file is deleted
5. When you press play in Stremio, the server fetches from Telegram CDN and proxies the stream to your player
6. Render bandwidth is only used while you are actively watching

## File Size Limits (Render Free Tier)

| Resource | Limit |
|----------|-------|
| RAM | 512 MB |
| Disk | 512 MB (ephemeral) |
| Max file size | ~2 GB recommended |
| Service sleeps | After 15 min no traffic |

> Since the file is deleted after uploading to Telegram, disk usage is only temporary during download.

## Limitations

- **Cold start**: Render free tier sleeps after 15 min. First Stremio request takes ~30-50 seconds to wake up
- **No split file support**: Only the largest video file in a torrent is downloaded
- **SQLite is ephemeral**: Database resets on each deploy. The app auto re-scans your Telegram channel to rebuild the catalog on first boot
- **Streaming bandwidth**: Goes through Render (Telegram → Render → Player). For heavy use, upgrade to a VPS

## Project Structure

```
tg-magnet-stremio/
├── main.py              # FastAPI app + bot webhook + Stremio addon + dashboard API
├── downloader.py         # libtorrent download + filename parser
├── db.py                 # SQLite metadata store
├── static/
│   └── index.html        # Web dashboard (dark theme)
├── setup_session.py      # One-time session string generator (run locally)
├── Dockerfile            # Render deployment
├── render.yaml           # Render config template
├── start.sh              # Entrypoint script
├── requirements.txt      # Python dependencies
└── .env.example          # Environment variable template
```

## Troubleshooting

**Build fails on Render**
- Make sure all env vars are set (especially `SESSION_STRING`)
- Check Render logs for the exact error

**Bot doesn't respond**
- Make sure `OWNER_ID` is correct (numeric, no quotes)
- If Render was sleeping, the webhook might not deliver — visit your dashboard URL first to wake it up, then resend

**Files not showing in Stremio**
- Make sure `BASE_URL` is set correctly (with `https://`, no trailing slash)
- Re-add the addon URL in Stremio
- Check the dashboard to confirm files exist

**Download stuck at 0%**
- The torrent might have no peers — try a different magnet link
- Check Render logs for errors

**Stream doesn't play**
- The file might be too large or in an unsupported format
- Try a smaller file first (~300MB) to test
