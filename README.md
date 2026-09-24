# Radio Showrunner

TZ-aware scheduler and resilient playout engine for Icecast radio streams.  
Watches a Supabase schedule, plays pre-recorded shows via ffmpeg, reconnects/resumes on drops, and exposes a JSON `/status` endpoint.

## Table of Contents

- [Docker deployment (TrueNAS / any host)](#docker-deployment-truenas--any-host)
  - [Prerequisites](#prerequisites)
  - [Quick start](#quick-start)
  - [Adding a new station](#adding-a-new-station)
  - [Port selection](#port-selection)
  - [Health check endpoints](#health-check-endpoints)
  - [Checking logs](#checking-logs)
  - [Building the image](#building-the-image)
- [Environment variable reference](#environment-variable-reference)
- [Running directly (without Docker)](#running-directly-without-docker)
- [How it works](#how-it-works)
- [Features](#features)
- [Local copy mode](#local-copy-mode)
- [Reconnect & EOF behavior](#reconnect--eof-behavior)
- [Icecast admin kick](#icecast-admin-kick)
- [Now Playing integration](#now-playing-integration)
- [Status endpoint](#status-endpoint)
- [Logging](#logging)
- [Database schema](#database-schema)
- [Troubleshooting](#troubleshooting)
- [Security checklist](#security-checklist)

---

## Docker deployment (TrueNAS / any host)

### Prerequisites

- Docker Engine 24+ and Docker Compose v2 (`docker compose`)
- Access to a running Icecast server
- Supabase project(s) with the expected schema (see [Database schema](#database-schema))

### Quick start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_ORG/Radio_showrunner.git
cd Radio_showrunner

# 2. Create your env file from the template
cp .env.compose.example .env.compose

# 3. Edit .env.compose — fill in Supabase URL/key, Icecast host/pass, etc.
nano .env.compose

# 4. Build the image and start all stations
docker compose --env-file .env.compose up -d --build

# 5. Verify both containers are running
docker compose --env-file .env.compose ps

# 6. Check station health
curl http://localhost:8080/status   # station1
curl http://localhost:8081/status   # station2
```

> On TrueNAS: add the app via **Apps → Custom App → Docker Compose**, paste in `docker-compose.yml`, create `.env.compose` via the shell, then start. No other setup needed.

### Adding a new station

1. **Open `docker-compose.yml`** and copy the `station2` block. Rename it `station3`.

2. **Add volumes** — in the `volumes:` section at the bottom, add:
   ```yaml
   station3_cache:
   station3_logs:
   ```

3. **Update the station3 service** to use those volumes and `STATION3_*` variables:
   ```yaml
   station3:
     build: .
     image: radio-showrunner:latest
     restart: unless-stopped
     environment:
       SUPABASE_URL: ${STATION3_SUPABASE_URL}
       SUPABASE_SERVICE_ROLE_KEY: ${STATION3_SERVICE_KEY}
       SUPABASE_STORAGE_BUCKET: ${STATION3_BUCKET:-show-audio}
       ICE_HOST: ${STATION3_ICE_HOST}
       ICE_PORT: ${STATION3_ICE_PORT:-8025}
       ICE_DEFAULT_SOURCE_USER: ${STATION3_ICE_USER}
       ICE_DEFAULT_SOURCE_PASS: ${STATION3_ICE_PASS}
       STATION_TZ: ${STATION3_TZ:-Europe/London}
       ENABLE_LOCAL_COPY: "true"
       ENABLE_FFPROBE_DURATION_CHECK: "true"
       HEALTH_PORT: "8080"
     ports:
       - "${STATION3_HOST_PORT:-8082}:8080"
     volumes:
       - station3_cache:/app/cache
       - station3_logs:/app/logs
   ```

4. **Add variables to `.env.compose`** (see the commented-out STATION3 block in `.env.compose.example`).

5. **Start the new station only** (without restarting the others):
   ```bash
   docker compose --env-file .env.compose up -d station3
   ```

### Port selection

Each station has two port concepts:

| | Value | Where set |
|---|---|---|
| **Internal port** | Always `8080` | Hardcoded in the image — never change |
| **Host port** | Any free port on your server | `STATION1_HOST_PORT`, `STATION2_HOST_PORT`, etc. in `.env.compose` |

To find free ports on your TrueNAS host:
```bash
ss -tlnp | grep LISTEN
```

Pick ports not in that list. Common choices: `8080`, `8081`, `8082`, `9080`, `9081`.

### Health check endpoints

Each running container exposes a `/status` JSON endpoint on its host port:

```bash
curl http://<truenas-ip>:8080/status | jq .   # station1
curl http://<truenas-ip>:8081/status | jq .   # station2
```

The response includes:
- `ok` — whether the runner is healthy
- `live` — any currently playing show
- `upcoming` — next shows in the queue
- `icecast` — whether Icecast is reachable
- `recent_events` — last N job events from the DB
- `last_ffmpeg_errors` — recent ffmpeg error lines

### Checking logs

```bash
# Live log stream for a station
docker compose --env-file .env.compose logs -f station1

# Last 100 lines
docker compose --env-file .env.compose logs --tail=100 station1

# All stations at once
docker compose --env-file .env.compose logs -f
```

ffmpeg per-attempt logs are written to the named volume. To inspect them:
```bash
docker run --rm -v showrunner_station1_logs:/logs alpine ls /logs
docker run --rm -v showrunner_station1_logs:/logs alpine cat /logs/<filename>
```

### Building the image

`docker compose up --build` builds automatically. To build manually:

```bash
docker build -t radio-showrunner:latest .
```

To rebuild after a code change and restart all stations:
```bash
docker compose --env-file .env.compose up -d --build
```

---

## Environment variable reference

### Per-station variables (in `.env.compose`)

| Variable | Example | Notes |
|---|---|---|
| `STATIONn_SUPABASE_URL` | `https://xxx.supabase.co` | Supabase project URL |
| `STATIONn_SERVICE_KEY` | `sbp_…` | Supabase Service Role key — keep secret |
| `STATIONn_BUCKET` | `show-audio` | Storage bucket name (default: `show-audio`) |
| `STATIONn_ICE_HOST` | `radio.example.com` | Icecast server hostname |
| `STATIONn_ICE_PORT` | `8025` | Icecast source port |
| `STATIONn_ICE_USER` | `source` | Default Icecast source username |
| `STATIONn_ICE_PASS` | `hackme` | Default Icecast source password |
| `STATIONn_TZ` | `Europe/London` | Station timezone (IANA name) |
| `STATIONn_HOST_PORT` | `8080` | Port exposed on the Docker host |

### Full runner variable reference

These can be added under `environment:` in `docker-compose.yml` to fine-tune behaviour:

#### Core

| Key | Default | Purpose |
|---|---|---|
| `SUPABASE_URL` | required | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | required | Supabase service key |
| `SUPABASE_STORAGE_BUCKET` | `show-audio` | Bucket where audio files live |
| `ICE_HOST` | required | Icecast hostname |
| `ICE_PORT` | `8025` | Icecast port |
| `STATION_TZ` | `UTC` | Station timezone |
| `FFMPEG_PATH` | `ffmpeg` | Path to ffmpeg binary |
| `FFPROBE_PATH` | `ffprobe` | Path to ffprobe binary |

#### Scheduling & retries

| Key | Default | Purpose |
|---|---|---|
| `SAFETY_RESYNC_MS` | `300000` | Periodic rescan for upcoming jobs (ms) |
| `PREFETCH_MS` | `0` | Pre-fetch window before `run_at` |
| `PREEMPT_WAIT_MS` | `5000` | Wait after cancelling a contender |
| `RETRY_TOTAL_MS` | `180000` | Initial connect retry window |
| `RETRY_DELAY_MS` | `5000` | Delay between retries |
| `CONNECT_GRACE_MS` | `7000` | Consider connected after alive this long |
| `RECONNECT_DELAY_MS` | `2000` | Delay before mid-show reconnect |

#### Status & logging

| Key | Default | Purpose |
|---|---|---|
| `HEALTH_PORT` | `8080` | `/status` HTTP port |
| `LOG_DIR` | `/app/logs` | ffmpeg log directory (inside container) |
| `LAST_ERROR_LIMIT` | `5` | Last N errors exposed in `/status` |

#### Local copy mode

| Key | Default | Purpose |
|---|---|---|
| `ENABLE_LOCAL_COPY` | `true` | Download to disk before playing |
| `LOCAL_CACHE_DIR` | `/app/cache` | Cache directory (inside container) |
| `DOWNLOAD_RETRY_TOTAL_MS` | `600000` | Max time to download a file |
| `MAX_LOCAL_FILE_MB` | `2048` | Safety cap |
| `MIN_LOCAL_FILE_BYTES` | `65536` | Reject tiny or invalid cache downloads |

#### Icecast admin kick

| Key | Default | Purpose |
|---|---|---|
| `ENABLE_ICECAST_ADMIN_KICK` | `false` | Enable `/admin/killsource` calls |
| `ICE_ADMIN_PROTO` | `http` | Admin UI protocol |
| `ICE_ADMIN_HOST` | `ICE_HOST` | Admin hostname |
| `ICE_ADMIN_PORT` | `ICE_PORT` | Admin port |
| `ICE_ADMIN_USER` | | Admin username |
| `ICE_ADMIN_PASS` | | Admin password |
| `ADMIN_KICK_BEFORE_START` | `false` | Kick before first attempt |
| `ADMIN_KICK_ON_ATTEMPT` | `0` | Kick on attempt N |

#### Now Playing

| Key | Example | Notes |
|---|---|---|
| `NOWPLAYING_UPDATE_URL` | `https://…/nowplaying/update` | POST target |
| `NOWPLAYING_API_KEY` | `abc:xyz` | Passed in `X-API-Key` header |

#### EOF behavior

| Key | Default | Options |
|---|---|---|
| `EOF_BEHAVIOR` | `resume` | `resume` · `loop` · `stop` |

---

## Running directly (without Docker)

```bash
# Install deps
npm install

# Copy template and fill in secrets
cp .env.example .env
nano .env

# Run a single station
node runner.js

# Run both stations with PM2
pm2 start ecosystem.config.cjs
pm2 save
```

---

## How it works

1. **Bootstrap** — on start, fetches the next 24h of pending jobs and sets timers. Attaches realtime listeners to `jobs` and `schedules` for live cancellations.
2. **Start window** — when a show's start time arrives, preempts any running job on the same mount (DB cancel + optional Icecast kick).
3. **Input** — if local copy mode is on, ensures the file is downloaded to disk. Otherwise, creates a fresh signed URL for each attempt.
4. **ffprobe** — checks duration and warns if the file is shorter than the slot.
5. **Connect** — spawns ffmpeg with ICY metadata. Once alive longer than `CONNECT_GRACE_MS`, fires the Now Playing POST.
6. **During show** — monitors ffmpeg continuously; on drop, reconnects and resumes from the last known playhead.
7. **End of slot** — at `ends_at`, SIGTERMs ffmpeg and marks the schedule `completed`.

---

## Features

- TZ-aware scheduling (Luxon)
- Reconnect + resume using ffmpeg `-progress out_time` tracking
- Local cache mode (download → play local → cleanup)
- ffprobe duration preflight
- Preemption with optional Icecast admin kick
- Configurable EOF behavior: `resume` / `loop` / `stop`
- Now Playing POST on first connect
- `/status` JSON: live, upcoming, recent events, Icecast stats, last ffmpeg errors
- Structured file logs per attempt
- Self-healing realtime subscriptions with periodic resync

---

## Local copy mode

Enabled by default in the Docker image (`ENABLE_LOCAL_COPY=true`).

Downloads the show file to `/app/cache` before playback, so a Supabase signed URL expiry mid-show can never interrupt the stream. The file is deleted after the job ends.

---

## Reconnect & EOF behavior

On error or disconnection, the runner reconnects and resumes from the last known playhead.

On clean EOF (file ended before `ends_at`), behaviour is controlled by `EOF_BEHAVIOR`:

- `resume` (default) — re-open the file and seek to the last playhead position
- `loop` — restart from the beginning to fill the slot
- `stop` — end the slot immediately

---

## Icecast admin kick

If a stuck source is sometimes left on the mount, enable this to clear it before connecting:

```yaml
ENABLE_ICECAST_ADMIN_KICK: "true"
ICE_ADMIN_USER: admin
ICE_ADMIN_PASS: yourpassword
ADMIN_KICK_BEFORE_START: "true"
```

This calls `/admin/killsource?mount=/yourmount` before each job starts.

---

## Now Playing integration

Once connected, the runner sends a single POST per job:

```
POST  NOWPLAYING_UPDATE_URL
Headers: Content-Type: application/json
         X-API-Key: NOWPLAYING_API_KEY
Body:    {"artist":"<DJ Display Name>","title":"<Show Title>"}
```

This fires once per job even if reconnections occur.

---

## Status endpoint

`GET http://localhost:8080/status`

Query params:
- `events` — number of recent events to return (default 100, max 500)
- `hours` — upcoming show horizon in hours (default 24, max 168)

Response includes: `ok`, `runner`, `config`, `icecast`, `live`, `upcoming`, `recent_events`, `last_ffmpeg_errors`.

---

## Logging

- **Console** — major state changes and job lifecycle events
- **DB table `job_events`** — every notable step (powers `/status` → `recent_events`)
- **File logs** — per-attempt ffmpeg output at `LOG_DIR/ffmpeg-job<id>-<attempt>-<ts>.log`
- **`last_ffmpeg_errors`** in `/status` — rolling buffer of error lines from ffmpeg output

---

## Database schema

Minimal columns the runner reads/writes:

- **`jobs`** — `id`, `status`, `run_at`, `schedule_id`, `pid`
- **`schedules`** — `id`, `starts_at`, `ends_at`, `status`, `show_id`
- **`shows`** — `id`, `title`, `storage_path`, `dj_id`
- **`djs`** — `id`, `display_name`, `icecast_username`, `icecast_password_encrypted`, `icecast_mountpoint`
- **`job_events`** — `job_id`, `message`, `level`, timestamp column

Valid `schedules.status` values: `scheduled`, `pending`, `live`, `completed`, `cancelled`, `failed`.

> Tip: add an index on `job_events(job_id, created_at desc)` for faster status queries.

---

## Troubleshooting

**Container exits immediately**  
Check logs: `docker compose logs station1`. Usually a missing required env var (`SUPABASE_URL`, `ICE_HOST`, etc.).

**`ffmpeg ended normally before ends_at`**  
The file is shorter than the slot. Check `last_ffmpeg_errors` in `/status`. Set `EOF_BEHAVIOR=loop` to fill the slot regardless.

**`HTTP 400 Bad Request` opening input**  
Likely an expired signed URL. Local copy mode (on by default) avoids this — confirm `ENABLE_LOCAL_COPY=true`.

**Mount is busy / `403 Forbidden` from Icecast**  
Enable admin kick (`ENABLE_ICECAST_ADMIN_KICK=true`) or increase `RETRY_TOTAL_MS`.

**`/status` returns nothing**  
Confirm `HEALTH_PORT=8080` and the host port mapping in `docker-compose.yml`.

**Realtime not updating**  
Check `/status.realtime`. The runner also resyncs periodically via `SAFETY_RESYNC_MS`.

---

## Security checklist

- Never commit `.env.compose` (it's in `.gitignore`) — only commit `.env.compose.example`
- Rotate Supabase service keys if ever exposed
- Keep `NOWPLAYING_API_KEY` and Icecast admin credentials out of source control
- Keep the GitHub repo private if it contains `.env.station*` files with real credentials
