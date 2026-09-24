// runner.js — Realtime Show Runner
// TZ-aware scheduling, preemption, robust reconnect+resume, optional local-copy mode,
// ffprobe duration checks, Icecast admin kick, JSON status, ffmpeg log files & last 5 errors,
// Now Playing POST update after stream connects, and static docs at /docs.
// Requirements: Node 24+, ffmpeg, (optional) ffprobe, luxon, @supabase/supabase-js, dotenv

import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { spawn, spawnSync } from 'node:child_process';
import { DateTime } from 'luxon';
import http from 'node:http';
import path from 'node:path';
import { createWriteStream } from 'node:fs';
import { promises as fsp } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable, Transform } from 'node:stream';

// ===== ENV =====
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_ROLE_KEY;
const BUCKET       = process.env.SUPABASE_STORAGE_BUCKET || 'public';

const ICE_HOST = process.env.ICE_HOST;
const ICE_PORT = process.env.ICE_PORT || '8025';
const FFMPEG   = process.env.FFMPEG_PATH || 'ffmpeg';
const FFPROBE  = process.env.FFPROBE_PATH || 'ffprobe';

const STATION_TZ = process.env.STATION_TZ || 'Europe/London';

const SAFETY_RESYNC_MS   = Number(process.env.SAFETY_RESYNC_MS || 60_000);
const PREFETCH_MS        = Number(process.env.PREFETCH_MS || 0);
const PREEMPT_WAIT_MS    = Number(process.env.PREEMPT_WAIT_MS || 5000);
const PREEMPT_POLL_MS    = Number(process.env.PREEMPT_POLL_MS || 500);

// Initial connect retries
const RETRY_TOTAL_MS     = Number(process.env.RETRY_TOTAL_MS || 180_000);
const RETRY_DELAY_MS     = Number(process.env.RETRY_DELAY_MS || 5_000);
const CONNECT_GRACE_MS   = Number(process.env.CONNECT_GRACE_MS || 7_000);

// Mid-show reconnect loop
const RECONNECT_DELAY_MS = Number(process.env.RECONNECT_DELAY_MS || 2_000);

// Status endpoint
const HEALTH_PORT = Number(process.env.HEALTH_PORT || 0);

// Logging
const LOG_DIR = process.env.LOG_DIR || './logs';
const LAST_ERROR_LIMIT = Number(process.env.LAST_ERROR_LIMIT || 5);

// EOF behavior when ffmpeg exits cleanly **before** ends_at: 'resume' | 'loop' | 'stop'
const EOF_BEHAVIOR = (process.env.EOF_BEHAVIOR || 'resume').toLowerCase();

// Supabase signed URL TTL (seconds) for remote streaming/download
const SIGNED_URL_TTL_SECS = Number(process.env.SIGNED_URL_TTL_SECS || 3600);

// Local copy mode (download → play from disk → delete)
const ENABLE_LOCAL_COPY = String(process.env.ENABLE_LOCAL_COPY || 'false').toLowerCase() === 'true';
const LOCAL_CACHE_DIR = process.env.LOCAL_CACHE_DIR || '/tmp/radio-cache';
const DOWNLOAD_RETRY_TOTAL_MS = Number(process.env.DOWNLOAD_RETRY_TOTAL_MS || 600_000);
const DOWNLOAD_RETRY_DELAY_MS = Number(process.env.DOWNLOAD_RETRY_DELAY_MS || 5_000);
const MAX_LOCAL_FILE_MB = Number(process.env.MAX_LOCAL_FILE_MB || 2048);
const MIN_LOCAL_FILE_BYTES = Number(process.env.MIN_LOCAL_FILE_BYTES || 65_536);

// Controlled handover between scheduled shows
const HANDOVER_GAP_MS = Number(process.env.HANDOVER_GAP_MS || 2_000);
const FADE_OUT_MS = Number(process.env.FADE_OUT_MS || 2_000);
const FORCE_KILL_AFTER_MS = Number(process.env.FORCE_KILL_AFTER_MS || 1_000);

// ffprobe duration check
const ENABLE_FFPROBE_DURATION_CHECK = String(process.env.ENABLE_FFPROBE_DURATION_CHECK || 'true').toLowerCase() === 'true';
const DURATION_WARN_PAD_SECS = Number(process.env.DURATION_WARN_PAD_SECS || 5);

// Icecast admin kick (hard preemption)
const ENABLE_ICECAST_ADMIN_KICK = String(process.env.ENABLE_ICECAST_ADMIN_KICK || 'false').toLowerCase() === 'true';
const ICE_ADMIN_PROTO = process.env.ICE_ADMIN_PROTO || 'http';
const ICE_ADMIN_HOST  = process.env.ICE_ADMIN_HOST  || ICE_HOST;
const ICE_ADMIN_PORT  = process.env.ICE_ADMIN_PORT  || ICE_PORT;
const ICE_ADMIN_USER  = process.env.ICE_ADMIN_USER  || '';
const ICE_ADMIN_PASS  = process.env.ICE_ADMIN_PASS  || '';
const ADMIN_KICK_BEFORE_START = String(process.env.ADMIN_KICK_BEFORE_START || 'false').toLowerCase() === 'true';
const ADMIN_KICK_ON_ATTEMPT = Number(process.env.ADMIN_KICK_ON_ATTEMPT || 0); // 0=disabled

// Default/fallback source credentials
const DEFAULT_SOURCE_USER = process.env.ICE_DEFAULT_SOURCE_USER || null;
const DEFAULT_SOURCE_PASS = process.env.ICE_DEFAULT_SOURCE_PASS || null;

// Failover threshold for 401 Unauthorized (switch to default after N 401s)
const AUTH_401_FAILOVER_THRESHOLD = Number(process.env.AUTH_401_FAILOVER_THRESHOLD || 3);

// Now Playing POST
const NOWPLAYING_UPDATE_URL = process.env.NOWPLAYING_UPDATE_URL || '';
const NOWPLAYING_API_KEY    = process.env.NOWPLAYING_API_KEY || '';

// ---- Docs serving (optional) ----
const DOCS_DIR = process.env.DOCS_DIR || path.resolve('./docs');
const MIME = {
  '.html':'text/html; charset=utf-8',
  '.css':'text/css; charset=utf-8',
  '.js':'application/javascript; charset=utf-8',
  '.json':'application/json; charset=utf-8',
  '.png':'image/png',
  '.jpg':'image/jpeg',
  '.jpeg':'image/jpeg',
  '.svg':'image/svg+xml',
  '.ico':'image/x-icon',
  '.txt':'text/plain; charset=utf-8'
};

// ===== Supabase =====
if (!SUPABASE_URL || !SERVICE_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SERVICE_KEY, { db: { schema: 'public' } });

// ===== Preflight =====
let ffprobeAvailable = false;
(function preflightFFmpeg() {
  const res = spawnSync(FFMPEG, ['-version'], { stdio: 'ignore' });
  if (res.status !== 0) {
    console.error(`❌ Could not run "${FFMPEG}". Install ffmpeg or set FFMPEG_PATH.`);
    process.exit(1);
  }
})();
(function preflightFFprobe() {
  const res = spawnSync(FFPROBE, ['-version'], { stdio: 'ignore' });
  if (res.status !== 0) {
    console.warn(`⚠️ Could not run "${FFPROBE}". Duration checks will be skipped unless available.`);
  } else {
    ffprobeAvailable = true;
  }
})();

// ===== Time helpers =====
function parseDbTime(value) {
  if (!value) return null;
  const s = String(value);
  const hasOffset = /[zZ]|[+\-]\d{2}:?\d{2}$/.test(s);
  if (hasOffset) {
    const dt = DateTime.fromISO(s, { setZone: true });
    return dt.isValid ? dt.toUTC() : null;
  } else {
    const local = DateTime.fromISO(s, { zone: STATION_TZ });
    return local.isValid ? local.toUTC() : null;
  }
}
function millisBetween(aIso, bIso) { const a = parseDbTime(aIso), b = parseDbTime(bIso); if (!a || !b) return 0; return Math.max(0, Math.floor(b.toMillis() - a.toMillis())); }
function secondsBetween(aIso, bIso) { return Math.max(1, Math.floor(millisBetween(aIso, bIso) / 1000)); }
function delayFromNowMs(targetIso) { const t = parseDbTime(targetIso); if (!t) return 0; return Math.max(0, t.toMillis() - DateTime.utc().toMillis()); }
function fmtLocal(dtUtc) { return dtUtc.setZone(STATION_TZ).toFormat(`yyyy-LL-dd HH:mm:ss '( ${STATION_TZ} )'`); }
function fmtUtc(dtUtc)   { return dtUtc.toFormat(`yyyy-LL-dd HH:mm:ss '(UTC)'`); }

// ===== Misc helpers =====
function decrypt(encrypted) { return encrypted; }
function normalizeStoragePath(bucket, raw) {
  if (!raw) return raw;
  let p = raw.trim();
  if (p.startsWith('/')) p = p.slice(1);
  if (p.startsWith(bucket + '/')) p = p.slice(bucket.length + 1);
  return p;
}
async function logEvent(jobId, message, level = 'info') {
  const tag = jobId ? `[job ${jobId}]` : '';
  console.log(`${tag} ${message}`);
  try { await supabase.from('job_events').insert({ job_id: jobId, message, level }); } catch {}
}

// ===== DB =====
async function getJobById(jobId) {
  const { data, error } = await supabase
    .from('jobs')
    .select(`
      id, status, run_at, schedule_id, pid,
      schedule:schedules (
        id, starts_at, ends_at, status,
        show:shows (
          id, title, storage_path,
          dj:djs (
            id,
            display_name,
            icecast_username,
            icecast_password_encrypted,
            icecast_mountpoint
          )
        )
      )
    `)
    .eq('id', jobId)
    .single();
  if (error) throw error;
  return data;
}
async function claimJob(jobId) {
  const { data, error } = await supabase
    .from('jobs')
    .update({ status: 'starting' })
    .eq('id', jobId)
    .eq('status', 'pending')
    .select('id')
    .single();
  if (error && error.code !== 'PGRST116') throw error;
  return !!data;
}
async function setJobStatus(jobId, status, pid) {
  const patch = pid ? { status, pid } : { status };
  const { error } = await supabase.from('jobs').update(patch).eq('id', jobId);
  if (error) throw error;
}
async function setScheduleStatus(schedId, status) {
  const { error } = await supabase.from('schedules').update({ status }).eq('id', schedId);
  if (error) throw error;
}
async function getJobStatus(jobId) {
  const { data, error } = await supabase.from('jobs').select('status').eq('id', jobId).single();
  if (error) throw error;
  return data.status;
}
async function getScheduleStatus(schedId) {
  const { data, error } = await supabase.from('schedules').select('status').eq('id', schedId).single();
  if (error) throw error;
  return data.status;
}
async function findRunningJobsOnMount(excludeJobId, mount) {
  const { data, error } = await supabase
    .from('jobs')
    .select(`
      id, status, pid, schedule_id,
      schedule:schedules ( id, show:shows ( id, dj:djs ( icecast_mountpoint ) ) )
    `)
    .in('status', ['running', 'starting'])
    .neq('id', excludeJobId);
  if (error) throw error;
  return (data || []).filter(j => (j?.schedule?.show?.dj?.icecast_mountpoint || '/live') === mount);
}

// ===== Supabase storage =====
async function createSignedDownloadUrl(storagePath) {
  if (typeof storagePath === 'string' && storagePath.startsWith('http')) return storagePath;
  const key = normalizeStoragePath(BUCKET, storagePath);
  if (!key) throw new Error('Empty storage_path');

  const { data, error } = await supabase
    .storage
    .from(BUCKET)
    .createSignedUrl(key, SIGNED_URL_TTL_SECS);

  if (error) throw error;
  return data.signedUrl.startsWith('http')
    ? data.signedUrl
    : `${SUPABASE_URL}/storage/v1${data.signedUrl}`;
}
async function head(url) {
  try {
    const res = await fetch(url, { method: 'HEAD' });
    return { ok: res.ok, status: res.status, contentType: res.headers.get('content-type') };
  } catch (e) {
    return { ok: false, status: 0, err: e.message };
  }
}

// ===== Local download helpers =====
const localFileCache = new Map(); // jobId -> { path, bytes, completed: bool }
const localDownloadPromises = new Map(); // jobId -> in-flight download promise
async function ensureCacheDir() {
  try { await fsp.mkdir(LOCAL_CACHE_DIR, { recursive: true }); }
  catch (e) { console.error('Failed to create LOCAL_CACHE_DIR:', e?.message || e); }
}
async function removeLocalFile(jobId) {
  const rec = localFileCache.get(jobId);
  if (!rec?.path) return;
  try { await fsp.unlink(rec.path); } catch {}
  localFileCache.delete(jobId);
}
process.on('exit', () => {
  for (const [jobId] of localFileCache) {
    try { fsp.unlink(localFileCache.get(jobId).path); } catch {}
  }
});
function safeBasenameFromStoragePath(storagePath, fallback = 'show.mp3') {
  try {
    const raw = String(storagePath || '');
    const name = raw.split('/').pop() || fallback;
    return name.replace(/[^\w.\-]+/g, '_');
  } catch { return fallback; }
}
async function downloadShowToLocal({ jobId, storagePath }) {
  const cached = localFileCache.get(jobId);
  if (cached?.completed && cached.path) {
    try {
      const stat = await fsp.stat(cached.path);
      if (stat.isFile() && stat.size === cached.bytes && stat.size >= MIN_LOCAL_FILE_BYTES) {
        return cached.path;
      }
    } catch {}
    localFileCache.delete(jobId);
  }

  const inFlight = localDownloadPromises.get(jobId);
  if (inFlight) return inFlight;

  const downloadPromise = (async () => {
    const deadline = Date.now() + DOWNLOAD_RETRY_TOTAL_MS;
    const fileName = `${jobId}-${safeBasenameFromStoragePath(storagePath)}`;
    const localPath = path.join(LOCAL_CACHE_DIR, fileName);

    while (Date.now() < deadline) {
      const tempPath = `${localPath}.part-${process.pid}-${Date.now()}`;
      try {
        const url = await createSignedDownloadUrl(storagePath);
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        if (!res.body) throw new Error('download response had no body');

        const contentType = String(res.headers.get('content-type') || '').toLowerCase();
        if (contentType.includes('application/json') || contentType.includes('text/html') || contentType.includes('text/plain')) {
          throw new Error(`unexpected content-type ${contentType || 'unknown'}`);
        }

        const contentLen = Number(res.headers.get('content-length') || 0);
        if (MAX_LOCAL_FILE_MB > 0 && contentLen > 0 && contentLen > MAX_LOCAL_FILE_MB * 1024 * 1024) {
          throw new Error(`file too large (${contentLen} bytes > cap ${MAX_LOCAL_FILE_MB}MB)`);
        }

        let bytes = 0;
        const counter = new Transform({ transform(chunk, _enc, cb) { bytes += chunk.length; cb(null, chunk); } });
        await fsp.mkdir(LOCAL_CACHE_DIR, { recursive: true });
        const out = createWriteStream(tempPath, { flags: 'wx' });

        await pipeline(Readable.fromWeb(res.body), counter, out);

        if (contentLen > 0 && bytes !== contentLen) {
          throw new Error(`incomplete download (${bytes}/${contentLen} bytes)`);
        }
        if (bytes < MIN_LOCAL_FILE_BYTES) {
          throw new Error(`download too small to be a show audio file (${bytes} bytes; minimum ${MIN_LOCAL_FILE_BYTES})`);
        }

        if (ENABLE_FFPROBE_DURATION_CHECK && ffprobeAvailable) {
          const duration = probeDurationSecondsSync(tempPath);
          if (duration == null || duration <= 0) {
            throw new Error('downloaded file failed ffprobe validation');
          }
        }

        await fsp.rename(tempPath, localPath);
        localFileCache.set(jobId, { path: localPath, bytes, completed: true });
        await logEvent(jobId, `Downloaded and validated local audio: ${localPath} (${bytes} bytes)`);
        return localPath;
      } catch (e) {
        try { await fsp.unlink(tempPath); } catch {}
        await logEvent(jobId, `Download error: ${e.message} — retrying`, 'error');
        await new Promise(r => setTimeout(r, DOWNLOAD_RETRY_DELAY_MS));
      }
    }
    throw new Error(`Download timed out after ${Math.floor(DOWNLOAD_RETRY_TOTAL_MS/1000)}s`);
  })();

  localDownloadPromises.set(jobId, downloadPromise);
  try {
    return await downloadPromise;
  } finally {
    if (localDownloadPromises.get(jobId) === downloadPromise) {
      localDownloadPromises.delete(jobId);
    }
  }
}

// ===== ffprobe (duration) =====
function trimFloat(s) { const n = Number(String(s).trim()); return isFinite(n) ? n : null; }
function formatSecs(sec) {
  const s = Math.max(0, Math.floor(sec));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  return `${h.toString().padStart(2,'0')}:${m.toString().padStart(2,'0')}:${r.toString().padStart(2,'0')}`;
}
function probeDurationSecondsSync(input) {
  try {
    const args = ['-v','error','-show_entries','format=duration','-of','default=noprint_wrappers=1:nokey=1', input];
    const res = spawnSync(FFPROBE, args, { encoding: 'utf8' });
    if (res.status !== 0) return null;
    const dur = trimFloat(res.stdout);
    return dur != null ? dur : null;
  } catch { return null; }
}

// ===== Icecast admin kick =====
async function adminKickMount(mount, reason = 'runner preemption') {
  if (!ENABLE_ICECAST_ADMIN_KICK) return { ok: false, skipped: true, reason: 'disabled' };
  if (!ICE_ADMIN_USER || !ICE_ADMIN_PASS) return { ok: false, skipped: true, reason: 'no credentials' };
  const url = `${ICE_ADMIN_PROTO}://${ICE_ADMIN_HOST}:${ICE_ADMIN_PORT}/admin/killsource?mount=${encodeURIComponent(mount)}`;
  const hdr = 'Basic ' + Buffer.from(`${ICE_ADMIN_USER}:${ICE_ADMIN_PASS}`).toString('base64');
  try {
    const res = await fetch(url, { headers: { 'Authorization': hdr } });
    return { ok: res.ok, status: res.status };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ===== Progress parsing (for resume) =====
function parseOutTimeSeconds(line) {
  const m = /^out_time=(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?/.exec(line);
  if (!m) return null;
  const h = Number(m[1]), mi = Number(m[2]), s = Number(m[3]);
  const frac = m[4] ? Number(`0.${m[4]}`) : 0;
  return h * 3600 + mi * 60 + s + frac;
}

// ===== Now Playing helper =====
function maskKey(k) {
  if (!k) return '';
  if (k.length <= 8) return '****';
  return k.slice(0,4) + '…' + k.slice(-4);
}
async function postNowPlayingUpdate({ jobId, artist, title }) {
  if (!NOWPLAYING_UPDATE_URL || !NOWPLAYING_API_KEY) {
    await logEvent(jobId, `nowplaying: skipped (missing NOWPLAYING_UPDATE_URL or NOWPLAYING_API_KEY)`);
    return;
  }
  try {
    const res = await fetch(NOWPLAYING_UPDATE_URL, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-API-Key': NOWPLAYING_API_KEY
      },
      body: JSON.stringify({ artist, title })
    });
    if (!res.ok) {
      await logEvent(jobId, `nowplaying: HTTP ${res.status} while updating "${title}" — ${artist}`, 'error');
      return;
    }
    await logEvent(jobId, `nowplaying: updated → title="${title}" | artist="${artist}" (key=${maskKey(NOWPLAYING_API_KEY)})`);
  } catch (e) {
    await logEvent(jobId, `nowplaying error: ${e.message}`, 'error');
  }
}

// ===== ffmpeg args =====
function buildFfmpegArgs({
  slotSeconds,
  fileUrl,
  iceUser, icePass, mount,
  streamTitle, artistName,
  seekSeconds = 0
}) {
  const outUrl = `icecast://${encodeURIComponent(iceUser)}:${encodeURIComponent(icePass)}@${ICE_HOST}:${ICE_PORT}${mount}`;
  const title  = streamTitle || 'Scheduled Show';
  const artist = artistName || '';
  const icyTitle = artist ? `${title} - ${artist}` : title;

  const inputOpts = [];
  if (seekSeconds > 0) inputOpts.push('-ss', String(Math.max(0, Math.floor(seekSeconds))));
  if (/^https?:\/\//i.test(fileUrl)) {
    inputOpts.push(
      '-reconnect', '1',
      '-reconnect_streamed', '1',
      '-reconnect_on_network_error', '1',
      '-reconnect_delay_max', '2'
    );
  }

  const fadeSeconds = Math.max(0, Math.min(Number(slotSeconds), FADE_OUT_MS / 1000));
  const fadeStartSeconds = Math.max(0, Number(slotSeconds) - fadeSeconds);
  const audioFilters = fadeSeconds > 0
    ? ['-af', `afade=t=out:st=${fadeStartSeconds.toFixed(3)}:d=${fadeSeconds.toFixed(3)}`]
    : [];

  return [
    '-hide_banner', '-nostats', '-loglevel', 'info',
    '-re',
    '-progress', 'pipe:1',
    '-t', String(Math.max(1, Math.floor(slotSeconds))),
    ...inputOpts,
    '-i', fileUrl,
    ...audioFilters,
    '-c:a', 'libmp3lame', '-b:a', '192k', '-ar', '44100', '-ac', '2',
    '-metadata', `title=${title}`,
    ...(artist ? ['-metadata', `artist=${artist}`] : []),
    '-metadata', `streamtitle=${icyTitle}`,
    '-metadata', `streamurl=https://${ICE_HOST}`,
    '-vn', '-content_type', 'audio/mpeg',
    '-ice_name', title,
    '-ice_genre', 'House',
    '-ice_public', '1',
    '-f', 'mp3',
    outUrl
  ];
}

// ===== Scheduler state & logging =====
const startTimers = new Map();
const prefetchTimers = new Map();
const signedUrlCache = new Map();
const activeProcs = new Map();
const cancelledJobs = new Set(); // in-memory cancellation flags, avoids per-second DB polling

let lastBootstrapAt = 0;
const rtStates = { 'jobs-rt': { state: 'INIT', at: null }, 'schedules-rt': { state: 'INIT', at: null } };

const lastFfmpegErrors = [];
function pushLastError(entry) {
  lastFfmpegErrors.push(entry);
  if (lastFfmpegErrors.length > LAST_ERROR_LIMIT) {
    lastFfmpegErrors.splice(0, lastFfmpegErrors.length - LAST_ERROR_LIMIT);
  }
}
async function ensureLogDir() { try { await fsp.mkdir(LOG_DIR, { recursive: true }); } catch (e) { console.error('Failed to create LOG_DIR:', e?.message || e); } }
function makeLogPath(jobId, attemptTag) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  return path.join(LOG_DIR, `ffmpeg-job${jobId}-${attemptTag}-${ts}.log`);
}
function shouldFlagAsError(line) {
  return /error|failed|failure|refused|denied|timed out|not found|unreachable|broken|invalid|disconnect|reset|403|401|404|500|502|503|504/i.test(line);
}
function attachFfmpegLogging(proc, { jobId, attemptTag }) {
  const logPath = makeLogPath(jobId, attemptTag);
  const stream = createWriteStream(logPath, { flags: 'a' });
  const write = (buf) => {
    const text = buf.toString();
    stream.write(text);
    for (const line of text.split(/\r?\n/)) {
      if (!line) continue;
      if (shouldFlagAsError(line)) {
        const msg = line.trim().slice(0, 500);
        pushLastError({ ts: new Date().toISOString(), job_id: jobId, attempt: attemptTag, message: msg, log: path.basename(logPath) });
        logEvent(jobId, msg, 'error').catch(() => {});
      }
    }
  };
  proc.stderr.on('data', write);
  proc.stdout.on('data', write);
  proc.once('exit', () => stream.end());
  proc.once('error', () => stream.end());
  return logPath;
}

function terminateFfmpeg(proc, { jobId = null, reason = 'stop requested' } = {}) {
  if (!proc || proc.exitCode !== null) return;
  try { proc.kill('SIGTERM'); } catch { return; }

  const hardKill = setTimeout(() => {
    if (proc.exitCode !== null) return;
    const message = `ffmpeg did not exit after SIGTERM (${reason}); sending SIGKILL`;
    if (jobId) logEvent(jobId, message, 'error').catch(() => {});
    else console.error(message);
    try { proc.kill('SIGKILL'); } catch {}
  }, Math.max(0, FORCE_KILL_AFTER_MS));
  hardKill.unref?.();
  proc.once('exit', () => clearTimeout(hardKill));
}

function clearTimers(jobId) {
  const t = startTimers.get(jobId);
  if (t) { clearTimeout(t); startTimers.delete(jobId); }
  const p = prefetchTimers.get(jobId);
  if (p) { clearTimeout(p); prefetchTimers.delete(jobId); }
}
function schedulePrefetch(jobId, runAtIso, storagePath) {
  if (!PREFETCH_MS || !storagePath) return;
  if (ENABLE_LOCAL_COPY && localFileCache.get(jobId)?.completed) return;
  const delay = Math.max(0, delayFromNowMs(runAtIso) - PREFETCH_MS);
  const pt = setTimeout(async () => {
    prefetchTimers.delete(jobId);
    try {
      if (ENABLE_LOCAL_COPY) {
        await ensureCacheDir();
        const pth = await downloadShowToLocal({ jobId, storagePath });
        await logEvent(jobId, `Prefetch local OK: ${pth}`);
      } else {
        const url = await createSignedDownloadUrl(storagePath);
        signedUrlCache.set(jobId, url);
        await logEvent(jobId, `Prefetched signed URL.`);
      }
    } catch (e) {
      await logEvent(jobId, `Prefetch failed: ${e?.message || e}`, 'error');
    }
  }, delay);
  prefetchTimers.set(jobId, pt);
}
function scheduleJob(job, storagePathForPrefetch) {
  clearTimers(job.id);
  const intendedDelay = delayFromNowMs(job.run_at);
  const delay = Math.max(0, intendedDelay);

  const runAtUtc = parseDbTime(job.run_at);
  if (runAtUtc) {
    console.log(`📅 Scheduling job ${job.id}: ${fmtLocal(runAtUtc)}  =  ${fmtUtc(runAtUtc)}`);
  }

  if (PREFETCH_MS) schedulePrefetch(job.id, job.run_at, storagePathForPrefetch);
  const t = setTimeout(() => runJobById(job.id).catch(e => console.error('start timer error:', e?.message || e)), delay);
  startTimers.set(job.id, t);
  console.log(`⏱️ will start in ${Math.round(delay/1000)}s`);
}

// ===== Preemption =====
async function preemptExisting(jobId, mount) {
  const contenders = await findRunningJobsOnMount(jobId, mount);
  if (!contenders.length) return;

  console.log(`⚠️ Preemption: found ${contenders.length} running/starting job(s) on ${mount}. Cancelling…`);
  await Promise.all(contenders.map(async j => {
    try {
      await setJobStatus(j.id, 'cancelled');
      if (j.schedule_id) { try { await setScheduleStatus(j.schedule_id, 'cancelled'); } catch {} }
      const local = activeProcs.get(j.id);
      if (local) terminateFfmpeg(local, { jobId: j.id, reason: 'preempted by next show' });
    } catch (e) { console.error(`Preempt cancel failed for job ${j.id}:`, e?.message || e); }
  }));

  const deadline = Date.now() + PREEMPT_WAIT_MS;
  let stillRunning = contenders.length;
  while (Date.now() < deadline) {
    const check = await findRunningJobsOnMount(jobId, mount);
    stillRunning = check.length;
    if (stillRunning === 0) break;
    await new Promise(r => setTimeout(r, PREEMPT_POLL_MS));
  }

  // Optional admin kick to clear a real live source
  if (ENABLE_ICECAST_ADMIN_KICK) {
    const res = await adminKickMount(mount, 'pre-start cleanup');
    if (res.ok) console.log(`🧹 Icecast admin kicked "${mount}" (status ${res.status})`);
    else console.log(`(kick skipped/failed)`, res);
  }

  if (stillRunning === 0) console.log(`✅ Preemption complete: mount ${mount} is clear.`);
  else console.log(`⏳ Preemption timed out after ${PREEMPT_WAIT_MS}ms; proceeding to start.`);
}

// ===== Cancel watcher =====
// Primary path: realtime subscription fires killActiveProc() immediately.
// Fallback poll (10s) catches cancellations when realtime is down.
function killActiveProc(jobId, schedId, showTitle, djName) {
  cancelledJobs.add(jobId);
  const proc = activeProcs.get(jobId);
  if (!proc) return;
  console.log(`🛑 CANCEL REQUESTED — "${showTitle}" by ${djName} (job ${jobId})`);
  logEvent(jobId, `Cancellation requested — stopping ffmpeg`, 'info').catch(() => {});
  terminateFfmpeg(proc, { jobId, reason: 'job cancelled' });
}

function startCancelWatcher(jobId, proc, schedId, showTitle, djName) {
  const interval = setInterval(async () => {
    try {
      if (cancelledJobs.has(jobId)) { clearInterval(interval); return; }
      const [jobStatus, schedStatus] = await Promise.all([ getJobStatus(jobId).catch(()=>null), getScheduleStatus(schedId).catch(()=>null) ]);
      if (jobStatus === 'cancelled' || schedStatus === 'cancelled') {
        killActiveProc(jobId, schedId, showTitle, djName);
        clearInterval(interval);
        try { await setJobStatus(jobId, 'cancelled'); } catch {}
        try { await setScheduleStatus(schedId, 'cancelled'); } catch {}
      }
    } catch (e) { console.error('cancel watcher error:', e?.message || e); }
  }, 10000); // reduced from 1s — realtime handles immediate kills
  proc.on('exit', () => clearInterval(interval));
}

// ===== Start ffmpeg with retries (attempts until grace or early exit)
// If we detect 401 Unauthorized during stderr, we throw an Error { code: 'AUTH_401' } immediately,
// so the caller can switch credentials.
async function startFfmpegWithRetries({ jobId, schedId, args, streamTitle, artistName, mount }) {
  const deadline = Date.now() + RETRY_TOTAL_MS;
  let attempt = 0, lastErrMsg = '';

  while (true) {
    attempt += 1;

    if (ENABLE_ICECAST_ADMIN_KICK) {
      if (ADMIN_KICK_BEFORE_START && attempt === 1) {
        await logEvent(jobId, `Admin kick before first attempt on ${mount}.`);
        try { await adminKickMount(mount, 'before-start'); } catch {}
      }
      if (ADMIN_KICK_ON_ATTEMPT > 0 && attempt === ADMIN_KICK_ON_ATTEMPT) {
        await logEvent(jobId, `Admin kick on attempt ${attempt} for ${mount}.`);
        try { await adminKickMount(mount, `retry-${attempt}`); } catch {}
      }
    }

    if (cancelledJobs.has(jobId)) throw new Error('Cancelled before start');

    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error(`Unable to connect within retry window (${Math.floor(RETRY_TOTAL_MS/1000)}s). Last error: ${lastErrMsg}`);

    await logEvent(jobId, `Attempt ${attempt}: starting ffmpeg (remaining ~${Math.ceil(remaining/1000)}s)…`);
    const proc = spawn(FFMPEG, args, { stdio: ['ignore','pipe','pipe'] });

    attachFfmpegLogging(proc, { jobId, attemptTag: `start-${attempt}` });

    // Watch early stderr for auth failures
    let saw401 = false;
    const authListener = (buf) => {
      const s = buf.toString();
      if (/401|Unauthorized|authorization failed/i.test(s)) saw401 = true;
    };
    proc.stderr.on('data', authListener);

    const connectedP = new Promise((resolve) => {
      const t = setTimeout(() => resolve({ ok: true }), CONNECT_GRACE_MS);
      proc.once('exit', (code, signal) => { clearTimeout(t); resolve({ ok: false, earlyExit: true, code, signal }); });
      proc.once('error', (err) => { clearTimeout(t); resolve({ ok: false, error: err }); });
    });

    const result = await connectedP;

    // Remove listener (avoid leaks)
    try { proc.stderr.off('data', authListener); } catch {}

    if (result.ok) {
      await logEvent(jobId, `Connected to Icecast (ffmpeg running).`);
      return { proc };
    }

    // Early exit: check if this was an auth failure
    if (saw401) {
      const err = new Error('401 Unauthorized');
      err.code = 'AUTH_401';
      throw err;
    }

    lastErrMsg = result.error?.message || `code=${result.code} signal=${result.signal ?? 'none'}`;
    await logEvent(jobId, `Connection failed early: ${lastErrMsg}`, 'error');

    const stillTime = deadline - Date.now();
    if (stillTime <= 0) throw new Error(`Unable to connect within retry window. Last error: ${lastErrMsg}`);
    await new Promise(r => setTimeout(r, Math.min(RETRY_DELAY_MS, stillTime)));
  }
}

// ===== Streaming loop with robust resume + 401→default failover =====
async function playShowWithReconnect({
  jobId, schedId, storagePath,
  iceUser, icePass,                 // active creds
  fallbackUser, fallbackPass,       // default creds (for failover after 401 threshold)
  mount, streamTitle, artistName,
  endsAtUtc, originalSlotSeconds
}) {
  // Absolute seconds played since start of source across all attempts
  let absPlayed = 0;
  let attempt = 0;

  // Current/active credentials (start with whatever caller chose)
  let currentUser = iceUser;
  let currentPass = icePass;
  let usingFallback = false;
  let auth401Count = 0;

  const haveFallback = !!(fallbackUser && fallbackPass);
  const isSameAsFallback = (u, p) => u === fallbackUser && p === fallbackPass;

  while (true) {
    // Cancel/time checks
    if (cancelledJobs.has(jobId)) return { outcome: 'cancelled' };

    const now = DateTime.utc();
    const remainingSec = Math.floor((endsAtUtc.toMillis() - now.toMillis()) / 1000);
    if (remainingSec <= 0) return { outcome: 'completed' };

    const boundedRemaining = Math.min(remainingSec, originalSlotSeconds);

    // Decide the input: local file if available, else fresh signed URL
    let inputForFfmpeg;
    const localRec = localFileCache.get(jobId);
    if (ENABLE_LOCAL_COPY && localRec?.completed && localRec.path) {
      inputForFfmpeg = localRec.path;
    } else {
      try {
        inputForFfmpeg = await createSignedDownloadUrl(storagePath);
      } catch (e) {
        await logEvent(jobId, `Signed URL error (attempt ${attempt+1}): ${e.message}`, 'error');
        await new Promise(r => setTimeout(r, RECONNECT_DELAY_MS));
        continue;
      }
      const headRes = await head(inputForFfmpeg);
      if (!headRes.ok) {
        await logEvent(jobId, `HEAD failed for fresh URL (status=${headRes.status}) — will retry`, 'error');
        await new Promise(r => setTimeout(r, RECONNECT_DELAY_MS));
        continue;
      }
    }

    attempt += 1;
    await logEvent(jobId, `Reconnect/attempt ${attempt}: absPlayed≈${Math.floor(absPlayed)}s, remaining≈${boundedRemaining}s (creds=${usingFallback ? 'DEFAULT' : 'ACTIVE'})`);

    const seekSeconds = Math.max(0, Math.floor(absPlayed > 1 ? absPlayed - 1 : 0)); // 1s overlap
    const args = buildFfmpegArgs({
      slotSeconds: boundedRemaining,
      fileUrl: inputForFfmpeg,
      iceUser: currentUser, icePass: currentPass,
      mount, streamTitle, artistName,
      seekSeconds
    });

    let proc;
    try {
      const started = await startFfmpegWithRetries({ jobId, schedId, args, streamTitle, artistName, mount });
      proc = started.proc;
      auth401Count = 0; // reset on success
    } catch (e) {
      const msg = String(e?.message || '');
      const is401 = e?.code === 'AUTH_401' || /401|Unauthorized/i.test(msg);

      if (is401) {
        auth401Count += 1;
        await logEvent(jobId, `401 Unauthorized (#${auth401Count}) with ${usingFallback ? 'DEFAULT' : 'ACTIVE'} credentials`, 'error');

        // If we've hit threshold and we have defaults (and aren't already using them), switch.
        if (
          AUTH_401_FAILOVER_THRESHOLD > 0 &&
          auth401Count >= AUTH_401_FAILOVER_THRESHOLD &&
          haveFallback &&
          !usingFallback &&
          !isSameAsFallback(currentUser, currentPass)
        ) {
          usingFallback = true;
          currentUser = fallbackUser;
          currentPass = fallbackPass;
          await logEvent(jobId, `Switching to DEFAULT credentials after ${AUTH_401_FAILOVER_THRESHOLD}× 401.`, 'error');
        }
      } else {
        await logEvent(jobId, `Reconnect window failed: ${msg}`, 'error');
      }

      await new Promise(r => setTimeout(r, RECONNECT_DELAY_MS));
      continue;
    }

    // Post "now playing" once after we successfully connect the first time
    if (!playShowWithReconnect._postedNowPlaying?.[jobId]) {
      await postNowPlayingUpdate({ jobId, artist: artistName || '', title: streamTitle || '' });
      playShowWithReconnect._postedNowPlaying = playShowWithReconnect._postedNowPlaying || {};
      playShowWithReconnect._postedNowPlaying[jobId] = true;
    }

    // Track per-attempt progress so we can accumulate into absPlayed
    let outTimeThisAttempt = 0;

    activeProcs.set(jobId, proc);
    await setJobStatus(jobId, 'running', proc.pid);
    await logEvent(jobId, `ffmpeg pid=${proc.pid} (attempt ${attempt}, seek=${seekSeconds}s)`);

    const killMs = Math.max(0, endsAtUtc.toMillis() - DateTime.utc().toMillis());
    const watchdog = setTimeout(
      () => terminateFfmpeg(proc, { jobId, reason: 'scheduled handover cutoff' }),
      killMs
    );

    startCancelWatcher(jobId, proc, schedId, streamTitle, artistName || 'DJ');

    let lastLog = 0;
    proc.stderr.on('data', async (buf) => {
      const n = Date.now();
      if (n - lastLog >= 1200) {
        lastLog = n;
        const s = buf.toString();
        // Only write to DB for errors; routine ffmpeg progress goes to the disk log only
        if (shouldFlagAsError(s)) {
          await logEvent(jobId, s.length > 400 ? s.slice(0, 400) + '…' : s, 'error');
        }
      }
    });
    proc.stdout.on('data', async (buf) => {
      const lines = buf.toString().split(/\r?\n/);
      for (const line of lines) {
        const t = parseOutTimeSeconds(line);
        if (t != null) outTimeThisAttempt = t; // per-attempt processed duration
      }
    });

    const exitResult = await new Promise((resolve) => {
      proc.once('exit', (code, signal) => resolve({ code, signal }));
      proc.once('error', (err) => resolve({ error: err }));
    });

    clearTimeout(watchdog);
    activeProcs.delete(jobId);

    // Accumulate absolute progress (add per-attempt processed duration)
    absPlayed += Math.max(0, outTimeThisAttempt || 0);

    const now2 = DateTime.utc();
    const remAfter = Math.floor((endsAtUtc.toMillis() - now2.toMillis()) / 1000);
    if (cancelledJobs.has(jobId)) return { outcome: 'cancelled' };
    if (remAfter <= 0) return { outcome: 'completed' };

    // Handle clean early exit (code 0) BEFORE ends_at
    if (!exitResult.error && exitResult.code === 0) {
      if (EOF_BEHAVIOR === 'loop') {
        await logEvent(jobId, `ffmpeg exited cleanly with ~${remAfter}s remaining — looping from start.`);
        absPlayed = 0;
        await new Promise(r => setTimeout(r, RECONNECT_DELAY_MS));
        continue;
      } else if (EOF_BEHAVIOR === 'stop') {
        await logEvent(jobId, 'ffmpeg ended normally before ends_at — finishing slot early (EOF_BEHAVIOR=stop).');
        return { outcome: 'completed' };
      } else {
        await logEvent(jobId, `ffmpeg ended cleanly with ~${remAfter}s remaining — resuming from absPlayed≈${Math.floor(absPlayed)}s (EOF_BEHAVIOR=resume).`);
        await new Promise(r => setTimeout(r, RECONNECT_DELAY_MS));
        continue;
      }
    }

    // Otherwise unexpected error or signal: attempt resume
    const reason = exitResult?.error?.message || `code=${exitResult.code} signal=${exitResult.signal ?? 'none'}`;
    await logEvent(jobId, `ffmpeg ended unexpectedly: ${reason} — will attempt resume in ${RECONNECT_DELAY_MS}ms`, 'error');
    await new Promise(r => setTimeout(r, RECONNECT_DELAY_MS));
  }
}

// ===== Core job execution =====
async function runJobById(jobId) {
  const job = await getJobById(jobId);
  if (!job || job.status !== 'pending') return;

  const gotIt = await claimJob(jobId);
  if (!gotIt) return;

  const sched = job.schedule;
  const show  = sched?.show;
  const dj    = show?.dj;

  if (!sched || !show || !dj) {
    await logEvent(jobId, 'Malformed job (missing schedule/show/dj)', 'error');
    await setJobStatus(jobId, 'failed');
    try { await setScheduleStatus(sched?.id, 'failed'); } catch {}
    return;
  }

  if ((await getScheduleStatus(sched.id)) === 'cancelled') {
    await logEvent(jobId, 'Schedule already cancelled — aborting job', 'info');
    await setJobStatus(jobId, 'cancelled');
    return;
  }

  const originalSlotSeconds = secondsBetween(sched.starts_at, sched.ends_at);
  const startUtc = parseDbTime(sched.starts_at);
  const endUtc   = parseDbTime(sched.ends_at);
  if (!startUtc || !endUtc || endUtc.toMillis() <= startUtc.toMillis()) {
    await logEvent(jobId, 'Invalid schedule window — starts_at/ends_at are missing or reversed', 'error');
    await setJobStatus(jobId, 'failed');
    try { await setScheduleStatus(sched.id, 'failed'); } catch {}
    return;
  }

  const maxGapMs = Math.max(0, endUtc.toMillis() - startUtc.toMillis() - 1_000);
  const appliedGapMs = Math.min(Math.max(0, HANDOVER_GAP_MS), maxGapMs);
  const playbackEndUtc = endUtc.minus({ milliseconds: appliedGapMs });
  const playbackSlotSeconds = Math.max(1, Math.floor(
    (playbackEndUtc.toMillis() - startUtc.toMillis()) / 1000
  ));

  const line = `⏰ Slot window: ${fmtLocal(startUtc)} → ${fmtLocal(endUtc)} | playback stops at ${fmtLocal(playbackEndUtc)} | fade=${FADE_OUT_MS}ms gap=${appliedGapMs}ms`;
  console.log(line);
  await logEvent(jobId, line);

  const streamTitle = show.title || `${dj.display_name} Show`;
  const artistName  = dj.display_name || '';

  const mount = dj.icecast_mountpoint || '/live';

  // Credential selection:
  // - If DJ has both username and password, start with theirs.
  // - If DJ password is null/empty, start with DEFAULT env creds.
  const djUser = dj.icecast_username || null;
  const djPass = decrypt(dj.icecast_password_encrypted) || null;

  let iceUser, icePass, credSource;
  if (djUser && djPass) {
    iceUser = djUser;
    icePass = djPass;
    credSource = 'dj';
  } else {
    iceUser = DEFAULT_SOURCE_USER;
    icePass = DEFAULT_SOURCE_PASS;
    credSource = 'default';
  }

  if (!iceUser || !icePass) {
    await logEvent(jobId, 'No usable Icecast credentials (DJ pass missing and no defaults configured).', 'error');
    await setJobStatus(jobId, 'failed');
    try { await setScheduleStatus(sched.id, 'failed'); } catch {}
    return;
  }

  await logEvent(jobId, `Using ${credSource === 'dj' ? 'DJ' : 'DEFAULT'} credentials for "${dj.display_name}" on ${mount}`);

  await logEvent(jobId, `Claimed. OrigSlot=${originalSlotSeconds}s | PlaySlot=${playbackSlotSeconds}s | DJ=${dj.display_name} | mount=${mount}`);
  await logEvent(jobId, `Preparing source: bucket=${BUCKET}, path=${show.storage_path}`);

  await preemptExisting(jobId, mount);
  await logEvent(jobId, `Mount may be busy (e.g., live DJ). Will retry connection for up to ${Math.floor(RETRY_TOTAL_MS/1000)}s if needed.`, 'info');

  // If not using local copy, do a quick early HEAD to fail fast
  if (!ENABLE_LOCAL_COPY) {
    let firstUrl;
    try { firstUrl = signedUrlCache.get(jobId) || await createSignedDownloadUrl(show.storage_path); }
    catch (e) {
      await logEvent(jobId, `Signed URL error: ${e.message}`, 'error');
      await setJobStatus(jobId, 'failed');
      try { await setScheduleStatus(sched.id, 'failed'); } catch {}
      return;
    }
    const headRes = await head(firstUrl);
    if (!headRes.ok) {
      await logEvent(jobId, `HEAD failed: status=${headRes.status} err=${headRes.err||''}`, 'error');
      await setJobStatus(jobId, 'failed');
      try { await setScheduleStatus(sched.id, 'failed'); } catch {}
      return;
    }
    await logEvent(jobId, `Source OK (status ${headRes.status}, type ${headRes.contentType || 'unknown'})`);
  }

  // Mark running/live
  await setJobStatus(jobId, 'running');
  await setScheduleStatus(sched.id, 'live');

  // If local copy mode and not already prefetched, try to download now (non-fatal)
  if (ENABLE_LOCAL_COPY && !localFileCache.get(jobId)?.completed) {
    try {
      await ensureCacheDir();
      await downloadShowToLocal({ jobId, storagePath: show.storage_path });
    } catch (e) {
      await logEvent(jobId, `Local download at start failed (${e.message}) — will stream remote this attempt`, 'error');
    }
  }

  // ffprobe duration check / warn
  if (ENABLE_FFPROBE_DURATION_CHECK) {
    try {
      let probeInput = null;
      const localRec = localFileCache.get(jobId);
      if (ENABLE_LOCAL_COPY && localRec?.completed && localRec.path) {
        probeInput = localRec.path;
      } else {
        probeInput = await createSignedDownloadUrl(show.storage_path);
      }
      const dur = probeDurationSecondsSync(probeInput);
      if (dur != null) {
        const slot = playbackSlotSeconds;
        if (dur + DURATION_WARN_PAD_SECS < slot) {
          await logEvent(jobId,
            `⚠️ ffprobe: media duration ${formatSecs(dur)} < slot ${formatSecs(slot)} (by ~${formatSecs(slot - dur)}) — will ${EOF_BEHAVIOR === 'loop' ? 'loop' : EOF_BEHAVIOR === 'stop' ? 'end early' : 'attempt resume'} if needed.`,
            'error'
          );
        } else {
          await logEvent(jobId, `ffprobe: media duration ${formatSecs(dur)} (slot ${formatSecs(slot)})`);
        }
      } else {
        await logEvent(jobId, `ffprobe: could not determine duration (non-fatal)`);
      }
    } catch (e) {
      await logEvent(jobId, `ffprobe error: ${e.message} (non-fatal)`, 'error');
    }
  }

  const liveBanner = `🎙️ LIVE (auto-reconnect/resume${ENABLE_LOCAL_COPY ? ', local-copy' : ''}, ${FADE_OUT_MS}ms fade, ${appliedGapMs}ms handover gap) — "${streamTitle}" by ${artistName || 'DJ'} on ${ICE_HOST}:${ICE_PORT}${mount} (job ${jobId}) @ ${fmtLocal(startUtc)} until ${fmtLocal(playbackEndUtc)}`;
  console.log(liveBanner);
  await logEvent(jobId, liveBanner);

  const outcome = await playShowWithReconnect({
    jobId, schedId: sched.id, storagePath: show.storage_path,
    iceUser, icePass,
    fallbackUser: DEFAULT_SOURCE_USER, fallbackPass: DEFAULT_SOURCE_PASS,
    mount, streamTitle, artistName,
    endsAtUtc: playbackEndUtc, originalSlotSeconds: playbackSlotSeconds
  });

  // Cleanup cache
  signedUrlCache.delete(jobId);
  await removeLocalFile(jobId);
  if (playShowWithReconnect._postedNowPlaying) delete playShowWithReconnect._postedNowPlaying[jobId];

  // (Optional) clear/replace now playing at end:
  // await postNowPlayingUpdate({ jobId, artist: '', title: '' });

  if (outcome.outcome === 'cancelled') {
    await setJobStatus(jobId, 'cancelled');
    await setScheduleStatus(sched.id, 'cancelled');
    await logEvent(jobId, `⏹️ CANCELLED — "${streamTitle}"`, 'info');
  } else if (outcome.outcome === 'completed') {
    await setJobStatus(jobId, 'ended');
    await setScheduleStatus(sched.id, 'completed');
    await logEvent(jobId, `🛑 ENDED — "${streamTitle}"`, 'info');
  } else {
    await setJobStatus(jobId, 'failed');
    try { await setScheduleStatus(sched.id, 'failed'); } catch {}
    await logEvent(jobId, `❌ FAILED — "${streamTitle}"`, 'error');
  }
}

// ===== Bootstrap & Realtime =====
async function bootstrapUpcoming() {
  const nowMinus = DateTime.utc().minus({ seconds: 15 }).toISO();
  const untilIso = DateTime.utc().plus({ days: 1 }).toISO();

  const { data, error } = await supabase
    .from('jobs')
    .select(`
      id, run_at, status, schedule_id,
      schedule:schedules ( id, show:shows ( storage_path ) )
    `)
    .eq('status','pending')
    .gte('run_at', nowMinus)
    .lte('run_at', untilIso)
    .order('run_at',{ascending:true})
    .limit(1000);

  if (error) { console.error('bootstrapUpcoming error:', error.message); return; }
  (data||[]).forEach(j => {
    const storagePath = j?.schedule?.show?.storage_path || null;
    scheduleJob(j, storagePath);
  });
  if (!data?.length) console.log('No upcoming jobs to schedule.');
  lastBootstrapAt = Date.now();
}

function resilientSubscribe({ name, table, onInsert, onUpdate }) {
  let channel;
  const join = () => {
    if (channel) supabase.removeChannel(channel);
    channel = supabase.channel(name)
      .on('postgres_changes', { event:'INSERT', schema:'public', table }, onInsert)
      .on('postgres_changes', { event:'UPDATE', schema:'public', table }, onUpdate)
      .subscribe((status) => {
        rtStates[name] = { state: status, at: new Date().toISOString() };
        if (status === 'SUBSCRIBED') console.log(`${name} subscribed`);
        else if (status === 'TIMED_OUT') console.warn(`${name} timed out — retrying`);
        else if (status === 'ERROR') console.error(`${name} error — retrying`);
        if (status === 'TIMED_OUT' || status === 'CLOSED' || status === 'ERROR') {
          const wait = status === 'TIMED_OUT' ? 1000 : 3000;
          setTimeout(() => join(), wait);
        }
      });
  };
  join();
}

function subscribeRealtime() {
  resilientSubscribe({
    name: 'jobs-rt',
    table: 'jobs',
    onInsert: (payload) => { const j = payload.new; if (j.status === 'pending') scheduleJob(j); },
    onUpdate: (payload) => {
      const j = payload.new;
      if (j.status === 'pending') scheduleJob(j);
      else if (j.status === 'cancelled') {
        clearTimers(j.id);
        cancelledJobs.add(j.id);
        const proc = activeProcs.get(j.id);
        if (proc) terminateFfmpeg(proc, { jobId: j.id, reason: 'realtime cancellation' });
      }
    }
  });

  resilientSubscribe({
    name: 'schedules-rt',
    table: 'schedules',
    onInsert: () => {},
    onUpdate: async (payload) => {
      const s = payload.new;
      if (s.status === 'cancelled') {
        try {
          const { data } = await supabase
            .from('jobs')
            .select('id,status')
            .eq('schedule_id', s.id)
            .in('status', ['pending','starting']);
          (data||[]).forEach(j => clearTimers(j.id));
        } catch {}
      }
    }
  });
}

// ===== Status endpoint (with /docs static serving) =====
async function gatherStatus({ eventsLimit = 100, upcomingHours = 24 } = {}) {
  const [live, upcoming, events, ice] = await Promise.all([
    getLiveJobs().catch(e => ({ error: e.message })),
    getUpcoming(upcomingHours).catch(e => ({ error: e.message })),
    getRecentEvents(eventsLimit).catch(e => ({ error: e.message })),
    fetchIcecastStatus()
  ]);
  const active = [];
  for (const [jobId, proc] of activeProcs.entries()) active.push({ job_id: jobId, pid: proc.pid });

  return {
    ok: true,
    runner: { pid: process.pid, tz: STATION_TZ, now: new Date().toISOString(), supabase_url: SUPABASE_URL, bucket: BUCKET },
    config: {
      SAFETY_RESYNC_MS, PREFETCH_MS, PREEMPT_WAIT_MS, PREEMPT_POLL_MS,
      RETRY_TOTAL_MS, RETRY_DELAY_MS, CONNECT_GRACE_MS, RECONNECT_DELAY_MS,
      EOF_BEHAVIOR, ENABLE_LOCAL_COPY, LOCAL_CACHE_DIR, SIGNED_URL_TTL_SECS,
      DOWNLOAD_RETRY_TOTAL_MS, DOWNLOAD_RETRY_DELAY_MS, MAX_LOCAL_FILE_MB, MIN_LOCAL_FILE_BYTES,
      HANDOVER_GAP_MS, FADE_OUT_MS, FORCE_KILL_AFTER_MS,
      AUTH_401_FAILOVER_THRESHOLD
    },
    timers: { start_timers: startTimers.size, prefetch_timers: prefetchTimers.size, active_procs: activeProcs.size, last_bootstrap_at: lastBootstrapAt ? new Date(lastBootstrapAt).toISOString() : null },
    realtime: rtStates,
    icecast: { host: ICE_HOST, port: ICE_PORT, admin_host: ICE_ADMIN_HOST, admin_port: ICE_ADMIN_PORT, reachable: !!ice.ok, status: ice.ok ? ice.status : { error: ice.error || `HTTP ${ice.status}` } },
    active_processes: active,
    live: Array.isArray(live) ? live : { error: live.error },
    upcoming: Array.isArray(upcoming) ? upcoming : { error: upcoming.error },
    recent_events: Array.isArray(events) ? events : { error: events.error },
    recent_events_order_col: EVENTS_ORDER_COL,
    last_ffmpeg_errors: lastFfmpegErrors
  };
}

let statusCache = { ts: 0, data: null };
const STATUS_CACHE_TTL_MS = 5000;

function startHealthServer() {
  if (!HEALTH_PORT) return;
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url || '/', `http://localhost:${HEALTH_PORT}`);
      const p = url.pathname;

      // ---- Serve /docs and /docs/* from DOCS_DIR (path traversal safe) ----
      if (p === '/docs' || p.startsWith('/docs/')) {
        const rel = decodeURIComponent(p.replace(/^\/docs\/?/, ''));
        const fileRel = rel === '' || rel.endsWith('/') ? 'index.html' : rel;
        const fullPath = path.normalize(path.join(DOCS_DIR, fileRel));

        // prevent path traversal outside DOCS_DIR
        if (!fullPath.startsWith(path.normalize(DOCS_DIR + path.sep))) {
          res.writeHead(403, { 'content-type': 'text/plain; charset=utf-8' });
          return res.end('Forbidden');
        }
        try {
          const data = await fsp.readFile(fullPath);
          const ext = path.extname(fullPath).toLowerCase();
          const type = MIME[ext] || 'application/octet-stream';
          res.writeHead(200, { 'content-type': type });
          return res.end(data);
        } catch (e) {
          const code = (e && e.code) || '';
          res.writeHead(code === 'ENOENT' ? 404 : 500, { 'content-type': 'text/plain; charset=utf-8' });
          return res.end(code === 'ENOENT' ? 'Not found' : `Error reading file: ${e.message}`);
        }
      }

      // ---- JSON status ----
      if (p.startsWith('/status') || p === '/') {
        const eventsLimit = Math.min(500, Math.max(0, Number(url.searchParams.get('events') || 100)));
        const upcomingHours = Math.min(168, Math.max(1, Number(url.searchParams.get('hours') || 24)));
        if (!statusCache.data || Date.now() - statusCache.ts > STATUS_CACHE_TTL_MS) {
          statusCache.data = await gatherStatus({ eventsLimit, upcomingHours });
          statusCache.ts = Date.now();
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        return res.end(JSON.stringify(statusCache.data));
      }

      // ---- Fallback 404 ----
      res.writeHead(404, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: 'Not found' }));
    } catch (e) {
      res.writeHead(500, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: e.message }));
    }
  });
  server.listen(HEALTH_PORT, () => {
    console.log(`🩺 Status at http://localhost:${HEALTH_PORT}/status`);
    console.log(`📘 Docs   at http://localhost:${HEALTH_PORT}/docs`);
  });
}

// ===== Icecast status + live/upcoming/events helpers for status =====
async function fetchIcecastStatus() {
  try {
    const url = `${ICE_ADMIN_PROTO}://${ICE_ADMIN_HOST}:${ICE_ADMIN_PORT}/status-json.xsl`;
    const headers = {};

    if (ICE_ADMIN_USER || ICE_ADMIN_PASS) {
      const credentials = Buffer.from(`${ICE_ADMIN_USER}:${ICE_ADMIN_PASS}`).toString('base64');
      headers.Authorization = `Basic ${credentials}`;
    }

    const res = await fetch(url, { headers });
    if (!res.ok) return { ok: false, status: res.status };

    const json = await res.json();
    return { ok: true, status: json.icestats || json };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}
function mapSafeDj(dj) { return dj ? { id: dj.id, display_name: dj.display_name, icecast_mountpoint: dj.icecast_mountpoint || '/live' } : null; }
function mapSafeShow(show) { return show ? { id: show.id, title: show.title } : null; }
function mapSafeSchedule(s) { return s ? { id: s.id, starts_at: s.starts_at, ends_at: s.ends_at, status: s.status } : null; }
function withShowTimes(row) {
  const startsUtc = parseDbTime(row?.schedule?.starts_at);
  const endsUtc   = parseDbTime(row?.schedule?.ends_at);
  return {
    ...row,
    show_start_utc:  startsUtc ? startsUtc.toISO() : null,
    show_start_local: startsUtc ? startsUtc.setZone(STATION_TZ).toISO() : null,
    show_end_utc:    endsUtc ? endsUtc.toISO() : null,
    show_end_local:  endsUtc ? endsUtc.setZone(STATION_TZ).toISO() : null,
    seconds_until_end: endsUtc ? Math.max(0, Math.floor((endsUtc.toMillis() - DateTime.utc().toMillis()) / 1000)) : null
  };
}
async function getLiveJobs() {
  const { data, error } = await supabase
    .from('jobs')
    .select(`
      id, status, pid, run_at, schedule_id,
      schedule:schedules ( id, starts_at, ends_at, status,
        show:shows ( id, title, dj:djs ( id, display_name, icecast_mountpoint ) ) )
    `)
    .in('status', ['running','starting'])
    .order('run_at', { ascending: true });
  if (error) throw error;
  return (data || [])
    .map(j => ({
      id: j.id, status: j.status, pid: j.pid, run_at: j.run_at,
      schedule: mapSafeSchedule(j.schedule),
      show: mapSafeShow(j?.schedule?.show),
      dj: mapSafeDj(j?.schedule?.show?.dj)
    }))
    .map(withShowTimes);
}
async function getUpcoming(hours = 24) {
  const now = DateTime.utc();
  const until = now.plus({ hours }).toISO();
  const { data, error } = await supabase
    .from('jobs')
    .select(`
      id, run_at, status, schedule_id,
      schedule:schedules ( id, starts_at, ends_at, status,
        show:shows ( id, title, dj:djs ( id, display_name, icecast_mountpoint ) ) )
    `)
    .eq('status','pending')
    .gte('run_at', now.minus({ seconds: 15 }).toISO())
    .lte('run_at', until)
    .order('run_at', { ascending: true })
    .limit(200);
  if (error) throw error;
  return (data || [])
    .map(j => ({
      id: j.id, status: j.status, run_at: j.run_at,
      schedule: mapSafeSchedule(j.schedule),
      show: mapSafeShow(j?.schedule?.show),
      dj: mapSafeDj(j?.schedule?.show?.dj)
    }))
    .map(withShowTimes);
}

// Recent events: auto-detect a timestamp/order column
let EVENTS_ORDER_COL = null;
async function detectEventsOrderColumn() {
  if (EVENTS_ORDER_COL) return EVENTS_ORDER_COL;
  const candidates = ['created_at','inserted_at','createdAt','timestamp','event_at','ts','time','created','id'];
  for (const col of candidates) {
    const { error } = await supabase.from('job_events').select(col).order(col, { ascending: false }).limit(1);
    if (!error) { EVENTS_ORDER_COL = col; return col; }
  }
  EVENTS_ORDER_COL = 'id';
  return 'id';
}
async function getRecentEvents(limit = 100) {
  const col = await detectEventsOrderColumn();
  const selectCols = ['level', 'job_id', 'message', col].join(', ');
  const { data, error } = await supabase
    .from('job_events')
    .select(selectCols)
    .order(col, { ascending: false })
    .limit(limit);
  if (error) throw error;
  return (data || []).map(r => ({ ts: r[col] ?? null, level: r.level, job_id: r.job_id, message: r.message }));
}

// ===== Signals =====
for (const sig of ['SIGTERM','SIGINT']) {
  process.on(sig, () => {
    for (const [,p] of activeProcs) { try { p.kill('SIGTERM'); } catch {} }
    process.exit(0);
  });
}

// ===== Main =====
async function main() {
  if (!ICE_HOST) { console.error('❌ Missing ICE_HOST'); process.exit(1); }
  await ensureLogDir();
  if (ENABLE_LOCAL_COPY) await ensureCacheDir();

  console.log(`ShowRunner (TZ=${STATION_TZ}) | FFmpeg="${FFMPEG}"`);
  console.log(`Supabase: ${SUPABASE_URL}  |  Bucket: ${BUCKET}`);
  console.log(`Icecast:  ${ICE_HOST}:${ICE_PORT}`);
  if (ENABLE_LOCAL_COPY) console.log(`Local copy: ENABLED @ ${LOCAL_CACHE_DIR}`);
  if (HEALTH_PORT) {
    console.log(`Status: http://localhost:${HEALTH_PORT}/status`);
    console.log(`Docs:   http://localhost:${HEALTH_PORT}/docs\n`);
  }

  await bootstrapUpcoming();
  subscribeRealtime();
  startHealthServer();

  setInterval(() => { bootstrapUpcoming().catch(()=>{}); }, SAFETY_RESYNC_MS);
}
main();


