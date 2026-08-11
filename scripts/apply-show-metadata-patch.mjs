import { readFileSync, writeFileSync } from 'node:fs';

const runnerPath = new URL('../runner.js', import.meta.url);
let source = readFileSync(runnerPath, 'utf8');

function replaceOnce(label, from, to) {
  const parts = source.split(from);
  if (parts.length !== 2) {
    throw new Error(`${label}: expected exactly one match, found ${parts.length - 1}`);
  }
  source = parts[0] + to + parts[1];
}

replaceOnce(
  'job metadata query',
`        show:shows (
          id, title, storage_path,
          dj:djs (
            id,
            display_name,
            icecast_username,
            icecast_password_encrypted,
            icecast_mountpoint
          )
        )`,
`        show:shows (
          id, title, storage_path,
          dj:djs (
            id,
            display_name,
            profile_picture_url,
            azuracast_streamer_id,
            icecast_username,
            icecast_password_encrypted,
            icecast_mountpoint
          )
        )`
);

replaceOnce(
  'now playing helper',
`async function postNowPlayingUpdate({ jobId, artist, title }) {
  if (!NOWPLAYING_UPDATE_URL || !NOWPLAYING_API_KEY) {
    await logEvent(jobId, \`nowplaying: skipped (missing NOWPLAYING_UPDATE_URL or NOWPLAYING_API_KEY)\`);
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
      await logEvent(jobId, \`nowplaying: HTTP \${res.status} while updating "\${title}" — \${artist}\`, 'error');
      return;
    }
    await logEvent(jobId, \`nowplaying: updated → title="\${title}" | artist="\${artist}" (key=\${maskKey(NOWPLAYING_API_KEY)})\`);
  } catch (e) {
    await logEvent(jobId, \`nowplaying error: \${e.message}\`, 'error');
  }
}`,
`async function postNowPlayingUpdate({ jobId, artist, title }) {
  if (!NOWPLAYING_UPDATE_URL || !NOWPLAYING_API_KEY) {
    await logEvent(jobId, \`nowplaying: skipped (missing NOWPLAYING_UPDATE_URL or NOWPLAYING_API_KEY)\`);
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
      await logEvent(jobId, \`nowplaying: HTTP \${res.status} while updating "\${title}" — \${artist}\`, 'error');
      return;
    }
    await logEvent(jobId, \`nowplaying: updated → artist="\${artist}" | title="\${title}" (key=\${maskKey(NOWPLAYING_API_KEY)})\`);
  } catch (e) {
    await logEvent(jobId, \`nowplaying error: \${e.message}\`, 'error');
  }
}

const syncedStreamerArtwork = new Set();

function getStreamerArtworkUpdateUrl(streamerId) {
  if (!NOWPLAYING_UPDATE_URL || !streamerId) return null;
  try {
    const url = new URL(NOWPLAYING_UPDATE_URL);
    const nextPath = url.pathname.replace(
      /\\/nowplaying\\/update\\/?$/,
      \`/streamer/\${streamerId}/art\`
    );
    if (nextPath === url.pathname) return null;
    url.pathname = nextPath;
    url.search = '';
    return url.toString();
  } catch {
    return null;
  }
}

async function syncStreamerArtwork({ jobId, streamerId, profilePictureUrl }) {
  if (!streamerId || !profilePictureUrl || !NOWPLAYING_API_KEY) return;

  const cacheKey = \`\${streamerId}:\${profilePictureUrl}\`;
  if (syncedStreamerArtwork.has(cacheKey)) return;

  const artworkUrl = getStreamerArtworkUpdateUrl(streamerId);
  if (!artworkUrl) {
    await logEvent(jobId, 'streamer artwork: skipped (could not derive AzuraCast artwork URL)', 'error');
    return;
  }

  try {
    const imageRes = await fetch(profilePictureUrl);
    if (!imageRes.ok) throw new Error(\`profile image HTTP \${imageRes.status}\`);

    const contentType = (imageRes.headers.get('content-type') || '').split(';')[0].trim().toLowerCase();
    if (!['image/jpeg', 'image/png'].includes(contentType)) {
      throw new Error(\`unsupported image type \${contentType || 'unknown'}\`);
    }

    const bytes = await imageRes.arrayBuffer();
    const ext = contentType === 'image/png' ? 'png' : 'jpg';
    const form = new FormData();
    form.append(
      'file_data',
      new Blob([bytes], { type: contentType }),
      \`streamer-\${streamerId}.\${ext}\`
    );

    const res = await fetch(artworkUrl, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'X-API-Key': NOWPLAYING_API_KEY
      },
      body: form
    });

    if (!res.ok) {
      const body = (await res.text()).slice(0, 240);
      throw new Error(\`AzuraCast artwork HTTP \${res.status}: \${body}\`);
    }

    syncedStreamerArtwork.add(cacheKey);
    await logEvent(jobId, \`streamer artwork: synced DJ profile image to AzuraCast streamer \${streamerId}\`);
  } catch (e) {
    await logEvent(jobId, \`streamer artwork sync failed: \${e.message}\`, 'error');
  }
}`
);

replaceOnce(
  'ffmpeg metadata normalization',
`  streamTitle, artistName,
  seekSeconds = 0
}) {
  const outUrl = \`icecast://\${encodeURIComponent(iceUser)}:\${encodeURIComponent(icePass)}@\${ICE_HOST}:\${ICE_PORT}\${mount}\`;
  const title  = streamTitle || 'Scheduled Show';
  const artist = artistName || '';
  const icyTitle = artist ? \`\${title} - \${artist}\` : title;`,
`  streamTitle, artistName,
  seekSeconds = 0
}) {
  const outUrl = \`icecast://\${encodeURIComponent(iceUser)}:\${encodeURIComponent(icePass)}@\${ICE_HOST}:\${ICE_PORT}\${mount}\`;
  const showName = String(streamTitle || 'Scheduled Show').replace(/\\s+/g, ' ').trim();
  const djName = String(artistName || '').replace(/\\s+/g, ' ').trim();
  const icyTitle = djName ? \`\${showName} - \${djName}\` : showName;`
);

replaceOnce(
  'ffmpeg metadata fields',
`    '-metadata', \`title=\${title}\`,
    ...(artist ? ['-metadata', \`artist=\${artist}\`] : []),
    '-metadata', \`streamtitle=\${icyTitle}\`,
    '-metadata', \`streamurl=https://\${ICE_HOST}\`,
    '-vn', '-content_type', 'audio/mpeg',
    '-ice_name', title,`,
`    '-metadata', \`title=\${showName}\`,
    ...(djName ? ['-metadata', \`artist=\${djName}\`] : []),
    '-metadata', \`streamtitle=\${icyTitle}\`,
    '-metadata', \`streamurl=https://\${ICE_HOST}\`,
    '-vn', '-content_type', 'audio/mpeg',
    '-ice_name', showName,`
);

replaceOnce(
  'play reconnect signature',
`  mount, streamTitle, artistName,
  endsAtUtc, originalSlotSeconds`,
`  mount, streamTitle, artistName,
  streamerId, profilePictureUrl,
  endsAtUtc, originalSlotSeconds`
);

replaceOnce(
  'connected now playing update',
`    if (!playShowWithReconnect._postedNowPlaying?.[jobId]) {
      await postNowPlayingUpdate({ jobId, artist: artistName || '', title: streamTitle || '' });
      playShowWithReconnect._postedNowPlaying = playShowWithReconnect._postedNowPlaying || {};
      playShowWithReconnect._postedNowPlaying[jobId] = true;
    }`,
`    if (!playShowWithReconnect._postedNowPlaying?.[jobId]) {
      await syncStreamerArtwork({ jobId, streamerId, profilePictureUrl });
      await postNowPlayingUpdate({
        jobId,
        artist: artistName || '',
        title: streamTitle || artistName || ''
      });
      playShowWithReconnect._postedNowPlaying = playShowWithReconnect._postedNowPlaying || {};
      playShowWithReconnect._postedNowPlaying[jobId] = true;
    }`
);

replaceOnce(
  'connected live transition',
`    activeProcs.set(jobId, proc);
    await setJobStatus(jobId, 'running', proc.pid);
    await logEvent(jobId, \`ffmpeg pid=\${proc.pid} (attempt \${attempt}, seek=\${seekSeconds}s)\`);`,
`    activeProcs.set(jobId, proc);
    await setJobStatus(jobId, 'running', proc.pid);
    await setScheduleStatus(schedId, 'live');
    await logEvent(jobId, \`ffmpeg pid=\${proc.pid} (attempt \${attempt}, seek=\${seekSeconds}s)\`);`
);

replaceOnce(
  'remove premature live transition',
`  // Mark running/live
  await setJobStatus(jobId, 'running');
  await setScheduleStatus(sched.id, 'live');

`,
`  // claimJob keeps the job in "starting". It becomes running/live only after
  // ffmpeg survives the connection grace period and has a real PID.

`
);

replaceOnce(
  'play reconnect call',
`    mount, streamTitle, artistName,
    endsAtUtc: endUtc, originalSlotSeconds`,
`    mount, streamTitle, artistName,
    streamerId: dj.azuracast_streamer_id,
    profilePictureUrl: dj.profile_picture_url,
    endsAtUtc: endUtc, originalSlotSeconds`
);

writeFileSync(runnerPath, source);
console.log('Applied ShowRunner title/DJ metadata and DJ artwork patch.');
