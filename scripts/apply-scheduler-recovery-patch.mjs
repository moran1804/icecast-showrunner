import { readFileSync, writeFileSync } from 'node:fs';

const runnerPath = new URL('../runner.js', import.meta.url);
let s = readFileSync(runnerPath, 'utf8');

function sub(label, re, replacement) {
  if (!re.test(s)) throw new Error(`${label}: pattern not found`);
  s = s.replace(re, replacement);
}

sub('scheduler maps',
  /const startTimers = new Map\(\);\nconst prefetchTimers = new Map\(\);/,
  `const startTimers = new Map();\nconst prefetchTimers = new Map();\nconst scheduledRunAt = new Map();\nconst lateStartOffsets = new Map();`
);

sub('clear timer state',
  /function clearTimers\(jobId\) \{([\s\S]*?)\n\}/,
  (m, body) => `function clearTimers(jobId) {${body}\n  scheduledRunAt.delete(jobId);\n}`
);

sub('schedule job',
  /function scheduleJob\(job, storagePathForPrefetch\) \{[\s\S]*?\n\}\n\n\/\/ ===== Preemption =====/,
`function scheduleJob(job, storagePathForPrefetch) {
  if (startTimers.has(job.id) && scheduledRunAt.get(job.id) === job.run_at) return;
  clearTimers(job.id);

  const intendedDelay = delayFromNowMs(job.run_at);
  const delay = Math.max(0, intendedDelay + 1000);
  const runAtUtc = parseDbTime(job.run_at);
  if (runAtUtc) console.log(\`📅 Scheduling job \${job.id}: \${fmtLocal(runAtUtc.plus({ seconds: 1 }))}\`);

  if (PREFETCH_MS && !localFileCache.get(job.id)?.completed) {
    schedulePrefetch(job.id, job.run_at, storagePathForPrefetch);
  }

  const t = setTimeout(() => {
    startTimers.delete(job.id);
    scheduledRunAt.delete(job.id);
    runJobById(job.id).catch(e => console.error('start timer error:', e?.message || e));
  }, delay);
  startTimers.set(job.id, t);
  scheduledRunAt.set(job.id, job.run_at);
  console.log(\`⏱️ will start in \${Math.round(delay/1000)}s\`);
}

// ===== Preemption =====`
);

sub('capture late offset',
  /const startUtc = parseDbTime\(sched\.starts_at\);\n  const endUtc   = parseDbTime\(sched\.ends_at\);/,
`const startUtc = parseDbTime(sched.starts_at);
  const endUtc   = parseDbTime(sched.ends_at);
  if (startUtc) {
    const late = Math.max(0, Math.floor((DateTime.utc().toMillis() - startUtc.toMillis()) / 1000));
    if (late > 2) {
      lateStartOffsets.set(jobId, late);
      await logEvent(jobId, \`Recovery start: ~\${late}s late; seeking into the show to stay on schedule.\`);
    }
  }`
);

sub('late seek',
  /let absPlayed = 0;/,
  `let absPlayed = Math.max(0, Number(lateStartOffsets.get(jobId) || 0));\n  lateStartOffsets.delete(jobId);`
);

sub('bootstrap recovery',
  /async function bootstrapUpcoming\(\) \{[\s\S]*?\n\}\n\nfunction resilientSubscribe/,
`async function bootstrapUpcoming() {
  const now = DateTime.utc();
  const lookbackIso = now.minus({ days: 1 }).toISO();
  const untilIso = now.plus({ days: 1 }).toISO();

  const { data, error } = await supabase
    .from('jobs')
    .select(\`id, run_at, status, schedule_id, schedule:schedules ( id, ends_at, show:shows ( storage_path ) )\`)
    .eq('status','pending')
    .gte('run_at', lookbackIso)
    .lte('run_at', untilIso)
    .order('run_at',{ascending:true})
    .limit(1000);

  if (error) { console.error('bootstrapUpcoming error:', error.message); return; }
  let count = 0;
  for (const j of (data || [])) {
    const endUtc = parseDbTime(j?.schedule?.ends_at);
    if (!endUtc || endUtc.toMillis() <= now.toMillis()) continue;
    const runAtUtc = parseDbTime(j.run_at);
    if (!startTimers.has(j.id) && runAtUtc && runAtUtc.toMillis() < now.minus({ seconds: 15 }).toMillis()) {
      const late = Math.floor((now.toMillis() - runAtUtc.toMillis()) / 1000);
      await logEvent(j.id, \`Recovery scan: missed start by ~\${late}s; slot still active, starting now.\`, 'error');
    }
    scheduleJob(j, j?.schedule?.show?.storage_path || null);
    count += 1;
  }
  if (!count) console.log('No upcoming or recoverable jobs to schedule.');
  lastBootstrapAt = Date.now();
}

function resilientSubscribe`
);

writeFileSync(runnerPath, s);
console.log('Applied scheduler recovery patch.');
