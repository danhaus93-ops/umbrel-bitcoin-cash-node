'use strict';
/*
 * LoneStrike Cash — dashboard API
 * Reads asicseer-pool (ckpool-lineage) stats from its logdir and queries the
 * backing BCHN node over JSON-RPC. Serves a single /api/status payload to the UI.
 * Degrades gracefully: if the pool hasn't started or the node is unreachable,
 * it returns whatever it can with poolUp/nodeUp flags so the UI stays honest.
 */
const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

const POOL_LOGDIR  = process.env.POOL_LOGDIR  || '/pool/logs';
const STRATUM_HOST = process.env.STRATUM_HOST || 'umbrel.local';
const STRATUM_PORT = process.env.STRATUM_PORT || '3335';
const VERSION      = process.env.APP_VERSION  || 'v1.0.0';

const POOL_DIR  = path.dirname(POOL_LOGDIR);            // /pool
const ADDR_FILE = path.join(POOL_DIR, 'config', 'bch_address');

const SV2_DIR       = process.env.SV2_DIR || '/sv2';
const SV2_ADDR_FILE = path.join(SV2_DIR, 'payout_address');
const SV2_PUB_FILE  = path.join(SV2_DIR, 'keys', 'authority.pub');
const SV2_LOG_FILE  = path.join(SV2_DIR, 'pool_sv2.log');
const SV2_BLOCKS_FILE = path.join(SV2_DIR, 'sv2_blocks.jsonl');
const SV2_RESETS_FILE = path.join(SV2_DIR, 'sv2_resets.json');
const SV2_SPM_FILE    = path.join(SV2_DIR, 'shares_per_minute');
// Miner-rollable extranonce2 space in bytes. SRI hardcodes this as
// CLIENT_SEARCH_SPACE_BYTES = 16; the pool patch reads this file to make it
// tunable. Bounds: <4 starves SV1-via-translator miners, >32 eats the
// coinbase scriptSig budget (100B total, ~44B used today).
const SV2_XN_FILE     = path.join(SV2_DIR, 'extranonce2_bytes');
// How long a silent worker stays on the roster, in minutes. Requested by a
// tester whose rigs lingered ~70 minutes after a rental ended. Read from disk
// on every use, so saving it applies on the next sweep -- no restart, hence no
// saved-vs-active gap like the pool-side settings have.
const WORKER_TTL_FILE = path.join(SV2_DIR, 'worker_ttl_min');
function workerTtlSec() {
  try {
    const n = parseInt(fs.readFileSync(WORKER_TTL_FILE, 'utf8').trim(), 10);
    if (n >= 5 && n <= 1440) return n * 60;
  } catch (_) {}
  return Number(process.env.WORKER_TTL_SEC || 3600);
}
const SV2_XN_DEFAULT  = 16;
function readSv2Xn() {
  try {
    const n = parseInt(fs.readFileSync(SV2_XN_FILE, 'utf8').trim(), 10);
    return (n >= 4 && n <= 32) ? n : SV2_XN_DEFAULT;
  } catch (_) { return SV2_XN_DEFAULT; }
}
const SV2_MON_URL = process.env.SV2_MON_URL || 'http://sslabs-solostrike-cash_sv2-pool_1:9090';
// SRI monitoring API poller: authoritative names/counts/rejects/best/declared.
// The log-parsed estimator stays authoritative for delivered hashrate + trend
// (the API cannot rate floor channels that declare nominal_hash_rate 0).
const sv2Api = { at: 0, channels: {} };
async function sv2ApiPoll() {
  if (Date.now() - sv2Api.at < 5000) return;
  sv2Api.at = Date.now();
  try {
    let items = [];
    for (let off = 0; off < 1000; off += 100) {
      const pg = await (await fetch(SV2_MON_URL + '/api/v1/clients?limit=100&offset=' + off, { signal: AbortSignal.timeout(1500) })).json();
      items = items.concat(pg.items || []);
      if (!pg.items || pg.items.length < 100) break;
    }
    const cl = { items };
    const next = {};
    for (const c of (cl.items || [])) {
      try {
        const ch = await (await fetch(SV2_MON_URL + '/api/v1/clients/' + c.client_id + '/channels?limit=50', { signal: AbortSignal.timeout(1500) })).json();
        for (const e of (ch.extended_channels || []).concat(ch.standard_channels || [])) {
          next[c.client_id + ':' + e.channel_id] = {
            identity: e.user_identity || '', nominal: Number(e.nominal_hashrate) || 0,
            accepted: Number(e.shares_accepted) || 0, rejected: Number(e.shares_rejected) || 0,
            reasons: e.shares_rejected_by_reason || {},
            best: Number(e.best_diff) || 0, blocks: Number(e.blocks_found) || 0,
            targetHex: e.target_hex || '', spm: Number(e.expected_shares_per_minute) || 0,
          };
        }
      } catch (_) {}
    }
    if (Object.keys(next).length) sv2Api.channels = next;
  } catch (_) { /* API unavailable: log-parsed telemetry carries on alone */ }
}
setInterval(() => { sv2ApiPoll().catch(() => {}); }, 10000);
const SV2_BEST_FILE   = path.join(SV2_DIR, 'sv2_best.json');
// ---- UNIFIED WORKER MODEL -------------------------------------------------
// One schema for both protocols. Before this, an SV1 row's `trend` was
// [1m,5m,1hr,1d,7d] window averages while an SV2 row's `trend` was five
// 1-minute buckets -- different meanings in the same UI field -- and SV2 rows
// carried no `accepted` at all, so per-worker accept/reject was impossible.
//
// Hashrate windows come from per-minute rings keyed by minute-epoch. SV2 feeds
// them with the existing per-minute bucket estimator (unchanged: that math is
// hard-won and must not regress); SV1 feeds them from asicseer's own
// hashrate1m. Rings are the ONLY way to reach 1h/1d for SV2, because
// sv2State.shares is pruned at 600s.
const WORKERS_STATE_FILE = path.join(SV2_DIR, 'workers_state.json');
const MINS_KEEP = 1440;                       // 24h of 1-minute samples
const workerMins = {};                        // "SV1:name" | "SV2:cid" -> {minute: hs}
function ringKey(proto, id) { return proto + ':' + id; }
function ringPut(key, hs) {
  if (!(hs > 0)) return;
  const m = workerMins[key] || (workerMins[key] = {});
  m[Math.floor(Date.now() / 60000)] = hs;     // idempotent per minute
}
function ringPutAt(key, minute, hs) {
  if (!(hs > 0)) return;
  const m = workerMins[key] || (workerMins[key] = {});
  m[minute] = hs;
}
function ringPrune() {
  const cut = Math.floor(Date.now() / 60000) - MINS_KEEP;
  for (const k of Object.keys(workerMins)) {
    const m = workerMins[k];
    for (const min of Object.keys(m)) if (Number(min) < cut) delete m[min];
    if (!Object.keys(m).length) delete workerMins[k];
  }
}
// Average over the last n COMPLETED minutes (the current minute is partial and
// would read low). Absent minutes are gaps, not zeroes: averaging over present
// samples keeps an idle-then-active miner honest instead of diluting it.
function ringWin(key, n) {
  const m = workerMins[key];
  if (!m) return 0;
  const now = Math.floor(Date.now() / 60000);
  let sum = 0, cnt = 0;
  for (let i = 1; i <= n; i++) {
    const v = m[now - i];
    if (v > 0) { sum += v; cnt++; }
  }
  return cnt ? sum / cnt : 0;
}
function ringTrend(key) {
  const now = Math.floor(Date.now() / 60000);
  const m = workerMins[key] || {};
  return [5, 4, 3, 2, 1].map((i) => Number(m[now - i]) || 0);
}
function unifiedHashrate(key) {
  return { '1m': ringWin(key, 1), '5m': ringWin(key, 5),
           '1h': ringWin(key, 60), '1d': ringWin(key, 1440) };
}

let sv2Resets = {};  // worker name (or __all__) -> reset ts (sec)
let sv2BestP  = {};  // worker name -> { best, ts }  (persisted across restarts)
try { sv2Resets = JSON.parse(fs.readFileSync(SV2_RESETS_FILE, 'utf8')) || {}; } catch (_) {}
try { sv2BestP  = JSON.parse(fs.readFileSync(SV2_BEST_FILE,   'utf8')) || {}; } catch (_) {}
// SV2 counters live in memory and were lost on every restart (the log tail
// re-scan only reaches back ~4MB). Snapshot channels + rings so a dashboard
// restart no longer zeroes a fleet's accepted/rejected/best.
function workersSave() {
  try {
    const chans = {};
    for (const [cid, ch] of Object.entries(sv2State.channels)) {
      chans[cid] = { name: ch.name, accepted: ch.accepted, rejected: ch.rejected,
                     best: ch.best, last: ch.last, firstSeen: ch.firstSeen,
                     maxSeq: ch.maxSeq, accounted: ch.accounted || 0 };
    }
    ringPrune();
    const tmp = WORKERS_STATE_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ v: 1, at: Date.now(),
                                           channels: chans, mins: workerMins }));
    fs.renameSync(tmp, WORKERS_STATE_FILE);
  } catch (_) {}
}
function workersLoad() {
  try {
    const j = JSON.parse(fs.readFileSync(WORKERS_STATE_FILE, 'utf8'));
    if (!j || j.v !== 1) return;
    for (const [cid, c] of Object.entries(j.channels || {})) {
      sv2State.channels[cid] = {
        name: c.name, accepted: Number(c.accepted) || 0,
        rejected: Number(c.rejected) || 0, best: Number(c.best) || 0,
        maxSeq: Number(c.maxSeq) >= 0 ? Number(c.maxSeq) : -1,
        last: Number(c.last) || 0,
        firstSeen: Number(c.firstSeen) || Math.floor(Date.now() / 1000),
        accounted: Number(c.accounted) || 0,
      };
    }
    for (const [k, m] of Object.entries(j.mins || {})) workerMins[k] = m;
    ringPrune();
  } catch (_) {}
}
function sv2SaveResets() { try { fs.writeFileSync(SV2_RESETS_FILE, JSON.stringify(sv2Resets)); } catch (_) {} }
// Chris (2026-07-18): "reset best doesn't reset sv2 workers" -- the translator
// stats API reports its OWN cumulative best, which knows nothing about
// dashboard resets, and the merge re-imported it seconds after every reset.
// Baseline the API value at reset; only a value EXCEEDING the baseline is a
// genuinely new record. (Post-reset bests below the old record come from the
// share-level log ingest, which honors reset markers precisely.)
const SV2_API_BASE_FILE = path.join(SV2_DIR, 'sv2_api_base.json');
let sv2ApiBase = (() => { try { return JSON.parse(fs.readFileSync(SV2_API_BASE_FILE, 'utf8')) || {}; } catch (_) { return {}; } })();
let sv2ApiLast = {};   // name -> highest api.best seen this process
function sv2SaveApiBase() { try { fs.writeFileSync(SV2_API_BASE_FILE, JSON.stringify(sv2ApiBase)); } catch (_) {} }
// Chris (2026-07-28): round effort "reset and came back" -- 101% shown 100
// minutes after a block his ~2 PH/s could only have re-earned ~3G of. The
// ingest replays the log tail on every dashboard restart (offset starts at
// zero), and the round accumulators had no timestamp guard, so the entire
// pre-block round resurrected from the tail. Same disease as the 1.4.62
// reset-best bug; same cure: a persisted watermark. Shares at or before the
// round start do not count toward the round.
const SV2_ROUND_FILE = path.join(SV2_DIR, 'sv2_round_start.json');
let sv2RoundStartTs = (() => { try { return Number(JSON.parse(fs.readFileSync(SV2_ROUND_FILE, 'utf8')).start) || 0; } catch (_) { return 0; } })();
// The status merge is client-driven: with the page closed, block detection
// happens whenever the user next opens the dashboard -- possibly an hour
// after the solve, when the live round snapshot is stale and the freshness
// gate rightly refuses it (Chris's 963169: dash). Persist a once-a-minute
// sample of the round total so a late detection can still read the round
// as it stood at solve time.
const SV2_ROUND_SAMPLES_FILE = path.join(SV2_DIR, 'round_samples.json');
let sv2RoundSamples = (() => { try { return JSON.parse(fs.readFileSync(SV2_ROUND_SAMPLES_FILE, 'utf8')) || []; } catch (_) { return []; } })();
let sv2LastSampleTs = 0;
function sv2RoundSample(total) {
  const now = Math.floor(Date.now() / 1000);
  if (now - sv2LastSampleTs < 60) return;
  sv2LastSampleTs = now;
  sv2RoundSamples.push({ ts: now, total: Math.round(total) });
  if (sv2RoundSamples.length > 2880) sv2RoundSamples = sv2RoundSamples.slice(-2880);
  try { fs.writeFileSync(SV2_ROUND_SAMPLES_FILE, JSON.stringify(sv2RoundSamples)); } catch (_) {}
}
function sv2RoundNear(ts) {
  let best = null, dt = Infinity;
  for (const r of sv2RoundSamples) {
    const d = Math.abs(r.ts - ts);
    if (d < dt) { dt = d; best = r.total; }
  }
  return (best != null && dt < 600) ? best : null;
}
// Chris (2026-08-31): "work done on sv2 and sv1 for effort isn't added
// together so doesn't reset the other when it hits" -- half right, and the
// half that's right matters: the SV1 component came straight from
// asicseer's own round counter, which only resets when asicseer itself
// solves. An SV2 block reset our SV2 accumulators but left the SV1 round
// climbing. A persisted SV1 baseline makes any LOCAL block zero the whole
// fleet's round; asicseer self-resets (its own solve) re-zero the baseline.
let sv1RoundBase = (() => { try { return Number(JSON.parse(fs.readFileSync(SV2_ROUND_FILE, 'utf8')).sv1Base) || 0; } catch (_) { return 0; } })();
function sv2SetRoundStart(ts, sv1Acc) {
  sv2RoundStartTs = ts;
  if (sv1Acc != null) sv1RoundBase = sv1Acc;
  try { fs.writeFileSync(SV2_ROUND_FILE, JSON.stringify({ start: ts, sv1Base: sv1RoundBase })); } catch (_) {}
}
function sv2SaveBest()   { try { fs.writeFileSync(SV2_BEST_FILE,   JSON.stringify(sv2BestP));  } catch (_) {} }
function sv2ResetTs(name) { return Math.max(Number(sv2Resets[name]) || 0, Number(sv2Resets.__all__) || 0); }
// Chris #1: an individual reset must also clear that worker's displayed
// accepted/rejected. Counters are cumulative since the channel opened, so a
// reset stores a baseline and rows report the delta.
const SV2_CNT_BASE_FILE = path.join(SV2_DIR, 'sv2_count_base.json');
let sv2CntBase = (() => {            // initialise AT declaration: loading
  try { return JSON.parse(fs.readFileSync(SV2_CNT_BASE_FILE, 'utf8')) || {}; }  // it earlier hit the TDZ
  catch (_) { return {}; }           // and crashed the dashboard at boot
})();
function sv2SaveCntBase() { try { fs.writeFileSync(SV2_CNT_BASE_FILE, JSON.stringify(sv2CntBase)); } catch (_) {} }
function sv2ApplyCountReset(scope) {
  for (const ch of Object.values(sv2State.channels)) {
    if (scope === 'all' || ch.name === scope) {
      sv2CntBase[ch.name] = { accepted: ch.accepted || 0, rejected: ch.rejected || 0 };
    }
  }
  if (scope === 'all') {
    for (const k of Object.keys(workerMins)) if (k.startsWith('SV2:')) delete workerMins[k];
  }
  sv2SaveCntBase();
  try { workersSave(); } catch (_) {}
}
function sv2CntFor(ch) {
  const b = sv2CntBase[ch.name] || { accepted: 0, rejected: 0 };
  return { accepted: Math.max(0, (ch.accepted || 0) - (b.accepted || 0)),
           rejected: Math.max(0, (ch.rejected || 0) - (b.rejected || 0)) };
}
function sv2ApplyReset(scope) {
  const now = Math.floor(Date.now() / 1000);
  if (scope === 'all') { sv2Resets.__all__ = now; } else { sv2Resets[scope] = now; }
  for (const ch of Object.values(sv2State.channels)) {
    if (scope === 'all' || ch.name === scope) ch.best = 0;
  }
  for (const n of Object.keys(sv2BestP)) {
    if (scope === 'all' || n === scope) delete sv2BestP[n];
  }
  // freeze the translator's cumulative best as the new floor for this scope
  const names = new Set(Object.values(sv2State.channels).map((c) => c.name));
  for (const n of Object.keys(sv2ApiLast)) names.add(n);
  for (const n of names) {
    if (scope === 'all' || n === scope) {
      sv2ApiBase[n] = Math.max(Number(sv2ApiBase[n]) || 0, Number(sv2ApiLast[n]) || 0);
    }
  }
  sv2SaveApiBase();
  sv2SaveResets(); sv2SaveBest();
}
function readSv2Address() {
  try { return fs.readFileSync(SV2_ADDR_FILE, 'utf8').trim(); } catch (_) { return ''; }
}

function readAddress() {
  try { return fs.readFileSync(ADDR_FILE, 'utf8').trim(); } catch (_) { return ''; }
}
function validAddress(a) {
  if (!a || typeof a !== 'string') return false;
  a = a.trim();
  if (a.length > 110) return false;
  // A 20-byte-hash cashaddr is exactly 42 chars (34 payload + 8 checksum).
  // Longer means a bigger hash (e.g. P2SH32), which the pool cannot pay to:
  // accepting one here used to hand the SV2 entrypoint an address it would
  // reject, crash-looping the container. Keep this in step with
  // sv2/pool/sv2-helpers/addr_to_script.py.
  if (/^(bitcoincash:|bchtest:|bchreg:)?[qp][0-9a-z]{41}$/i.test(a)) return true; // cashaddr
  if (/^[13][1-9A-HJ-NP-Za-km-z]{25,34}$/.test(a)) return true;                    // legacy
  return false;
}
function writeAddress(a) {
  fs.mkdirSync(path.dirname(ADDR_FILE), { recursive: true });
  fs.writeFileSync(ADDR_FILE, a.trim());
}

const RPC_HOST = process.env.RPC_HOST || 'sslabs-bitcoin-cash-node_bitcoind_1';
const RPC_PORT = process.env.RPC_PORT || '8332';
const RPC_USER = process.env.RPC_USER || 'bchn';
const RPC_PASS = process.env.RPC_PASS || 'bchn_local_rpc_pw_2f9c';

// ---- helpers -------------------------------------------------------------
const SUFFIX = { E:1e18, P:1e15, T:1e12, G:1e9, M:1e6, K:1e3 };
const UNIT   = { E:'EH/s', P:'PH/s', T:'TH/s', G:'GH/s', M:'MH/s', K:'KH/s', '':'H/s' };

function hashToHs(str) {
  const m = String(str || '0').trim().match(/^([0-9.]+)\s*([KMGTPE]?)/i);
  if (!m) return 0;
  const mult = { '': 1, K: 1e3, M: 1e6, G: 1e9, T: 1e12, P: 1e15, E: 1e18 }[m[2].toUpperCase()] || 1;
  return parseFloat(m[1]) * mult;
}
// ckpool reports hashrate as strings like "92.4T". Split into display + numeric.
function parseHash(s) {
  if (s == null) return { val: '0', unit: 'H/s', n: 0 };
  const m = String(s).trim().match(/^([\d.]+)\s*([EPTGMK]?)/i);
  if (!m) return { val: '0', unit: 'H/s', n: 0 };
  const suf = (m[2] || '').toUpperCase();
  return { val: m[1], unit: UNIT[suf] || 'H/s', n: parseFloat(m[1]) * (SUFFIX[suf] || 1) };
}

function fmtHs(hs) {
  hs = Number(hs) || 0;
  const steps = [[1e18,'EH/s'],[1e15,'PH/s'],[1e12,'TH/s'],[1e9,'GH/s'],[1e6,'MH/s'],[1e3,'KH/s']];
  for (const [m, u] of steps) if (hs >= m) return { val: (hs / m).toFixed(2), unit: u, n: hs };
  return { val: hs.toFixed(0), unit: 'H/s', n: hs };
}

// ---- SV2 telemetry: parse pool_sv2's own log from the shared volume -------
// Every accepted share logs share_hash (-> difficulty) and share_work
// (difficulty units credited). Hashrate = sum(share_work in window) * 2^32 / secs.
const SV2_D1 = 0xffffn << 208n;                    // difficulty-1 target
// The solving share's achieved difficulty IS the block hash's difficulty --
// chain-authoritative, no log needed. share_work in the current pool build
// is the vardiff CREDITED target (~800K), which briefly displayed as an
// SV2 block's "best" (six orders of magnitude under the real solve).
function hashAchievedDiff(hexHash) {
  try { return Number(SV2_D1 * 1000000n / BigInt('0x' + hexHash)) / 1e6; } catch (_) { return 0; }
}
const sv2State = {
  offset: 0, pendingId: '', pendingAge: 99, roundWork: 0, lastBlockCount: -1,
  // The extranonce2 size the pool is REALLY running, read off the wire rather
  // than from the file. The file is what you SAVED; the pool only reads it at
  // container start, so the two can disagree indefinitely with nothing on
  // screen to say so. That cost a tester a day: he saved 5, the pool kept
  // enforcing 4, his translator asked for 5 and was refused -- correctly.
  activeXn: 0,
  roundDiff: 0, allDiff: 0,          // difficulty-weighted share accounting
  channels: {},                                    // cid -> stats
  shares: [],                                      // [ts_ms, work, cid] rolling window
};
const SV2_RE_SHARE = /valid share \| downstream_id: (\d+), channel_id: (\d+), sequence_number: (\d+), share_hash: ([0-9a-fA-F]{64}), share_work: ([0-9.eE+-]+)/;
// What the pool actually GRANTED a channel. Authoritative: this is the size the
// miner is rolling right now. Preferred over the entrypoint banner because the
// banner prints once at boot and rotates out of a busy log.
// NB: [^)]* does NOT work here -- target: U256(...) closes a paren before
// extranonce_size ever appears, so the class stops short. Caught by testing
// against a real line from a tester's log rather than an imagined one.
const SV2_RE_XN_GRANT = /OpenExtendedMiningChannelSuccess.*?extranonce_size: (\d+)/;
// Fallback: the boot banner, for a pool that has not opened a channel yet.
const SV2_RE_XN_BOOT  = /\[entrypoint\] extranonce2 bytes: (\d+)/;
const SV2_RE_TS    = /^(\d{4}-\d{2}-\d{2}T[0-9:.]+Z)/;
// Current pool build splits identity across two lines: Open carries
// (request_id, user_identity); Success carries (request_id, channel_id).
// Join by request_id -- robust to line distance and redaction brackets.
const sv2PendingIdent = new Map();
const SV2_RE_OPEN_REQ = /Received Open(?:Standard|Extended)MiningChannel\b.*?request_id[=:\s]+(\d+).*?user_identity[=:\s]+\[?([^\],\s)]+)/;
const SV2_RE_OPEN_OK2 = /\(downstream_id[=:\s]+(\d+)\).*?MiningChannelSuccess\(request_id[=:\s]+(\d+),\s*channel_id[=:\s]+(\d+)/;
const SV2_RE_IDENT = /Open(?:Standard|Extended)MiningChannel\b[^\n]*?user_identity[^"\x27]*["\x27]?([^"\x27,\s)]+)/;
const SV2_RE_OPENOK = /Open(?:Standard|Extended)MiningChannelSuccess[^\n]*?channel_id[=:\s]+(\d+)/;
const SV2_RE_BAD   = /(?:invalid share|SubmitSharesError)[^\n]*?channel_id[=:\s]+(\d+)/;

function sv2Chan(cid) {
  return sv2State.channels[cid] || (sv2State.channels[cid] = {
    name: 'sv2-' + String(cid).replace(':', '.'), accepted: 0, rejected: 0, best: 0,
    maxSeq: -1, last: 0, firstSeen: Math.floor(Date.now() / 1000),
  });
}
function sv2Ingest() {
  let st; try { st = fs.statSync(SV2_LOG_FILE); } catch (_) { return; }
  // Rotate in place BEFORE the pool container's own 16MB rotator would fire:
  // writeFileSync truncates the SAME inode, so the pool's tee (O_APPEND)
  // keeps writing. A rename-based rotate detaches the writer into an
  // unlinked inode and blinds telemetry (learned the hard way).
  if (st.size > 12 * 1024 * 1024) {
    try {
      const keep = 6 * 1024 * 1024;
      const fd = fs.openSync(SV2_LOG_FILE, 'r');
      const buf = Buffer.alloc(keep);
      fs.readSync(fd, buf, 0, keep, st.size - keep);
      fs.closeSync(fd);
      const nl = buf.indexOf(10) + 1;                 // start at a line boundary
      fs.writeFileSync(SV2_LOG_FILE, buf.slice(nl));  // truncate same inode
      st = fs.statSync(SV2_LOG_FILE);
      sv2State.offset = 0;                            // rescan; seq dedupe absorbs
    } catch (_) {}
  }
  if (st.size < sv2State.offset) sv2State.offset = 0;          // rotated
  if (st.size === sv2State.offset) return;
  let start = sv2State.offset;
  if (st.size - start > 4 * 1024 * 1024) start = st.size - 4 * 1024 * 1024;
  let fd, chunk;
  try {
    fd = fs.openSync(SV2_LOG_FILE, 'r');
    const buf = Buffer.alloc(st.size - start);
    fs.readSync(fd, buf, 0, buf.length, start);
    chunk = buf.toString('utf8');
  } catch (_) { return; } finally { try { if (fd !== undefined) fs.closeSync(fd); } catch (_) {} }
  sv2State.offset = st.size;
  const nowS = Math.floor(Date.now() / 1000);
  for (const line of chunk.split('\n')) {
    const xg = SV2_RE_XN_GRANT.exec(line) || SV2_RE_XN_BOOT.exec(line);
    if (xg) { const n = parseInt(xg[1], 10); if (n >= 1 && n <= 32) sv2State.activeXn = n; }
    const oq2 = SV2_RE_OPEN_REQ.exec(line);
    if (oq2) { sv2PendingIdent.set(oq2[1], oq2[2]); if (sv2PendingIdent.size > 64) sv2PendingIdent.delete(sv2PendingIdent.keys().next().value); }
    const ok2 = SV2_RE_OPEN_OK2.exec(line);
    if (ok2 && sv2PendingIdent.has(ok2[2])) {
      const idn = sv2PendingIdent.get(ok2[2]); sv2PendingIdent.delete(ok2[2]);
      if (/^[A-Za-z0-9:._\-]{4,64}$/.test(idn)) {
        const dj = idn.lastIndexOf('.');
        sv2Chan(ok2[1] + ':' + ok2[3]).name = (dj > 0 && dj < idn.length - 1) ? idn.slice(dj + 1) : idn;
      }
    }
    if (/Open(?:Standard|Extended)MiningChannel\b/.test(line) && line.includes('user_identity')) {
      let ident = '';
      const plain = line.match(/user_identity[=:\s]+([^\s,"\x27)]+)/);
      if (plain) ident = plain[1];
      const q = !ident && line.match(/user_identity[^"\x27]*["\x27]([^"\x27]+)["\x27]/);
      if (q) ident = q[1];
      if (!ident) {
        const ba = line.match(/user_identity[^\[]*\[([0-9,\s]+)\]/);
        if (ba) {
          try {
            ident = String.fromCharCode(...ba[1].split(',').map(x => parseInt(x, 10)));
          } catch (_) { ident = ''; }
        }
      }
      if (!ident) { const pm = line.match(SV2_RE_IDENT); if (pm) ident = pm[1]; }
      ident = String(ident || '').trim().slice(0, 64);
      // accept only plausible worker/address names; junk keeps the sv2-chN default
      sv2State.pendingAge = 0;
      if (/^[A-Za-z0-9:._\-]{4,64}$/.test(ident)) {
        // pool convention: payout-address.workername -> show the worker part
        const dot = ident.lastIndexOf('.');
        sv2State.pendingId = (dot > 0 && dot < ident.length - 1) ? ident.slice(dot + 1) : ident;
      }
    }
    const okm = line.match(/downstream_id[=:\s]+(\d+)[^\n]*?Open(?:Standard|Extended)MiningChannelSuccess[^\n]*?channel_id[=:\s]+(\d+)/) ||
                line.match(SV2_RE_OPENOK);
    if (okm && sv2State.pendingId && sv2State.pendingAge <= 3) {
      const key = okm[2] !== undefined ? okm[1] + ':' + okm[2] : okm[1];
      sv2Chan(key).name = sv2State.pendingId; sv2State.pendingId = '';
    }
    sv2State.pendingAge++;
    if (/invalid share|SubmitSharesError/.test(line)) {
      const dm = line.match(/downstream_id[=:\s]+(\d+)/);
      const cm = line.match(/channel_id[=:\s]+(\d+)/);
      if (dm && cm) {
        const ch2 = sv2Chan(dm[1] + ':' + cm[1]);
        ch2.rejected++;
        const rm = line.match(/error_code[=:\s]+([a-zA-Z-]+)/);
        ch2.lastReject = (rm ? rm[1] : 'invalid') + ' @ ' + line.slice(0, 19);
      }
    }
    const m = line.match(SV2_RE_SHARE);
    if (!m) continue;
    const cid = m[1] + ':' + m[2], seq = Number(m[3]);
    const ch = sv2Chan(cid);
    if (seq <= ch.maxSeq) continue;                            // replay dedupe
    ch.maxSeq = seq;
    const work = parseFloat(m[5]) || 0;
    let diff = 0;
    try { diff = Number(SV2_D1 * 1000000n / BigInt('0x' + m[4])) / 1e6; } catch (_) {}
    const tm = line.match(SV2_RE_TS);
    const tsMs = tm ? Date.parse(tm[1]) : Date.now();
    ch.accepted++;
    if (tsMs / 1000 > sv2ResetTs(ch.name) && diff > ch.best) {
      ch.best = diff;
      const p = sv2BestP[ch.name];
      if (!p || diff > p.best) { sv2BestP[ch.name] = { best: diff, ts: Math.floor(tsMs / 1000) }; sv2SaveBest(); }
    }
    ch.last = Math.max(ch.last, Math.floor(tsMs / 1000));
    if (tsMs / 1000 > sv2RoundStartTs) {
      sv2State.roundWork += work;
      ch.roundAcc = (ch.roundAcc || 0) + 1;
    }
    sv2State.shares.push([tsMs, work, cid, diff, seq]);
  }
  const cut = Date.now() - 600 * 1000;
  while (sv2State.shares.length && sv2State.shares[0][0] < cut) sv2State.shares.shift();
  void nowS;
}
setInterval(() => { try { sv2Ingest(); } catch (_) {} }, 10000);
setInterval(() => { try { workersSave(); } catch (_) {} }, 60000);

setInterval(() => {
  try {
    const nowS = Math.floor(Date.now() / 1000);
    const TTL = workerTtlSec();
    let dirty = false;
    for (const [cid, ch] of Object.entries(sv2State.channels)) {
      const fb = /^sv2-\d+\.\d+$/.test(ch.name);
      // Chris (2026-07-31): host-side connection churn presented every
      // reconnect under a fresh incremented name (miner5..miner79, "80-100s
      // of workers but they are the same workers"). A ghost that connected
      // and never contributed a share dies 3 minutes after last contact;
      // contributors keep the full TTL.
      const ghost = !(ch.accepted > 0) && !(ch.best > 0);
      const ttl = ghost ? 180 : (fb ? Math.min(TTL, 300) : TTL);
      if (!ch.last || nowS - ch.last > (ghost ? ttl : ttl + 600)) {
        delete sv2State.channels[cid];
        if (fb && sv2BestP[ch.name]) { delete sv2BestP[ch.name]; dirty = true; }
      }
    }
    if (dirty) sv2SaveBest();
  } catch (_) {}
}, 120000);

function sv2Blocks() {
  let raw; try { raw = fs.readFileSync(SV2_BLOCKS_FILE, 'utf8'); } catch (_) { return []; }
  const out = [];
  for (const l of raw.split('\n')) {
    if (!l.trim()) continue;
    try { const j = JSON.parse(l); if (j && j.height) out.push(j); } catch (_) {}
  }
  return out;
}
function sv2Stats() {
  sv2Ingest();
  const nowS = Math.floor(Date.now() / 1000);
  const winMs = 300 * 1000, cutoff = Date.now() - winMs;
  const TTL = workerTtlSec();
  const perChan = {};
  for (const [ts, w, cid, diff, seq] of sv2State.shares) {
    if (ts < cutoff) continue;
    const pc = perChan[cid] || (perChan[cid] = {
      work: 0, n: 0, diffs: [], minSeq: Infinity, maxSeq: -1, firstTs: ts, lastTs: ts,
    });
    pc.work += w; pc.n++;
    if (diff > 0) pc.diffs.push(diff);
    if (seq >= 0) {
      if (seq < pc.minSeq) pc.minSeq = seq;
      if (seq > pc.maxSeq) pc.maxSeq = seq;
    }
    if (ts < pc.firstTs) pc.firstTs = ts;
    if (ts > pc.lastTs) pc.lastTs = ts;
  }
  const chanBuckets = (cid) => {
    // five 1-minute buckets ending now; per-bucket median handles the
    // submitter's threshold shifting mid-window (mixed populations would
    // bias a single global median)
    const nowMs = Date.now();
    const B = 5, W = 60 * 1000;
    const bk = Array.from({ length: B }, () => ({ n: 0, work: 0, diffs: [], minSeq: Infinity, maxSeq: -1 }));
    for (const [ts, w, c, diff, seq] of sv2State.shares) {
      if (c !== cid) continue;
      const idx = Math.floor((nowMs - ts) / W);
      if (idx < 0 || idx >= B) continue;
      const b = bk[B - 1 - idx];
      b.n++; b.work += w;
      if (diff > 0) b.diffs.push(diff);
      if (seq >= 0) { if (seq < b.minSeq) b.minSeq = seq; if (seq > b.maxSeq) b.maxSeq = seq; }
    }
    return bk.map((b) => {
      if (b.work / b.n >= 1) {
        // vardiff-credited: exact at any sample size (a single diff-500K
        // share is known work) - rental fleets fan out across many
        // sparse channels and zeroing them hid hundreds of TH
        const nS = (b.maxSeq >= b.minSeq) ? (b.maxSeq - b.minSeq + 1) : b.n;
        const nA = Math.max(b.n, Math.min(nS, Math.ceil(b.n * 1.25)));
        return b.work * (nA / b.n) * 4294967296 / 60;
      }
      if (b.n < 5) return 0;   // sparse guard: statistical path only
      const nSeq = (b.maxSeq >= b.minSeq) ? (b.maxSeq - b.minSeq + 1) : b.n;
      const n = Math.max(b.n, Math.min(nSeq, Math.ceil(b.n * 1.25)));
      // every 10th share is batch-acked without a valid-share line: the
      // sequence span recovers its COUNT, this ratio recovers its CREDIT
      const workAdj = b.work * (n / b.n);
      const workHs = workAdj * 4294967296 / 60;
      if (b.work / b.n >= 1) return workHs;   // vardiff live: credited work is authoritative
      let hashHs = 0;
      if (b.diffs.length >= 8) {
        // floor-target rescue only: hashes are uniform below the
        // submitter's own threshold. Estimate it from the 85th-percentile
        // hash: junk-immune like a median, half the variance.
        const d = b.diffs.sort((a, b2) => a - b2);
        const tDiff = d[Math.floor(d.length * 0.15)] * 0.85;
        hashHs = n * tDiff * 4294967296 / 60;
      }
      return Math.max(workHs, hashHs);
    });
  };
  const chanHs = (buckets) => {
    const completed = buckets.slice(0, -1).filter((v) => v > 0);
    if (completed.length) return completed.reduce((a, v) => a + v, 0) / completed.length;
    const live = buckets.filter((v) => v > 0);           // warmup: newest is all we have
    if (!live.length) return 0;
    return live.reduce((a, v) => a + v, 0) / live.length;
  };
  let hidden = {};
  try { hidden = JSON.parse(fs.readFileSync(path.join(POOL_DIR, 'config', 'hidden.json'), 'utf8')) || {}; } catch (_) {}
  const winDiffs = {};
  for (const [ts, w, c, diff] of sv2State.shares) {
    if (ts < cutoff || !(diff > 0)) continue;
    (winDiffs[c] || (winDiffs[c] = [])).push(diff);
  }
  const workerList = [];
  let best = 0, accepted = 0, rejected = 0, totHs = 0;
  for (const [cid, ch] of Object.entries(sv2State.channels)) {
    const pBest = (sv2BestP[ch.name] && sv2BestP[ch.name].ts > sv2ResetTs(ch.name)) ? sv2BestP[ch.name].best : 0;
    if (pBest > ch.best) ch.best = pBest;
    best = Math.max(best, ch.best);
    // rental/proxy churn: a connection that died before ever earning a
    // real name (API identity) is disposable - retire it in 5 minutes.
    // Named miners keep the full TTL so a real rig going offline stays
    // visible for an hour.
    const isFallbackName = /^sv2-\d+\.\d+$/.test(ch.name);
    const ttlEff = isFallbackName ? Math.min(TTL, 300) : TTL;
    if (!ch.last || nowS - ch.last > ttlEff) continue;
    const ht = Number(hidden[ch.name]) || 0;
    if (ht && ch.last <= ht) continue;
    // a reconnected miner opens a new channel: retire the stale twin
    let fresher = false;
    for (const [ocid, och] of Object.entries(sv2State.channels)) {
      if (ocid !== cid && och.name === ch.name && (och.last > ch.last ||
          (och.last === ch.last && och.firstSeen > ch.firstSeen))) { fresher = true; break; }
    }
    if (fresher) continue;
    accepted += ch.accepted; rejected += ch.rejected;
    const buckets = chanBuckets(cid);
    const api = sv2Api.channels[cid];
    if (api) {
      if (api.identity) {
        const dot = api.identity.lastIndexOf('.');
        ch.name = (dot > 0 && dot < api.identity.length - 1) ? api.identity.slice(dot + 1) : api.identity;
      }
      if (api.accepted > ch.accepted) ch.accepted = api.accepted;
      if (api.rejected > ch.rejected) ch.rejected = api.rejected;
      if (api.best > 0) {
        sv2ApiLast[ch.name] = Math.max(Number(sv2ApiLast[ch.name]) || 0, api.best);
        // import only a value that BEATS the reset-time baseline: the old
        // '* 0' guard was always-true and resurrected pre-reset bests.
        if (api.best > (Number(sv2ApiBase[ch.name]) || 0) && api.best > ch.best) ch.best = api.best;
      }
    }
    // Feed the unified rings from the existing per-minute estimator. buckets
    // are [t-5min .. t-1min]; index 4 is the current partial minute, so only
    // 0..3 are complete. Writing them keyed by minute-epoch is idempotent
    // across the many /api/status polls per minute.
    const rk = ringKey('SV2', cid);
    const nowMin = Math.floor(Date.now() / 60000);
    for (let i = 0; i < 4; i++) ringPutAt(rk, nowMin - (4 - i), buckets[i]);
    const cHs = chanHs(buckets);
    totHs += cHs;
    // credit newly seen shares at estimated target diff (SV1 semantics)
    const wd = winDiffs[cid];
    if (wd && wd.length >= 8) {
      const sorted = wd.slice().sort((a, b) => a - b);
      const tDiff = sorted[Math.floor(sorted.length * 0.15)] * 0.85;
      const deltaAll = ch.accepted - (ch.accounted || 0);
      if (deltaAll > 0 && tDiff > 0) {
        ch.accounted = ch.accepted;
        sv2State.allDiff += deltaAll * tDiff;
      }
      // round accounting draws only on shares AFTER the round watermark --
      // replayed pre-block shares rebuild ch.accepted but not ch.roundAcc
      const deltaRound = (ch.roundAcc || 0) - (ch.accountedRound || 0);
      if (deltaRound > 0 && tDiff > 0) {
        ch.accountedRound = ch.roundAcc || 0;
        sv2State.roundDiff += deltaRound * tDiff;
      }
    }
    const hr = fmtHs(cHs);
    // UNIFIED SCHEMA -- identical field set to the SV1 rows in readWorkers().
    workerList.push({
      name: ch.name, proto: 'SV2',
      conns: 1,
      declared: api ? api.nominal : null,
      accepted: sv2CntFor(ch).accepted,
      rejected: sv2CntFor(ch).rejected,
      rejectReasons: api ? api.reasons : null,
      hashrate: hr.val + ' ' + hr.unit,       // display string (unchanged)
      hs: unifiedHashrate(rk),                // {1m,5m,1h,1d} in H/s
      trend: ringTrend(rk),                   // last 5 COMPLETE minutes, H/s
      idle: nowS - ch.last > 300,
      best: ch.best, last: ch.last,
      firstSeen: ch.firstSeen,
      uptime: nowS - ch.firstSeen,
    });
  }
  return {
    enabled: !!readSv2Address(), workers: workerList.length,
    hs: totHs, accepted, rejected, best,
    roundWork: sv2State.roundWork, workerList,
  };
}

// pool.status is several JSON objects, one per line — merge them all.
function readPoolStatus() {
  const f = path.join(POOL_LOGDIR, 'pool', 'pool.status');
  const raw = fs.readFileSync(f, 'utf8');
  const merged = {};
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (!t.startsWith('{')) continue;
    try { Object.assign(merged, JSON.parse(t)); } catch (_) {}
  }
  return merged;
}

// per-user files live in logdir/users/<address>; each lists its workers.
// ---- per-worker reset masking (fallback when share logging is off) ----
const BASELINE_FILE = path.join(POOL_DIR, 'config', 'best_baselines.json');
function readBaselines() { try { return JSON.parse(fs.readFileSync(BASELINE_FILE, 'utf8')) || {}; } catch (_) { return {}; } }
function writeBaselines(b) { try { fs.mkdirSync(path.dirname(BASELINE_FILE), { recursive: true }); fs.writeFileSync(BASELINE_FILE, JSON.stringify(b)); } catch (_) {} }
// ---- SHARELOG_REBUILD: live per-worker best-since-reset from asicseer's -L sharelog ----
const LOGSHARES_FLAG = path.join(POOL_DIR, 'config', 'logshares');
const RESET_STATE_FILE = path.join(POOL_DIR, 'config', 'best_resets.json');
const shareBest = {};      // worker -> best sdiff since its reset (in-memory)
const SHAREBEST_FILE = path.join(POOL_DIR, 'config', 'best_values.json');
function writeShareBest() { try { fs.mkdirSync(path.dirname(SHAREBEST_FILE), { recursive: true }); fs.writeFileSync(SHAREBEST_FILE, JSON.stringify(shareBest)); } catch (_) {} }
try { Object.assign(shareBest, JSON.parse(fs.readFileSync(SHAREBEST_FILE, 'utf8')) || {}); } catch (_) {}
const fileSeen = {};       // sharelog path -> last size consumed (skip unchanged)
function logSharesOn() { try { return fs.existsSync(LOGSHARES_FLAG); } catch (_) { return false; } }
function setLogShares(on) { try { fs.mkdirSync(path.dirname(LOGSHARES_FLAG), { recursive: true }); if (on) fs.writeFileSync(LOGSHARES_FLAG, '1'); else fs.rmSync(LOGSHARES_FLAG, { force: true }); } catch (_) {} }
function readResetState() { try { return JSON.parse(fs.readFileSync(RESET_STATE_FILE, 'utf8')) || {}; } catch (_) { return {}; } }
function writeResetState(s) { try { fs.mkdirSync(path.dirname(RESET_STATE_FILE), { recursive: true }); fs.writeFileSync(RESET_STATE_FILE, JSON.stringify(s)); } catch (_) {} }
function shortName(wn) { wn = String(wn || ''); const p = wn.split('.'); return p.slice(1).join('.') || wn; }
function findSharelogs() {
  const out = []; const seen = new Set();
  const walk = (d, depth) => {
    if (depth > 4) return;
    let ents = []; try { ents = fs.readdirSync(d, { withFileTypes: true }); } catch (_) { return; }
    for (const e of ents) {
      const fp = path.join(d, e.name);
      if (e.isDirectory()) walk(fp, depth + 1);
      else if (e.name.endsWith('.sharelog') && !seen.has(fp)) {
        seen.add(fp);
        let st; try { st = fs.statSync(fp); } catch (_) { continue; }
        out.push({ path: fp, mtime: Math.floor(st.mtimeMs / 1000), size: st.size });
      }
    }
  };
  walk(POOL_LOGDIR, 0);
  return out;
}
function shareTs(j) { return parseInt(String(j.createdate || '').split(',')[0], 10) || 0; }
function tailSharelogs() {
  if (!logSharesOn()) return;
  const rs = readResetState(); const keys = Object.keys(rs);
  if (!keys.length) return;
  const minReset = Math.min.apply(null, keys.map(k => Number(rs[k]) || Infinity));
  let dirty = false;
  for (const f of findSharelogs()) {
    if (f.mtime < minReset - 5) continue;
    if (fileSeen[f.path] === f.size) continue;
    fileSeen[f.path] = f.size;
    let raw; try { raw = fs.readFileSync(f.path, 'utf8'); } catch (_) { continue; }
    for (const line of raw.split('\n')) {
      const t = line.trim(); if (!t) continue;
      let j; try { j = JSON.parse(t); } catch (_) { continue; }
      if (j.result !== true) continue;
      const sn = shortName(j.workername); const at = rs[sn]; if (at == null) continue;
      const ts = shareTs(j); if (ts && ts < at) continue;
      const sd = Number(j.sdiff) || 0; if (sd > (shareBest[sn] || 0)) { shareBest[sn] = sd; dirty = true; }
    }
  }
  if (dirty) writeShareBest();
}
function pruneSharelogs() {
  if (!logSharesOn()) return;
  const rs = readResetState(); const keys = Object.keys(rs);
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - 120;  // consumed files; running max is persisted in best_values.json
  for (const f of findSharelogs()) {
    if (f.mtime < cutoff) { try { fs.rmSync(f.path, { force: true }); delete fileSeen[f.path]; } catch (_) {} }
  }
}
function purgeAllSharelogs() { for (const f of findSharelogs()) { try { fs.rmSync(f.path, { force: true }); } catch (_) {} } for (const k of Object.keys(fileSeen)) delete fileSeen[k]; }
setInterval(tailSharelogs, 3000);
setInterval(pruneSharelogs, 120000);
function readWorkers() {
  const out = [];
  const dir = path.join(POOL_LOGDIR, 'users');
  let files = [];
  try { files = fs.readdirSync(dir); } catch (_) { return out; }
  for (const name of files) {
    let u;
    try { u = JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8')); } catch (_) { continue; }
    const ws = Array.isArray(u.worker) ? u.worker : [];
    for (const w of ws) {
      const wn = (w.workername || name).split('.').slice(1).join('.') || (w.workername || name);
      // UNIFIED SCHEMA -- identical field set to the SV2 rows in sv2Stats().
      // Previously `trend` here was [1m,5m,1hr,1d,7d] window averages, which
      // the UI drew as if it were a time series next to SV2's real 1-minute
      // buckets. Now both feed the same per-minute rings.
      const rk = ringKey('SV1', wn);
      ringPut(rk, hashToHs(w.hashrate1m));
      const winFromPool = {
        '1m': hashToHs(w.hashrate1m), '5m': hashToHs(w.hashrate5m),
        '1h': hashToHs(w.hashrate1hr), '1d': hashToHs(w.hashrate1d),
      };
      out.push({
        name: wn, proto: 'SV1',
        conns: 1,
        declared: null,
        // asicseer's per-user file carries an accepted counter but no
        // per-worker reject counter. Report null rather than a fake 0: a
        // dash tells the truth, a zero claims a fact we do not have.
        accepted: ('shares' in w) ? (Number(w.shares) || 0) : null,
        rejected: ('rejects' in w) ? (Number(w.rejects) || 0)
                : ('rejected' in w) ? (Number(w.rejected) || 0) : null,
        rejectReasons: null,
        hashrate: (() => { const h = parseHash(w.hashrate5m || w.hashrate1m); return h.val + ' ' + h.unit; })(),
        // asicseer's own windows are authoritative for SV1; rings only supply
        // the sparkline, so a fresh dashboard is not blind for an hour.
        hs: winFromPool,
        trend: ringTrend(rk),
        idle: (Math.floor(Date.now() / 1000) - (Number(w.lastshare) || 0)) > 300,
        best: Number(w.bestshare) || 0,
        last: Number(w.lastshare) || 0,
        firstSeen: 0,
      });
    }
  }
  out.sort((a, b) => b.best - a.best);
  // Inactive miners fall off the roster after ~1h of no shares. asicseer-pool keeps
  // a per-user file around indefinitely, so without this a powered-off ASIC lingers
  // forever. Override with WORKER_TTL_SEC if you want a different window.
  const WORKER_TTL = workerTtlSec();
  const nowS = Math.floor(Date.now() / 1000);
  let live = out.filter(w => (w.last || 0) > 0 && (nowS - w.last) <= WORKER_TTL);
  // hidden workers: stay hidden unless they show fresh activity
  let result = live;
  try {
    const hf = path.join(POOL_DIR, 'config', 'hidden.json');
    let hidden = {};
    try { hidden = JSON.parse(fs.readFileSync(hf, 'utf8')); } catch (_) {}
    let changed = false;
    const vis = live.filter(w => {
      const t = hidden[w.name];
      if (!t) return true;
      if ((w.last || 0) > t) { delete hidden[w.name]; changed = true; return true; }
      return false;
    });
    if (changed) { try { fs.writeFileSync(hf, JSON.stringify(hidden)); } catch (_) {} }
    result = vis;
  } catch (_) { result = live; }

  // Per-worker session uptime. asicseer-pool doesn't expose a connect time, so we
  // track first-seen ourselves: record when a worker shows up, and drop the record
  // when it falls off the roster — so a reconnect starts a fresh session.
  try {
    const sf = path.join(POOL_DIR, 'config', 'sessions.json');
    try { fs.mkdirSync(path.dirname(sf), { recursive: true }); } catch (_) {}
    let sessions = {};
    try { sessions = JSON.parse(fs.readFileSync(sf, 'utf8')); } catch (_) {}
    const next = {};
    for (const w of result) {
      const started = sessions[w.name] || nowS;
      next[w.name] = started;
      w.uptime = nowS - started;        // seconds connected this session
    }
    const nextStr = JSON.stringify(next);
    try { if (nextStr !== JSON.stringify(sessions)) fs.writeFileSync(sf, nextStr); } catch (_) {}
  } catch (_) {}
  const _bl = readBaselines();
  const _rs = readResetState();
  const _ls = logSharesOn();
  for (const w of result) {
    w.rawBest = w.best;
    if (_ls && _rs[w.name] != null) w.best = Number(shareBest[w.name]) || 0;
    else if (w.best <= (Number(_bl[w.name]) || 0)) w.best = 0;
  }
  result.sort((a, b) => b.best - a.best);
  return result;
}

// count solved blocks if asicseer-pool logged any (logdir/pool/blocks.log)
function countBlocks() {
  try {
    const f = path.join(POOL_LOGDIR, 'pool', 'blocks.log');
    return fs.readFileSync(f, 'utf8').split('\n').filter(l => l.trim()).length;
  } catch (_) { return 0; }
}

// Pull the real block-solving share diff out of the asicseer-pool log, e.g.
//   "Solved block 954947 ..."  near  "... solve ... diff 1972695353385 !"
// Returns 0 if no readable log / no match (caller falls back to the block's net diff).
// asicseer writes a JSON file per solved block at logdir/pool/blocks/<height>.<state>
// with "solution_difficulty" = the winning share's diff. That's the real best diff
// submitted (asicseer's console log isn't written to a findable file, only stdout).
function solveDiffFromBlocks(height) {
  for (const ext of ['confirmed', 'unconfirmed', 'orphaned']) {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(POOL_LOGDIR, 'pool', 'blocks', height + '.' + ext), 'utf8'));
      const sd = Number(j.solution_difficulty);
      if (sd > 0) return Math.round(sd);
    } catch (_) {}
  }
  return 0;
}
function solveDiffFromLog(height) {
  const files = [
    path.join(POOL_LOGDIR, 'pool.log'),
    path.join(POOL_LOGDIR, 'pool', 'pool.log'),
    path.join(POOL_DIR, 'pool.log'),
  ];
  for (const f of files) {
    let lines;
    try { lines = fs.readFileSync(f, 'utf8').split('\n'); } catch (_) { continue; }
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].indexOf('Solved block ' + height) === -1) continue;
      for (let j = Math.max(0, i - 5); j <= Math.min(lines.length - 1, i + 3); j++) {
        const m = lines[j].match(/diff\s+([0-9]+(?:\.[0-9]+)?)/i);
        if (m) return Math.round(parseFloat(m[1]));
      }
    }
  }
  return 0;
}

// SV2 pool log format differs from ckpool's: the block-found line is
//   "... 💰 Block Found!!! 💰<blockhash>"
// and carries no difficulty. The solving share is the immediately-preceding
//   "valid share | ... channel_id: N ... share_work: <diff>"
// on the SAME channel. share_work is the real solve difficulty -- which is
// what "Best Diff Submitted" is supposed to show, NOT network difficulty.
// Chris correctly flagged that SV2 rows were showing net diff.
function sv2SolveDiffFromLog(hash) {
  if (!hash) return 0;
  const files = [
    path.join(SV2_DIR, 'pool_sv2.log'),
    path.join(POOL_DIR, 'pool_sv2.log'),
    path.join(POOL_LOGDIR, 'pool_sv2.log'),
  ];
  const shortH = hash.replace(/^0+/, '');
  for (const f of files) {
    let lines;
    try { lines = fs.readFileSync(f, 'utf8').split('\n'); } catch (_) { continue; }
    for (let i = 0; i < lines.length; i++) {
      const fl = lines[i];
      if (fl.indexOf('Block Found') === -1) continue;
      if (fl.indexOf(hash) === -1 && (!shortH || fl.indexOf(shortH) === -1)) continue;
      const chM = fl.match(/channel_id[:=\s]+(\d+)/i);
      // walk back for the most recent valid share; prefer the same channel
      let anyWork = 0;
      for (let j = i; j >= Math.max(0, i - 40); j--) {
        if (lines[j].indexOf('valid share') === -1) continue;
        const wM = lines[j].match(/share_work[:=\s]+([0-9]+(?:\.[0-9]+)?)/i);
        if (!wM) continue;
        const work = Math.round(parseFloat(wM[1]));
        if (!anyWork) anyWork = work;
        if (chM) {
          const cM = lines[j].match(/channel_id[:=\s]+(\d+)/i);
          if (cM && cM[1] === chM[1]) return work;   // exact solving share
        } else {
          return work;
        }
      }
      if (anyWork) return anyWork;                   // fallback: nearest share
    }
  }
  return 0;
}

// minimal JSON-RPC call to BCHN
function rpc(method, params = []) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ jsonrpc: '1.0', id: 'ssc', method, params });
    const req = http.request({
      host: RPC_HOST, port: RPC_PORT, method: 'POST', path: '/',
      auth: `${RPC_USER}:${RPC_PASS}`,
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      timeout: 4000,
    }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { const j = JSON.parse(d); j.error ? reject(j.error) : resolve(j.result); } catch (e) { reject(e); } });
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('rpc timeout')));
    req.write(body); req.end();
  });
}


// ---- winning-worker lookup (best-effort) -----------------------------------
function solveWorkerFromBlocks(height) {
  for (const ext of ['confirmed', 'unconfirmed', 'orphaned']) {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(POOL_LOGDIR, 'pool', 'blocks', height + '.' + ext), 'utf8'));
      const w = j.solvedby || j.workername || j.worker || j.username;
      if (w) return shortName(String(w));
    } catch (_) {}
  }
  return null;
}
function solveWorkerFromLog(height) {
  const files = [path.join(POOL_LOGDIR, 'pool.log'), path.join(POOL_LOGDIR, 'pool', 'pool.log'), path.join(POOL_DIR, 'pool.log')];
  const re = new RegExp('Solved block ' + height + ' by (\\S+)');
  for (const f of files) {
    let lines; try { lines = fs.readFileSync(f, 'utf8').split('\n'); } catch (_) { continue; }
    for (let i = lines.length - 1; i >= 0; i--) { const m = lines[i].match(re); if (m) return shortName(m[1]); }
  }
  return null;
}
// ---- found-block scanner ---------------------------------------------------
// Engine-agnostic: watches the chain itself for blocks whose coinbase pays the
// configured payout address. Persists to /pool/config/blocks.json.
const BLOCKS_FILE = path.join(POOL_DIR, 'config', 'blocks.json');
let blockState = { lastScanned: 0, blocks: [] };
let lastAcceptedTotal = 0;
try { blockState = JSON.parse(fs.readFileSync(BLOCKS_FILE, 'utf8')); } catch (_) {}
let lastBestSeen = 0;

function saveBlocks() {
  try {
    fs.mkdirSync(path.dirname(BLOCKS_FILE), { recursive: true });
    fs.writeFileSync(BLOCKS_FILE, JSON.stringify(blockState));
  } catch (_) {}
}
function addrKey(a) { return String(a || '').toLowerCase().replace(/^(bitcoincash|bchtest|bchreg):/, ''); }

// A stored payout address and the address BCHN reports in a coinbase vout can
// be the same destination in two spellings: BCHN reports cashaddr, but users
// paste legacy addresses (a real one in the field: 1QJr...). addrKey() string
// comparison can never match across the two, so the chain scan silently missed
// blocks. Ask the node to normalize: validateaddress echoes the canonical
// form; if it echoes the input unchanged this is a harmless no-op.
async function addrKeysFor(stored) {
  const keys = new Set();
  const k = addrKey(stored);
  if (k) keys.add(k);
  if (stored) {
    try {
      const v = await rpc('validateaddress', [stored]);
      if (v && v.isvalid && v.address) keys.add(addrKey(v.address));
    } catch (_) { /* node not ready -- raw key still applies */ }
  }
  return keys;
}

async function coinbasePaysUs(hash, mine) {
  const blk = await rpc('getblock', [hash, 1]);
  const cbTxid = blk.tx && blk.tx[0];
  if (!cbTxid) return null;
  const tx = await rpc('getrawtransaction', [cbTxid, true, hash]);
  for (const v of (tx.vout || [])) {
    const spk = v.scriptPubKey || {};
    const addrs = spk.addresses || (spk.address ? [spk.address] : []);
    if (addrs.some(a => mine.has(addrKey(a)))) return { time: blk.time, height: blk.height, netdiff: Number(blk.difficulty) || 0 };
  }
  return null;
}

let scanning = false;
// Surface blocks straight from asicseer's own solve files
// (logdir/pool/blocks/<height>.<state>). These are written ONLY when THIS pool
// solves a block, so they are authoritative even when the block paid an address
// other than the one currently configured (a different HD address, or a miner's
// username such as NiceHash). Without this, such a block is invisible because the
// chain scanner only matches the configured payout address.
function healFromBlockFiles() {
  const dir = path.join(POOL_LOGDIR, 'pool', 'blocks');
  let files = [];
  try { files = fs.readdirSync(dir); } catch (_) { return; }
  for (const f of files) {
    const m = f.match(/^(\d+)\.(confirmed|unconfirmed|orphaned)$/);
    if (!m) continue;
    const h = Number(m[1]);
    if (!Number.isFinite(h)) continue;
    let j;
    try { j = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')); } catch (_) { continue; }
    const sd = Number(j.solution_difficulty) || null;
    const worker = j.solvedby ? shortName(String(j.solvedby)) : null;
    const existing = blockState.blocks.find((b) => b.height === h);
    if (existing) {
      if (!existing.worker && worker) existing.worker = worker;
      if (!existing.solveDiff && sd) existing.solveDiff = sd;
      if (sd && sd > (existing.best || 0)) existing.best = sd;
      if (!existing.hash && j.hash) existing.hash = j.hash;
      existing.state = m[2];
      continue;
    }
    blockState.blocks.push({
      height: h,
      hash: j.hash || null,
      // asicseer block-file 'time' units are not guaranteed: an epoch-ms
      // value made b.time ~1.78e12, which slid past the freshness gate
      // (now - b.time < 900 is true for huge b.time) while making the
      // declaration matcher's timestamp distance astronomical -> null ->
      // snapshot fallback -> Chris's 302.9%% round stamped as 3.4%%.
      time: (() => { let t = Number(j.time) || 0; if (t > 1e12) t = Math.floor(t / 1000); return t || Math.floor(Date.now() / 1000); })(),
      best: sd || 0,
      solveDiff: sd,
      netdiff: Number(j.network_difficulty) || null,
      worker,
      state: m[2],
      healed: true,
    });
    console.log(`[LoneStrike Cash] BLOCK from pool file: height ${h} (${m[2]})${worker ? ' by ' + worker : ''}`);
  }
}

// The bridge appends to sv2_blocks.jsonl at the moment the node ACCEPTS a
// submitblock -- it is the most authoritative "we found a block" record that
// exists. It was previously only used to annotate blocks the chain-scan had
// already found, so if the scan missed (address-form mismatch, node hiccup,
// anything), the block silently never appeared: Chris's 959807 sat in the
// jsonl while Blocks Found stayed at 2. Upsert, never just decorate.
function mergeSv2FoundBlocks() {
  let added = 0;
  for (const rec of sv2Blocks()) {
    if (!(rec.height > 0)) continue;
    const b = blockState.blocks.find((x) =>
      (rec.hash && x.hash === rec.hash) || x.height === rec.height);
    if (b) {
      if (!b.worker) b.worker = 'SV2';
      if (!b.hash && rec.hash) b.hash = rec.hash;
      continue;
    }
    const sd = sv2SolveDiffFromLog(rec.hash) || solveDiffFromBlocks(rec.height);
    blockState.blocks.push({
      height: rec.height,
      hash: rec.hash || null,
      time: Number(rec.time) || Math.floor(Date.now() / 1000),
      best: sd || 0,          // real solve diff if known; heal loop may fill later
      solveDiff: sd || null,  // stays null rather than borrowing netdiff
      netdiff: null,
      worker: 'SV2',
      state: (rec.result === 'accepted' || rec.result === 'duplicate')
        ? 'confirmed' : String(rec.result || 'submitted'),
      healed: true,
      source: 'sv2-bridge',
    });
    added++;
    console.log(`[LoneStrike Cash] BLOCK from SV2 bridge record: height ${rec.height}`);
  }
  if (added) saveBlocks();
  return added;
}

const PROC_START = Date.now();
// asicseer declares the true round effort at the moment of solve:
//   "Block solved after 47520286401 shares at 11.1% effort"
// That line is authoritative. The dashboard-side snapshot can lie when
// detection lags the solve (asicseer resets its own round instantly, so a
// late snapshot measures the NEW round -- Chris's block stamped 0.1% when
// the pool itself said 11.1%). Parse the declaration, matched by timestamp.
// Solve declarations must survive log churn: the asicseer status ticker
// writes ~1 line/second, so a 256KB tail reaches back about an hour -- a
// user who updates tomorrow would find yesterday's declaration already
// scrolled away and the heal would have nothing to work with. Capture
// every declaration into a durable store within seconds of it appearing.
const SV1_DECL_FILE = path.join(SV2_DIR, 'solve_declarations.json');
let sv1Decls = (() => { try { return JSON.parse(fs.readFileSync(SV1_DECL_FILE, 'utf8')) || []; } catch (_) { return []; } })();
function sv1DeclScan() {
  for (const f of [path.join(POOL_LOGDIR, 'pool', 'pool.log'),
                   path.join(POOL_LOGDIR, 'pool.log')]) {
    let txt;
    try {
      const st = fs.statSync(f);
      const fd = fs.openSync(f, 'r');
      const sz = Math.min(st.size, 262144);
      const buf = Buffer.alloc(sz);
      fs.readSync(fd, buf, 0, sz, st.size - sz);
      fs.closeSync(fd);
      txt = buf.toString('utf8');
    } catch (_) { continue; }
    const re = /\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})[^\]]*\] Block solved after (\d+) shares at ([0-9.]+)% effort/g;
    let m, added = false;
    while ((m = re.exec(txt))) {
      const ts = Date.parse(m[1].replace(' ', 'T') + 'Z') / 1000;
      if (!sv1Decls.some((d) => d.ts === ts)) {
        sv1Decls.push({ ts, shares: Number(m[2]) || 0, pct: parseFloat(m[3]) });
        added = true;
      }
    }
    if (added) {
      sv1Decls.sort((a, b) => a.ts - b.ts);
      if (sv1Decls.length > 500) sv1Decls = sv1Decls.slice(-500);
      try { fs.writeFileSync(SV1_DECL_FILE, JSON.stringify(sv1Decls)); } catch (_) {}
    }
  }
}
setInterval(() => { try { sv1DeclScan(); } catch (_) {} }, 30000);
try { sv1DeclScan(); } catch (_) {}

function sv1SolveEffortFromLog(blockTime) {
  let bt = Number(blockTime) || 0;
  if (bt > 1e12) bt = Math.floor(bt / 1000);          // ms-epoch defence
  if (!(bt > 0)) bt = Math.floor(Date.now() / 1000);  // attach runs seconds after solve
  blockTime = bt;
  // durable store first: complete history, immune to ticker churn
  let sBest = null, sDt = Infinity;
  for (const d of sv1Decls) {
    const dt = Math.abs(d.ts - blockTime);
    if (dt < sDt) { sDt = dt; sBest = d.pct; }
  }
  if (sBest != null && sDt < 900) return sBest;
  for (const f of [path.join(POOL_LOGDIR, 'pool', 'pool.log'),
                   path.join(POOL_LOGDIR, 'pool.log')]) {
    let txt;
    try {
      const st = fs.statSync(f);
      const fd = fs.openSync(f, 'r');
      const sz = Math.min(st.size, 262144);
      const buf = Buffer.alloc(sz);
      fs.readSync(fd, buf, 0, sz, st.size - sz);
      fs.closeSync(fd);
      txt = buf.toString('utf8');
    } catch (_) { continue; }
    let best = null, bestDt = Infinity;
    const re = /\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})[^\]]*\] Block solved after \d+ shares at ([0-9.]+)% effort/g;
    let m;
    while ((m = re.exec(txt))) {
      const ts = Date.parse(m[1].replace(' ', 'T') + 'Z') / 1000;
      const dt = Math.abs(ts - blockTime);
      if (dt < bestDt) { bestDt = dt; best = parseFloat(m[2]); }
    }
    if (best != null && bestDt < 900) return best;
  }
  return null;
}

async function scanBlocks() {
  if (scanning) return;
  scanning = true;
  try {
    const mineKeys = await addrKeysFor(readAddress());
    for (const k of await addrKeysFor(readSv2Address())) mineKeys.add(k);
    if (!mineKeys.size) return;
    const tip = await rpc('getblockcount', []);
    if (!blockState.lastScanned) blockState.lastScanned = Math.max(0, tip - 200); // first-run backfill
    let h = blockState.lastScanned + 1;
    let budget = 50;                                   // be gentle per pass
    while (h <= tip && budget-- > 0) {
      const hash = await rpc('getblockhash', [h]);
      const hit = await coinbasePaysUs(hash, mineKeys);
      if (hit && !blockState.blocks.some(b => b.hash === hash)) {
        const solveDiff = solveDiffFromBlocks(h) || sv2SolveDiffFromLog(hash) || solveDiffFromLog(h);
        // Reddit field report (2026-08): a user mined the SAME payout address
        // on a remote pool; the remote pool found the block, and this scan --
        // which matches coinbase outputs by address -- celebrated it as ours.
        // The address-match proves the block PAID us, not that we FOUND it.
        // Local evidence (a pool solve record or a solve declaration near the
        // block time) is what proves authorship.
        const localEvidence = solveDiff ||
          sv1Decls.some((d) => Math.abs(d.ts - (hit.time || 0)) < 900);
        const ext = !localEvidence;
        // best === the submitted solve difficulty, or 0 (shown as a dash).
        // NEVER borrow network difficulty here: that is what made SV2 rows
        // read 449G/460G, i.e. net diff mislabeled as a submitted share.
        blockState.blocks.push({ height: h, hash, time: hit.time, best: solveDiff || 0, solveDiff: solveDiff || null, netdiff: hit.netdiff || null, worker: solveWorkerFromBlocks(h) || solveWorkerFromLog(h) || (ext ? 'external' : null), external: ext || undefined, healed: true });
        blockState.acceptedAtLastBlock = lastAcceptedTotal || 0;
        console.log(`[LoneStrike Cash] BLOCK ${ext ? 'PAID (external solve)' : 'FOUND'} at height ${h} (${hash})`);
      }
      blockState.lastScanned = h; h++;
    }
    // Heal older entries stamped with the rolling best (e.g. 148G) instead of the
    // real solving diff. Prefer the pool log; fall back to the block's net diff.
    for (const b of blockState.blocks) {
      if (b.solveDiff || b.recheck) continue;
      const sd = solveDiffFromBlocks(b.height) || solveDiffFromLog(b.height);
      let val = sd;
      if (!val && b.hash) {
        try { const blk = await rpc('getblock', [b.hash, 1]); val = Number(blk.difficulty) || 0; } catch (_) {}
      }
      if (val && val > (b.best || 0)) b.best = val;
      if (sd) b.solveDiff = sd;
      b.recheck = true;
    }
    for (const b of blockState.blocks) {
      if (b.worker) continue;
      const w = solveWorkerFromBlocks(b.height) || solveWorkerFromLog(b.height);
      if (w) b.worker = w;
    }
    mergeSv2FoundBlocks();
    healFromBlockFiles();
    saveBlocks();
  } catch (_) { /* node not ready — retry next pass */ }
  finally { scanning = false; }
}
setInterval(scanBlocks, 60 * 1000);
setTimeout(scanBlocks, 8 * 1000);
// ---- api -----------------------------------------------------------------
app.get('/api/status', async (_req, res) => {
  const out = {
    poolUp: false, nodeUp: false,
    hashrate: { val: '0', unit: 'H/s' }, workers: 0, users: 0,
    accepted: 0, rejected: 0, bestShare: 0, blocks: 0,
    netDiff: 0, height: 0, chain: 'main',
    stratum: `stratum+tcp://${STRATUM_HOST}:${STRATUM_PORT}`,
    workerList: [], version: VERSION, address: readAddress(),
    blockList: [...blockState.blocks].sort((a, b) => b.height - a.height).slice(0, 200),
    diff: (() => { try {
      const [mi, st, mx] = fs.readFileSync(path.join(POOL_DIR, 'config', 'diff'), 'utf8').trim().split(/\s+/).map(Number);
      return { min: mi || 1, start: st || 42, max: mx || 0 };
    } catch (_) { return { min: 1, start: 42, max: 0 }; } })(),
  };

  try {
    const p = readPoolStatus();
    out.poolUp   = true;
    out.users    = Number(p.Users) || 0;
    out.workers  = Number(p.Workers) || 0;
    out.accepted = Number(p.accepted) || 0;
    out.rejected = Number(p.rejected) || 0;
    out.bestShare = Number(p.bestshare) || 0;
    lastBestSeen = out.bestShare;
    const h = parseHash(p.hashrate5m || p.hashrate1m);
    out.hashrate = { val: h.val, unit: h.unit };
    lastAcceptedTotal = out.accepted;
    // asicseer-pool zeroes 'accepted' (accounted_diff_shares) on every block solve
    // via reset_bestshares(), so pool.status 'accepted' is already the current
    // round's share total. Subtracting a stale acceptedAtLastBlock baseline pinned
    // effort at 0% after a solve until 'accepted' re-climbed past it. Mirror the
    // pool's own effort calc (accounted_diff_shares / network_diff) and use it directly.
    sv2State._sv1AccRaw = out.accepted;
    if (out.accepted < sv1RoundBase) sv2SetRoundStart(sv2RoundStartTs, 0);  // asicseer self-reset (SV1 solve)
    out.roundShares = Math.max(0, out.accepted - sv1RoundBase);
    const hs = hashToHs(p.hashrate5m || p.hashrate1m);
    out.poolHs = hs;
    out.workerList = readWorkers();
    out.logShares = logSharesOn();
    out.blocks = Math.max(blockState.blocks.length, countBlocks());
  } catch (_) { /* pool not started yet */ }

  try {
    const s2 = sv2Stats();
    const nBefore = blockState.blocks.length;
    const upserted = mergeSv2FoundBlocks();
    try { healFromBlockFiles(); } catch (_) {}   // SV1 solves land as files instantly
    if (upserted || blockState.blocks.length !== nBefore) {
      out.blockList = [...blockState.blocks].sort((a, b) => b.height - a.height).slice(0, 200);
      out.blocks = Math.max(blockState.blocks.length, out.blocks || 0);
    }
    out.sv2 = {
      enabled: s2.enabled, workers: s2.workers, xn: readSv2Xn(),
      hashrate: (() => { const h = fmtHs(s2.hs); return h.val + ' ' + h.unit; })(),
      accepted: s2.accepted, rejected: s2.rejected, best: s2.best,
    };
    if (s2.enabled || s2.workers) {
      out.poolUp = out.poolUp || s2.workers > 0;
      out.workers += s2.workers;
      if (s2.workers) out.users += 1;
      out.accepted += Math.round(sv2State.allDiff); out.rejected += s2.rejected;
      out.bestShare = Math.max(out.bestShare, s2.best);
      out.poolHs = (out.poolHs || 0) + s2.hs;
      const th = fmtHs(out.poolHs);
      out.hashrate = { val: th.val, unit: th.unit };
    }
    // Chris (2026-08-02): block detection, the round reset, and the effort
    // snapshot lived inside the SV2-only branch above -- an SV1-only
    // deployment (his fleet, the day he dropped the translator) never ran
    // ANY of it: two blocks landed with no effort, and the heal (gated
    // downstream on pendingEffortShares) could never fill old dashes
    // either. A found block is a pool event, not an SV2 event.
    if (out.blockList.length !== sv2State.lastBlockCount) {
        // an EXTERNAL block (paid our address, solved elsewhere) must not
        // reset the local round: our miners' shares are still climbing
        // toward OUR block
        const localCount = out.blockList.filter((b) => !b.external).length;
        if (localCount === (sv2State.lastLocalCount ?? localCount) && sv2State.lastBlockCount >= 0) {
          sv2State.lastBlockCount = out.blockList.length;
          sv2State.lastLocalCount = localCount;
        } else if (sv2State.lastBlockCount >= 0) {
          console.log('[LoneStrike Cash] local block detected: round + bests reset (' + (sv2State.lastLocalCount ?? '?') + ' -> ' + localCount + ' local blocks)');
          // Chris: record the round effort this block was found at. netDiff is
          // computed later in this merge, so snapshot the shares now (before
          // the round zeroes) and attach the percentage once netDiff exists.
          // asicseer resets its own round the instant it solves; whether this
          // merge's pool.status read is pre- or post-reset is a coin flip.
          // The PREVIOUS poll's total (5s earlier) is always pre-reset.
          sv2State.pendingEffortShares = Math.max(
            (out.roundShares || 0) + Math.max(sv2State.roundWork, sv2State.roundDiff),
            sv2State.prevRoundTotal || 0);
          sv2State.roundWork = 0; sv2State.roundDiff = 0;
          sv2SetRoundStart(Math.floor(Date.now() / 1000), sv2State._sv1AccRaw ?? null);
          for (const c of Object.values(sv2State.channels)) { c.roundAcc = 0; c.accountedRound = 0; }
          // Chris #2: a block is a FLEET event. asicseer clears SV1's best
          // itself when SV1 solves, but SV2's best survived, so the combined
          // "best diff" stayed pinned to a stale SV2 value and never dropped
          // for the new round. Whichever protocol solved, clear both.
          try { sv2ApplyReset('all'); } catch (_) {}
          try {
            fs.mkdirSync(path.join(POOL_DIR, 'config'), { recursive: true });
            fs.writeFileSync(path.join(POOL_DIR, 'config', 'reset_request'), 'all');
          } catch (_) {}
        }
        sv2State.lastBlockCount = out.blockList.length;
        sv2State.lastLocalCount = out.blockList.filter((b) => !b.external).length;
      } else {
        sv2State.lastBlockCount = out.blockList.length;
        sv2State.lastLocalCount = out.blockList.filter((b) => !b.external).length;
      }
    out.roundShares = (out.roundShares || 0) + Math.max(sv2State.roundWork, sv2State.roundDiff);
    sv2State.prevRoundTotal = out.roundShares;
    try { sv2RoundSample(out.roundShares || 0); } catch (_) {}
    if (s2.enabled || s2.workers) {
      // aggregate: rental/proxy fleets fan one identity across many channels;
    // collapse same-name SV2 rows into a single row (sum hs+shares, max best,
    // freshest last, count connections) so 132 Braiins channels read as one
    const merged = {};
    const passthrough = [];
    for (const w of s2.workerList) {
      if (w.proto !== 'SV2') { passthrough.push(w); continue; }
      const k = w.name;
      if (!merged[k]) { merged[k] = Object.assign({}, w, { conns: 1, _hs: hashToHs(w.hashrate) || 0 }); }
      else {
        const m = merged[k];
        m.conns++;
        m._hs += hashToHs(w.hashrate) || 0;
        m.best = Math.max(m.best || 0, w.best || 0);
        // unified counters/windows aggregate across a fleet's many channels
        if (typeof w.accepted === 'number') m.accepted = (m.accepted || 0) + w.accepted;
        if (typeof w.rejected === 'number') m.rejected = (m.rejected || 0) + w.rejected;
        if (w.hs && m.hs) for (const k2 of ['1m', '5m', '1h', '1d']) m.hs[k2] = (m.hs[k2] || 0) + (w.hs[k2] || 0);
        if (Array.isArray(w.trend) && Array.isArray(m.trend)) m.trend = m.trend.map((v, i) => (v || 0) + (w.trend[i] || 0));
        if (w.firstSeen && (!m.firstSeen || w.firstSeen < m.firstSeen)) m.firstSeen = w.firstSeen;
        m.last = Math.max(m.last || 0, w.last || 0);
        m.rejected = (m.rejected || 0) + (w.rejected || 0);
        m.uptime = Math.max(m.uptime || 0, w.uptime || 0);
        m.idle = m.idle && w.idle;
        if (w.declared === 0) m.declared = 0;
      }
    }
    // Chris (2026-08-31, Braiins rental): hundreds of short-lived connections
    // for one worker name made the aggregated row's rate swing (cold windows
    // on fresh channels, warm tails on dead ones). Recompute each merged
    // name's hashrate from the raw share ring across ALL its channels --
    // one window, one truth, immune to connection churn.
    const nameOfCid = {};
    for (const [cid2, ch2] of Object.entries(sv2State.channels)) nameOfCid[cid2] = ch2.name;
    const nowMs2 = Date.now();
    for (const m of Object.values(merged)) {
      let work = 0;
      for (const [ts2, w2, c2] of sv2State.shares) {
        if (nameOfCid[c2] === m.name && nowMs2 - ts2 <= 300000) work += w2;
      }
      if (work > 0) m._hs = work * 4294967296 / 300;
    }
    const aggregated = Object.values(merged).map((m) => {
      if (m.conns > 1) { const h = fmtHs(m._hs); m.hashrate = h.val + ' ' + h.unit; m.trend = [m._hs,m._hs,m._hs,m._hs,m._hs]; }
      return m;
    });
    out.workerList = out.workerList.concat(passthrough, aggregated).sort((a, b) => (b.best || 0) - (a.best || 0));
    }
  } catch (_) { /* sv2 telemetry unavailable */ }

  try {
    const info = await rpc('getblockchaininfo');
    out.nodeUp  = true;
    out.height  = info.blocks;
    out.chain   = info.chain;
    out.netDiff = Number(info.difficulty) || 0;
    if (out.netDiff > 0) {
      const nowS2 = Math.floor(Date.now() / 1000);
      let changed = false;
      // best-diff heal: a solve's achieved diff can never be BELOW the
      // network difficulty it beat; recompute impossible bests from the
      // block's own hash (fixes the 804K share_work stamp, retroactively)
      for (const b of blockState.blocks) {
        const nd = Number(b.netdiff) || out.netDiff;
        if (b.hash && nd > 0 && (!(b.best > 0) || b.best < nd)) {
          const ad = hashAchievedDiff(b.hash);
          if (ad >= nd) {
            console.log('[LoneStrike Cash] best healed for ' + b.height + ': ' + Math.round(b.best || 0) + ' -> ' + Math.round(ad) + ' (block-hash achieved diff)');
            b.best = ad; b.solveDiff = ad; changed = true;
          }
        }
      }
      if (sv2State.pendingEffortShares != null) {
        const pct = sv2State.pendingEffortShares / out.netDiff * 100;
        // A snapshot taken within minutes of a process start is amnesiac: the
        // in-memory round began at boot, not at the round's true start. Better
        // a dash than a lie.
        const snapshotTrusted = (Date.now() - PROC_START) > 600000;
        for (const b of blockState.blocks) {
          // fresh solves only: a heal that surfaces an OLD block must not get
          // today's round effort stamped on it
          if (!b.external && b.effort == null && nowS2 - (b.time || 0) < 900) {
            const declared = sv1SolveEffortFromLog(b.time);
            if (declared != null) { b.effort = declared; b.effortSrc = 'pool'; changed = true; }
            else if (snapshotTrusted) { b.effort = Math.round(pct * 10) / 10; b.effortSrc = 'snapshot'; changed = true; }
            else { const rt = sv2RoundNear(b.time); const ndL = Number(b.netdiff) || out.netDiff;
              if (rt != null && ndL > 0) { b.effort = Math.round(rt / ndL * 1000) / 10; b.effortSrc = 'sample'; changed = true; } }
            console.log('[LoneStrike Cash] effort for ' + b.height + ': declared=' +
              (declared != null ? declared : 'none') + ' snapshot=' + Math.round(pct * 10) / 10 +
              ' -> ' + b.effort + '%');
          }
        }
        sv2State.pendingEffortShares = null;
      }
      // declaration-heal: a snapshot stamp that the pool's own log contradicts
      // by more than 2x within 24h gets corrected to the declaration
      for (const b of blockState.blocks) {
        if (!b.external && b.effortSrc !== 'pool' && nowS2 - (b.time || 0) < 7 * 86400) {
          const declared = sv1SolveEffortFromLog(b.time);
          if (declared == null && b.effort == null) {
            const rt = sv2RoundNear(b.time); const ndH = Number(b.netdiff) || out.netDiff;
            if (rt != null && ndH > 0) {
              console.log('[LoneStrike Cash] effort filled for ' + b.height + ': ' + Math.round(rt / ndH * 1000) / 10 + '% (round sample)');
              b.effort = Math.round(rt / ndH * 1000) / 10; b.effortSrc = 'sample'; changed = true;
            }
          }
          if (declared != null && b.effort == null) {
            console.log('[LoneStrike Cash] effort filled for ' + b.height + ': ' + declared + '% (pool declaration)');
            b.effort = declared; b.effortSrc = 'pool'; changed = true;
          } else if (declared != null && (declared > b.effort * 2 || declared < b.effort / 2)) {
            console.log('[LoneStrike Cash] effort healed for ' + b.height + ': ' + b.effort + '% -> ' + declared + '% (pool declaration)');
            b.effort = declared; b.effortSrc = 'pool'; changed = true;
          }
        }
      }
      if (changed) { try { saveBlocks(); } catch (_) {} }
    }
  } catch (_) { /* node unreachable */ }

  res.json(out);
});


// ---- surgical per-worker best reset (runs while entrypoint holds pool stopped) ----
function zeroBestIn(obj) {
  for (const k of Object.keys(obj)) {
    if (/^best/i.test(k) && (typeof obj[k] === 'number' || /^[0-9.eE+-]+$/.test(String(obj[k])))) obj[k] = 0;
  }
}
function surgicalReset(scope) {
  const usersDir = path.join(POOL_DIR, 'logs', 'users');
  let files = [];
  try { files = fs.readdirSync(usersDir); } catch (_) { return; }
  for (const fn of files) {
    const fp = path.join(usersDir, fn);
    try {
      const j = JSON.parse(fs.readFileSync(fp, 'utf8'));
      let touched = false;
      const stack = [j];
      while (stack.length) {
        const o = stack.pop();
        if (Array.isArray(o)) { o.forEach(x => x && typeof x === 'object' && stack.push(x)); continue; }
        if (o && typeof o === 'object') {
          if (o.workername === scope) { zeroBestIn(o); touched = true; }
          else Object.values(o).forEach(v => v && typeof v === 'object' && stack.push(v));
        }
      }
      if (touched) {
        // user-level best = max of remaining workers
        const workers = Array.isArray(j.worker) ? j.worker : [];
        const maxBest = (key) => workers.reduce((m, w) => Math.max(m, Number(w[key]) || 0), 0);
        if ('bestshare' in j) j.bestshare = maxBest('bestshare');
        if ('bestever' in j) j.bestever = maxBest('bestever');
        fs.writeFileSync(fp, JSON.stringify(j));
        console.log(`[LoneStrike Cash] surgical best reset: ${scope} in ${fn}`);
      }
    } catch (_) { /* not JSON or unreadable — skip */ }
  }
}
setInterval(() => {
  try {
    const stopMark = path.join(POOL_DIR, 'config', 'pool_stopped');
    const doneMark = path.join(POOL_DIR, 'config', 'edit_done');
    if (!fs.existsSync(stopMark) || fs.existsSync(doneMark)) return;
    let scope = '';
    try { scope = fs.readFileSync(path.join(POOL_DIR, 'config', 'reset_request'), 'utf8').trim(); } catch (_) {}
    if (scope && scope !== 'all') surgicalReset(scope);
    fs.writeFileSync(doneMark, '1');
  } catch (_) {}
}, 1500);

app.post('/api/worker/hide', (req, res) => {
  const name = ((req.body && req.body.name) || '').toString().slice(0, 120);
  if (!name) return res.status(400).json({ ok: false });
  const hf = path.join(POOL_DIR, 'config', 'hidden.json');
  let hidden = {};
  try { hidden = JSON.parse(fs.readFileSync(hf, 'utf8')); } catch (_) {}
  hidden[name] = Math.floor(Date.now() / 1000);
  try {
    fs.mkdirSync(path.dirname(hf), { recursive: true });
    fs.writeFileSync(hf, JSON.stringify(hidden));
  } catch (e) { return res.status(500).json({ ok: false }); }
  res.json({ ok: true, name });
});

app.post('/api/diff', (req, res) => {
  const b = req.body || {};
  const min = Math.max(1, parseInt(b.min, 10) || 1);
  const start = Math.max(min, parseInt(b.start, 10) || 42);
  let max = parseInt(b.max, 10) || 0;
  if (max && max < start) max = 0;
  try {
    fs.mkdirSync(path.join(POOL_DIR, 'config'), { recursive: true });
    fs.writeFileSync(path.join(POOL_DIR, 'config', 'diff'), `${min} ${start} ${max}`);
  } catch (e) { return res.status(500).json({ ok: false }); }
  res.json({ ok: true, min, start, max });
});

app.post('/api/reset', (req, res) => {
  const scope = ((req.body && req.body.scope) || 'all').toString().slice(0, 120);
  if (!/^[A-Za-z0-9:._\-]+$|^all$/.test(scope)) return res.status(400).json({ ok: false, error: 'bad scope' });
  try {
    const sv2Names = new Set(Object.values(sv2State.channels).map((c) => c.name));
    if (scope === 'all' || sv2Names.has(scope)) { sv2ApplyReset(scope); sv2ApplyCountReset(scope); }
    if (sv2Names.has(scope) && scope !== 'all') return res.json({ ok: true, scope, sv2: true });
    if (scope === 'all') {
      try { writeBaselines({}); } catch (_) {}
      try { writeResetState({}); } catch (_) {}
      for (const k of Object.keys(shareBest)) delete shareBest[k]; writeShareBest();
      fs.mkdirSync(path.join(POOL_DIR, 'config'), { recursive: true });
      fs.writeFileSync(path.join(POOL_DIR, 'config', 'reset_request'), scope);
    } else if (logSharesOn()) {
      const rs = readResetState(); rs[scope] = Math.floor(Date.now() / 1000); writeResetState(rs);
      shareBest[scope] = 0; writeShareBest();
    } else {
      const b = readBaselines();
      const w = readWorkers().find(x => x.name === scope);
      b[scope] = w ? (Number(w.rawBest) || 0) : (Number(b[scope]) || 0);
      writeBaselines(b);
    }
  } catch (e) { return res.status(500).json({ ok: false, error: 'could not reset' }); }
  res.json({ ok: true, scope });
});

// ---- SV2 log download ----------------------------------------------------
// Redacted by DEFAULT. The point of this button is pasting the log into
// Discord for support, and pool_sv2.log carries the payout address, worker
// identities and miner IPs -- publishing a rental fleet's layout to get help
// with a warning is a bad trade. ?raw=1 is opt-in and labelled as such.
function sv2Redact(text) {
  let out = text;
  try {
    const addr = readAddress && readAddress();
    if (addr && addr.length > 8) out = out.split(addr).join('[payout-address]');
  } catch (_) {}
  out = out.replace(/\b(?:bitcoincash:|bchtest:|bchreg:)?[qp][0-9a-z]{41}\b/gi, '[bch-address]');
  out = out.replace(/\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b/g, (m) =>
    (m.startsWith('127.0.0.1') || m.startsWith('0.0.0.0')) ? m : '[ip]');
  out = out.replace(/\b(?:[0-9a-f]{1,4}:){4,7}[0-9a-f]{1,4}\b/gi, '[ipv6]');
  out = out.replace(/(user_identity[=:\s"']*)([^"'\s,)]+)/gi, '$1[worker]');
  return out;
}
app.get('/api/sv2/log', (req, res) => {
  const raw = req.query && String(req.query.raw) === '1';
  const tail = Math.min(Math.max(parseInt((req.query && req.query.tail) || '4000', 10) || 4000, 100), 50000);
  let text = '';
  try {
    const fd = fs.openSync(SV2_LOG_FILE, 'r');
    const size = fs.fstatSync(fd).size;
    const want = Math.min(size, 4 * 1024 * 1024);   // cap the read, not just the lines
    const buf = Buffer.alloc(want);
    fs.readSync(fd, buf, 0, want, size - want);
    fs.closeSync(fd);
    text = buf.toString('utf8');
    if (size > want) text = '[... earlier lines omitted ...]\n' + text.slice(text.indexOf('\n') + 1);
  } catch (e) {
    return res.status(404).type('text/plain').send('no SV2 log yet (is SV2 enabled?)');
  }
  const lines = text.split('\n');
  text = lines.slice(Math.max(0, lines.length - tail)).join('\n');
  if (!raw) text = sv2Redact(text);
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  res.set('Content-Disposition',
          'attachment; filename="sv2-log-' + stamp + (raw ? '-RAW' : '-redacted') + '.txt"');
  res.type('text/plain').send(
    '# LoneStrike Cash SV2 log' + (raw ? ' (RAW - contains your payout address, worker names and miner IPs)'
                                       : ' (redacted: address/worker/IP removed)') +
    '\n# last ' + tail + ' lines\n\n' + text);
});

app.post('/api/address', (req, res) => {
  const a = (req.body && req.body.address || '').trim();
  if (!validAddress(a)) return res.status(400).json({ ok: false, error: 'invalid BCH address' });
  try { writeAddress(a); } catch (e) { return res.status(500).json({ ok: false, error: 'could not save' }); }
  res.json({ ok: true, address: a });
});

app.get('/api/logshares', (_req, res) => res.json({ ok: true, on: logSharesOn() }));
app.post('/api/logshares', (req, res) => {
  const on = !!(req.body && req.body.on);
  setLogShares(on);
  if (!on) { try { writeResetState({}); } catch (_) {} for (const k of Object.keys(shareBest)) delete shareBest[k]; writeShareBest(); try { purgeAllSharelogs(); } catch (_) {} }
  res.json({ ok: true, on });
});
const ELEC_FILE = path.join(POOL_DIR, 'config', 'electricity.json');
function readElectricity() { try { return JSON.parse(fs.readFileSync(ELEC_FILE, 'utf8')) || {}; } catch (_) { return {}; } }
function writeElectricity(e) { try { fs.mkdirSync(path.dirname(ELEC_FILE), { recursive: true }); fs.writeFileSync(ELEC_FILE, JSON.stringify(e)); } catch (_) {} }
app.get('/api/electricity', (_req, res) => res.json({ ok: true, ...readElectricity() }));
app.post('/api/electricity', (req, res) => {
  const e = readElectricity(); const b = req.body || {};
  if (b.rate  != null) e.rate  = Math.max(0, Number(b.rate)  || 0);
  if (b.watts != null) e.watts = Math.max(0, Number(b.watts) || 0);
  writeElectricity(e); res.json({ ok: true, ...e });
});
app.get('/api/sv2', (req, res) => {
  let pub = '';  try { pub  = fs.readFileSync(SV2_PUB_FILE,  'utf8').trim(); } catch (_) {}
  let addr = ''; try { addr = fs.readFileSync(SV2_ADDR_FILE, 'utf8').trim(); } catch (_) {}
  let savedSpm = 6;
  try { const v = parseFloat(fs.readFileSync(SV2_SPM_FILE, 'utf8')); if (v > 0) savedSpm = v; } catch (_) {}
  let activeSpm = 0;
  for (const c of Object.values(sv2Api.channels)) { if (c.spm > 0) { activeSpm = c.spm; break; } }
  // Both, always. Reporting only the saved value is what let a tester spend a
  // day fighting a pool that was enforcing something else.
  sv2Ingest();
  const out = { ok: true, enabled: !!addr, address: addr, authorityPub: pub,
                savedSpm, activeSpm,
                savedXn: readSv2Xn(), activeXn: sv2State.activeXn || null,
                workerTtlMin: Math.round(workerTtlSec() / 60),
                endpoint: STRATUM_HOST + ':33333' };
  if (req.query && req.query.debug) {
    try {
      sv2Ingest();
      out.debug = {};
      const nowMs = Date.now(), B = 5, W = 60 * 1000;
      for (const [cid, ch] of Object.entries(sv2State.channels)) {
        const bk = Array.from({ length: B }, () => ({ n: 0, work: 0, minSeq: Infinity, maxSeq: -1, diffs: [] }));
        for (const [ts, w, c, diff, seq] of sv2State.shares) {
          if (c !== cid) continue;
          const idx = Math.floor((nowMs - ts) / W);
          if (idx < 0 || idx >= B) continue;
          const b = bk[B - 1 - idx];
          b.n++; b.work += w;
          if (diff > 0) b.diffs.push(diff);
          if (seq >= 0) { if (seq < b.minSeq) b.minSeq = seq; if (seq > b.maxSeq) b.maxSeq = seq; }
        }
        out.debug[cid] = { name: ch.name, rejected: ch.rejected, lastReject: ch.lastReject || null, buckets: bk.map((b) => {
          const d = b.diffs.slice().sort((x, y) => x - y);
          return { n: b.n, span: b.maxSeq >= b.minSeq ? b.maxSeq - b.minSeq + 1 : 0,
                   workPerShare: b.n ? +(b.work / b.n).toExponential(2) : 0,
                   tDiffQ: d.length >= 8 ? Math.round(d[Math.floor(d.length * 0.15)] * 0.85) : 0 };
        }) };
      }
    } catch (e) { out.debugError = String(e); }
  }
  res.json(out);
});
app.post('/api/sv2', (req, res) => {
  const a = (req.body && req.body.address || '').trim();
  if (a === '') {
    try { fs.unlinkSync(SV2_ADDR_FILE); } catch (_) {}
    return res.json({ ok: true, enabled: false, address: '' });
  }
  if (!validAddress(a)) return res.status(400).json({ ok: false, error: 'invalid BCH address' });
  try { fs.mkdirSync(SV2_DIR, { recursive: true }); fs.writeFileSync(SV2_ADDR_FILE, a); }
  catch (e) { return res.status(500).json({ ok: false, error: 'could not save' }); }
  const spm = Number(req.body && req.body.sharesPerMinute);
  if (spm && spm >= 0.5 && spm <= 600) {
    try { fs.writeFileSync(SV2_SPM_FILE, String(spm)); } catch (_) {}
  }
  const ttl = Number(req.body && req.body.workerTtlMin);
  if (Number.isFinite(ttl)) {
    if (!Number.isInteger(ttl) || ttl < 5 || ttl > 1440) {
      return res.status(400).json({ ok: false, error: 'idle worker timeout must be a whole number of minutes from 5 to 1440' });
    }
    try { fs.writeFileSync(WORKER_TTL_FILE, String(ttl)); } catch (_) {}
  }
  const xn = Number(req.body && req.body.extranonce2Bytes);
  if (xn) {
    if (!Number.isInteger(xn) || xn < 4 || xn > 32) {
      return res.status(400).json({ ok: false, error: 'extranonce2 bytes must be a whole number from 4 to 32' });
    }
    try { fs.writeFileSync(SV2_XN_FILE, String(xn)); } catch (_) {}
  }
  res.json({ ok: true, enabled: true, address: a });
});

app.get('/health', (_req, res) => res.json({ ok: true }));
app.use(express.static(path.join(__dirname, 'public')));

// Umbrel home-screen widget: glanceable solo-mining stats (fetched by umbreld)
app.get('/api/widget/stats', (_req, res) => {
  const fmtDiff = (n) => {
    n = Number(n) || 0;
    const u = [[1e18,'E'],[1e15,'P'],[1e12,'T'],[1e9,'G'],[1e6,'M'],[1e3,'K']];
    for (const [v,sfx] of u) if (n >= v) return (n/v).toFixed((n/v) >= 100 ? 0 : 1) + sfx;
    return String(Math.round(n));
  };
  let hashVal = '0', hashUnit = 'H/s', miners = 0, best = 0, blocks = 0;
  try {
    const p = readPoolStatus();
    let hs = hashToHs(p.hashrate5m || p.hashrate1m) || 0;
    best = Number(p.bestshare) || 0;
    miners = readWorkers().filter(w => !w.idle).length;
    blocks = Math.max(blockState.blocks.length, countBlocks());
    try {
      const s2 = sv2Stats();
      hs += s2.hs || 0;
      miners += s2.workers || 0;
      best = Math.max(best, s2.best || 0);
    } catch (_) { /* sv2 telemetry unavailable */ }
    const h = fmtHs(hs); hashVal = h.val; hashUnit = h.unit;
  } catch (_) { /* pool not up yet */ }
  res.json({
    type: 'four-stats',
    refresh: '10s',
    items: [
      { title: 'Pool hashrate', text: hashVal, subtext: hashUnit },
      { title: 'Miners', text: String(miners), subtext: 'online' },
      { title: 'Best share', text: fmtDiff(best), subtext: 'best diff' },
      { title: 'Blocks', text: String(blocks), subtext: 'found' },
    ],
  });
});

app.listen(PORT, () => console.log(`LoneStrike Cash dashboard on :${PORT}`));
