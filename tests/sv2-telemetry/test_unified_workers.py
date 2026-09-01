#!/usr/bin/env python3
"""Regression tests for the unified SV1/SV2 worker data model (#4).

THE PROBLEM: the two protocols emitted different shapes into the same UI.
  SV1 row: trend = [1m, 5m, 1hr, 1d, 7d]  <- window AVERAGES
  SV2 row: trend = five 1-minute buckets  <- a real TIME SERIES
Same field, different meaning, drawn as one sparkline. SV2 rows carried no
`accepted` at all, so per-worker accept/reject was impossible (Chris #4);
resets left the displayed counters untouched (Chris #1); and a block found by
one protocol did not clear the other's best, so the fleet number stayed pinned
to a stale value (Chris #2).

These tests assert the CONTRACT rather than re-deriving hashrate maths, which
the existing run_tests.js harness already covers against real logs.
Python 3 stdlib only.
"""
import os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SERVER = os.path.join(ROOT, "sslabs-solostrike-cash", "dashboard", "server.js")

FAILURES = []


def check(name, cond, detail=""):
    print(f"{'PASS' if cond else 'FAIL'}  {name}"
          f"{': ' + str(detail) if detail and not cond else ''}")
    if not cond:
        FAILURES.append(name)


SRC = open(SERVER).read()
HTML = open(SERVER.replace("server.js", os.path.join("public", "index.html"))).read()


def row_block(marker):
    """Extract a workerList.push({...}) object literal following a marker."""
    i = SRC.index(marker)
    j = SRC.index("push({", i)
    depth, k = 0, j + 5
    while k < len(SRC):
        if SRC[k] == "{":
            depth += 1
        elif SRC[k] == "}":
            depth -= 1
            if depth == 0:
                return SRC[j:k + 1]
        k += 1
    return ""


UNIFIED_FIELDS = ["name", "proto", "conns", "declared", "accepted", "rejected",
                  "rejectReasons", "hashrate", "hs", "trend", "idle", "best",
                  "last", "firstSeen"]


def test_both_protocols_emit_one_schema():
    sv2 = row_block("UNIFIED SCHEMA -- identical field set to the SV1 rows")
    sv1 = row_block("UNIFIED SCHEMA -- identical field set to the SV2 rows")
    check("SV2 row block found", bool(sv2))
    check("SV1 row block found", bool(sv1))
    for f in UNIFIED_FIELDS:
        check(f"SV2 row has '{f}'", re.search(rf"\b{f}\s*:", sv2) is not None)
        check(f"SV1 row has '{f}'", re.search(rf"\b{f}\s*:", sv1) is not None)


def test_accepted_present_for_sv2():
    """Chris #4: per-worker accept/reject on the SV2 worker list."""
    sv2 = row_block("UNIFIED SCHEMA -- identical field set to the SV1 rows")
    check("SV2 reports accepted (was absent entirely)",
          "accepted:" in sv2 and "sv2CntFor" in sv2)
    check("SV2 reports rejected", "rejected:" in sv2)


def test_sv1_rejects_are_honest():
    """SV1 has no per-worker reject counter: report null, never a fake 0."""
    sv1 = row_block("UNIFIED SCHEMA -- identical field set to the SV2 rows")
    m = re.search(r"rejected:.*?(?=\n\s+\w+:)", sv1, re.S)
    check("SV1 rejected falls back to null, not 0",
          bool(m) and ": null" in m.group(0), m.group(0) if m else "")
    check("SV1 accepted is null when the counter is absent",
          "('shares' in w)" in sv1 and ": null" in sv1)


def test_trend_means_the_same_thing():
    """The core bug: `trend` must be the same quantity on both protocols."""
    sv1 = row_block("UNIFIED SCHEMA -- identical field set to the SV2 rows")
    sv2 = row_block("UNIFIED SCHEMA -- identical field set to the SV1 rows")
    check("SV1 trend comes from the shared ring", "ringTrend(" in sv1)
    check("SV2 trend comes from the shared ring", "ringTrend(" in sv2)
    check("SV1 no longer emits [1m,5m,1hr,1d,7d] as a fake time series",
          "hashrate7d" not in sv1)


def test_named_windows_exist():
    check("ringWin exists", "function ringWin(" in SRC)
    check("unifiedHashrate emits 1m/5m/1h/1d",
          all(f"'{w}':" in SRC for w in ("1m", "5m", "1h", "1d")))
    m = re.search(r"function unifiedHashrate\(key\) \{(.*?)\n\}", SRC, re.S)
    check("windows are 1/5/60/1440 minutes",
          bool(m) and "ringWin(key, 1)" in m.group(1)
          and "ringWin(key, 5)" in m.group(1)
          and "ringWin(key, 60)" in m.group(1)
          and "ringWin(key, 1440)" in m.group(1))
    check("rings retain 24h", "MINS_KEEP = 1440" in SRC)
    # 1h/1d are unreachable from sv2State.shares: it is pruned at 600s
    check("share buffer is still pruned at 600s (rings are the only 1h/1d path)",
          "600 * 1000" in SRC)


def test_sv2_restart_persistence():
    check("snapshot function exists", "function workersSave(" in SRC)
    check("load function exists", "function workersLoad(" in SRC)
    check("snapshot is atomic (tmp + rename)",
          "renameSync(tmp, WORKERS_STATE_FILE)" in SRC)
    check("snapshot runs on a 60s timer",
          re.search(r"workersSave\(\).*?\}, 60000\)", SRC, re.S) is not None)
    m = re.search(r"function workersSave\(\) \{(.*?)\n\}", SRC, re.S)
    for f in ("accepted", "rejected", "best", "firstSeen"):
        check(f"snapshot persists {f}", bool(m) and f in m.group(1))
    check("snapshot persists the rings", bool(m) and "mins: workerMins" in m.group(1))
    check("state file lives on the shared volume",
          "WORKERS_STATE_FILE = path.join(SV2_DIR, 'workers_state.json')" in SRC)


def test_reset_clears_counters():
    """Chris #1: an individual reset must also clear that row's acc/rej."""
    check("count-reset applies a baseline", "function sv2ApplyCountReset(" in SRC)
    check("rows report the delta, not the raw counter",
          "sv2CntFor(ch).accepted" in SRC)
    check("reset endpoint clears counters as well as best",
          "sv2ApplyReset(scope); sv2ApplyCountReset(scope);" in SRC)
    check("baselines survive restart", "SV2_CNT_BASE_FILE" in SRC
          and "sv2SaveCntBase" in SRC)
    m = re.search(r"function sv2ApplyCountReset\(scope\) \{(.*?)\n\}", SRC, re.S)
    check("reset all also clears SV2 rings",
          bool(m) and "startsWith('SV2:')" in m.group(1))


def test_block_resets_best_fleet_wide():
    """Chris #2: a block is a fleet event -- both protocols' best must clear."""
    m = re.search(r"if \(out\.blockList\.length !== sv2State\.lastBlockCount\) \{"
                  r"(.*?)\n      \}", SRC, re.S)
    check("block-found hook found", bool(m))
    if not m:
        return
    body = m.group(1)
    check("block clears SV2 best", "sv2ApplyReset('all')" in body)
    check("block clears SV1 best via reset_request", "reset_request" in body)
    check("first observation does not fire a reset",
          "sv2State.lastBlockCount >= 0" in body)


def test_fleet_aggregation_sums_unified_fields():
    """Rental fleets fan one identity across many channels; merged rows must
    sum the new counters instead of showing only the first channel's."""
    i = SRC.index("collapse same-name SV2 rows")
    blk = SRC[i:i + 2000]
    check("aggregation sums accepted", "m.accepted = (m.accepted || 0) + w.accepted" in blk)
    check("aggregation sums rejected", "m.rejected = (m.rejected || 0) + w.rejected" in blk)
    check("aggregation sums the named windows", "m.hs[k2] = (m.hs[k2] || 0)" in blk)
    check("aggregation keeps the earliest firstSeen", "w.firstSeen < m.firstSeen" in blk)


def test_no_temporal_dead_zone():
    """sv2CntBase was loaded above its own `let` and crashed the dashboard at
    boot. The harness caught it; keep it caught."""
    decl = SRC.index("let sv2CntBase")
    uses = [m.start() for m in re.finditer(r"\bsv2CntBase\b", SRC)]
    check("sv2CntBase is never touched before its declaration",
          all(u >= decl for u in uses),
          f"first use at {min(uses)}, declared at {decl}")




def test_extranonce2_setting():
    """Chris's request: tunable extranonce2, labelled, beside payout address.
    Stored and bounds-checked here; SRI hardcodes CLIENT_SEARCH_SPACE_BYTES=16
    so the Part B pool patch consumes this file."""
    check("extranonce2 file defined", "SV2_XN_FILE" in SRC)
    check("default matches SRI's current constant", "SV2_XN_DEFAULT  = 16" in SRC)
    m = re.search(r"function readSv2Xn\(\) \{(.*?)\n\}", SRC, re.S)
    check("reader clamps to 4..32 with a safe fallback",
          bool(m) and "n >= 4 && n <= 32" in m.group(1)
          and "SV2_XN_DEFAULT" in m.group(1))
    i = SRC.index("const xn = Number(req.body && req.body.extranonce2Bytes)")
    blk = SRC[i:i + 500]
    check("API rejects non-integers and out-of-range values",
          "Number.isInteger(xn)" in blk and "xn < 4 || xn > 32" in blk)
    check("API returns an explanatory error", "whole number from 4 to 32" in blk)
    check("current value exposed on status", "xn: readSv2Xn()" in SRC)
    html = open(os.path.join(ROOT, "sslabs-solostrike-cash", "dashboard",
                             "public", "index.html")).read()
    check("UI has the extranonce2 input", 'id="sv2XnInput"' in html)
    check("UI sends it on save", "extranonce2Bytes:parseInt" in html)


def test_ui_is_labelled():
    """Chris #3: every SV2 control needs a visible label, not a bare box."""
    html = open(os.path.join(ROOT, "sslabs-solostrike-cash", "dashboard",
                             "public", "index.html")).read()
    for label in ("Shares / min", "Extranonce2 bytes", "Payout address"):
        check("visible label: " + label, label in html)
    for ident in ("sv2SpmInput", "sv2XnInput", "sv2AddrInput"):
        check(ident + " has a <label for=>", 'for="' + ident + '"' in html)
    check("shares/min label explains vardiff", "Vardiff tunes each miner" in html)
    check("extranonce2 label explains the tradeoff",
          "more nonce room" in html and "scriptSig" in html)
    check("worker rows show accepted AND rejected",
          "typeof w.accepted" in html and "typeof w.rejected" in html)


def test_sv2_log_download():
    """The log download exists to be pasted into Discord for support, so the
    default MUST be redacted: pool_sv2.log carries the payout address, worker
    identities and miner IPs."""
    check("log endpoint exists", "app.get('/api/sv2/log'" in SRC)
    check("redaction helper exists", "function sv2Redact(" in SRC)
    i = SRC.index("app.get('/api/sv2/log'")
    blk = SRC[i:i + 1600]
    check("redacted is the DEFAULT (raw is opt-in)",
          "String(req.query.raw) === '1'" in blk and "if (!raw) text = sv2Redact(text)" in blk)
    check("tail is bounded", "Math.min(Math.max(" in blk)
    check("read size is capped, not just line count", "4 * 1024 * 1024" in blk)
    check("served as a download", "Content-Disposition" in blk
          and "attachment; filename=" in blk)
    check("filename marks whether it is raw", "'-RAW' : '-redacted'" in blk)
    check("header warns what a raw log contains", "contains your payout address" in blk)
    r = SRC[SRC.index("function sv2Redact("):SRC.index("app.get('/api/sv2/log'")]
    check("redacts the configured payout address", "[payout-address]" in r)
    check("redacts any BCH address", "[bch-address]" in r)
    check("redacts miner IPs", "'[ip]'" in r)
    check("keeps loopback/bind addresses (needed for debugging)",
          "127.0.0.1" in r and "0.0.0.0" in r)
    check("redacts worker identities", "[worker]" in r)
    html = open(os.path.join(ROOT, "sslabs-solostrike-cash", "dashboard",
                             "public", "index.html")).read()
    check("UI has a download button", 'id="sv2LogDl"' in html)
    check("download button says it is safe to share", "safe to paste" in html)
    check("raw link is present but de-emphasised", 'id="sv2LogRaw"' in html
          and "Only share privately" in html)



def _extract_fn(src, header):
    i = src.index(header)
    j = src.index("{", i)
    depth, k = 0, j
    while k < len(src):
        if src[k] == "{": depth += 1
        elif src[k] == "}":
            depth -= 1
            if depth == 0: return src[i:k + 1]
        k += 1
    raise AssertionError("unbalanced braces for " + header)


def test_sv2_found_blocks_create_entries():
    """Chris's 959807: the bridge recorded BLOCK ACCEPTED in sv2_blocks.jsonl,
    but the dashboard only used that file to ANNOTATE blocks the chain scan had
    already found -- so when the scan missed, Blocks Found stayed at 2 and the
    fleet round-reset never fired. The jsonl is written at the moment the node
    accepts the block; it must CREATE entries."""
    check("upsert function exists", "function mergeSv2FoundBlocks(" in SRC)
    check("old annotate-only loop is gone",
          "if (b && !b.worker) b.worker = 'SV2';" not in SRC)
    fn = _extract_fn(SRC, "function mergeSv2FoundBlocks(")
    check("upsert creates entries (push)", "blockState.blocks.push({" in fn)
    check("created entries are marked as bridge-sourced", "source: 'sv2-bridge'" in fn)
    check("accepted/duplicate map to confirmed",
          "rec.result === 'accepted' || rec.result === 'duplicate'" in fn)
    check("dedupes by hash OR height",
          "x.hash === rec.hash" in fn and "x.height === rec.height" in fn)
    check("persists when anything was added", "if (added) saveBlocks();" in fn)
    i = SRC.index("async function scanBlocks()")
    scan = SRC[i:i + 4000]
    check("scan runs the upsert before healFromBlockFiles",
          0 < scan.find("mergeSv2FoundBlocks();") < scan.find("healFromBlockFiles();"))
    check("status merge upserts BOTH protocols and refreshes the visible list",
          "const upserted = mergeSv2FoundBlocks();" in SRC and
          "healFromBlockFiles(); } catch (_) {}" in SRC and
          "out.blockList = [...blockState.blocks]" in SRC)

    # functional: run the real function under node with stubs (CI only)
    import shutil, subprocess, tempfile
    if not shutil.which("node"):
        print("SKIP  upsert behaviour (node unavailable on this host)")
        return
    js = """
let saved = 0;
const saveBlocks = () => { saved++; };
const blockState = { blocks: [{ height: 100, hash: 'aa', time: 1, worker: null }] };
let jsonl = [];
const sv2Blocks = () => jsonl;
const sv2SolveDiffFromLog = () => 0;      // no pool log in this harness
const solveDiffFromBlocks = () => 0;
%s
// 1) rec matching an existing chain-scan entry: annotate only
jsonl = [{ height: 100, hash: 'aa', time: 1, result: 'accepted' }];
let n1 = mergeSv2FoundBlocks();
// 2) brand-new accepted rec: must CREATE
jsonl = [{ height: 959807, hash: 'ae8f398', time: 1784183321, result: 'accepted' }];
let n2 = mergeSv2FoundBlocks();
// 3) idempotent on re-run
let n3 = mergeSv2FoundBlocks();
const nb = blockState.blocks.find(b => b.height === 959807);
console.log(JSON.stringify({ n1, n2, n3, len: blockState.blocks.length, saved,
  annotated: blockState.blocks[0].worker, state: nb && nb.state,
  worker: nb && nb.worker, src: nb && nb.source }));
""" % _extract_fn(SRC, "function mergeSv2FoundBlocks(")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    import json as _json
    try: d = _json.loads(r.stdout.strip().split("\n")[-1])
    except Exception:
        check("upsert functional run", False, r.stdout + r.stderr); return
    check("existing entry annotated, not duplicated", d["n1"] == 0 and d["annotated"] == "SV2")
    check("new accepted rec creates exactly one entry", d["n2"] == 1 and d["len"] == 2)
    check("created entry: confirmed / worker SV2 / bridge-sourced",
          d["state"] == "confirmed" and d["worker"] == "SV2" and d["src"] == "sv2-bridge")
    check("idempotent on re-run", d["n3"] == 0)
    check("saveBlocks called only when something was added", d["saved"] == 1)


def test_addr_matching_normalized_by_node():
    """BCHN reports vout addresses in cashaddr; a legacy-stored payout (a real
    one in the field: 1QJr...) could never string-match, so the chain scan
    silently missed paid blocks. The node normalizes via validateaddress."""
    check("addrKeysFor exists", "async function addrKeysFor(" in SRC)
    fn = _extract_fn(SRC, "async function addrKeysFor(")
    check("asks the node to normalize", "'validateaddress'" in fn)
    check("keeps the raw key too (node not ready is survivable)",
          "keys.add(k)" in fn and "node not ready" in fn)
    check("coinbasePaysUs matches against the key set", "mine.has(addrKey(a))" in SRC)
    check("scan unions both payout addresses into one key set",
          "for (const k of await addrKeysFor(readSv2Address())) mineKeys.add(k);" in SRC)
    check("single-string matching is gone", "addrKey(a) === mine" not in SRC)


def test_sv2_best_diff_is_solve_not_network():
    """Chris flagged SV2 blocks showing 449G/460G -- network difficulty -- in
    the "Best Diff Submitted" column. The SV2 pool logs the block-found line
    without a diff; the solving share's `share_work` is the real solve
    difficulty and must be what's shown. netdiff must NEVER populate `best`."""
    check("SV2 solve-diff reader exists", "function sv2SolveDiffFromLog(" in SRC)
    fn = _extract_fn(SRC, "function sv2SolveDiffFromLog(")
    check("reader keys off the block hash", "indexOf(hash)" in fn)
    check("reader reads share_work", "share_work" in fn)
    check("reader prefers the solving share's own channel", "chM[1]" in fn or "cM[1] === chM[1]" in fn)
    check("bridge-record blocks use solve diff, not 0-then-netdiff",
          "sv2SolveDiffFromLog(rec.hash)" in SRC)
    check("heal path never borrows netdiff into best",
          "hit.netdiff || lastBestSeen" not in SRC and
          "best: solveDiff || 0" in SRC)

    # functional: run the extracted reader against Chris's real block-found log
    import shutil, subprocess, tempfile, os as _os
    if not shutil.which("node"):
        print("SKIP  solve-diff reader (node unavailable)"); return
    fx = _os.path.join(HERE, "fixtures", "pool_sv2_blockfound.log")
    if not _os.path.exists(fx):
        check("blockfound fixture present", False); return
    js = """
const fs = require('fs'); const path = require('path');
let sv1Decls = [];
const SV2_DIR = %r, POOL_DIR = '/nonexistent', POOL_LOGDIR = '/nonexistent';
%s
const h = '000000000000000001e739924629fda5fa834f89517946f94292a04bf5aec98f';
console.log(String(sv2SolveDiffFromLog(h)));
""" % (_os.path.dirname(fx), _extract_fn(SRC, "function sv2SolveDiffFromLog("))
    # point the reader's first candidate path at the fixture dir
    js = js.replace("path.join(SV2_DIR, 'pool_sv2.log')",
                    "path.join(SV2_DIR, 'pool_sv2_blockfound.log')")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    out = (r.stdout or "").strip()
    check("reader extracts a real solve diff from Chris's log (not 0, not 449G)",
          out.isdigit() and 1000 < int(out) < 1_000_000_000,
          out + r.stderr)
    check("solve diff matches the winning share_work (88416)",
          out == "88416", out)



def test_sv2_reset_survives_translator_merge():
    """Chris (2026-07-18): "reset best doesn't reset sv2 workers". The
    translator stats API reports its own cumulative best; the merge guard was
    `api.best > sv2ResetTs(name) * 0` -- always true -- so seconds after every
    reset the old best was re-imported. Baseline at reset; only values that
    EXCEED the baseline are imported."""
    check("the *0 always-true guard is gone", "sv2ResetTs(ch.name) * 0" not in SRC)
    check("api baseline state exists", "sv2ApiBase" in SRC and "SV2_API_BASE_FILE" in SRC)
    check("merge records the api high-water mark", "sv2ApiLast[ch.name] = Math.max" in SRC)
    check("merge imports only above the baseline",
          "api.best > (Number(sv2ApiBase[ch.name]) || 0) && api.best > ch.best" in SRC)
    check("reset freezes the baseline for the scope",
          "sv2ApiBase[n] = Math.max(Number(sv2ApiBase[n]) || 0, Number(sv2ApiLast[n]) || 0)" in SRC)

    import shutil, subprocess, tempfile, json as _json
    if not shutil.which("node"):
        print("SKIP  reset/merge functional run (node unavailable)"); return
    js = """
const path={join:()=>'/dev/null'}; const fs={writeFileSync:()=>{},readFileSync:()=>{throw 0}};
let sv2Resets={}, sv2BestP={};
const sv2State={channels:{c1:{name:'miner3',best:196.45e9,accepted:0,rejected:0,last:1e12}}};
function sv2SaveResets(){} function sv2SaveBest(){}
const SV2_DIR='';
%s
%s
// translator has been reporting a big cumulative best
const ch=sv2State.channels.c1; let api={best:196.45e9};
sv2ApiLast[ch.name]=api.best;
// user hits RESET BEST (scope all)
sv2ApplyReset('all');
const afterReset=ch.best;
// next poll: the merge sees the SAME cumulative api.best again
if (api.best > 0) {
  sv2ApiLast[ch.name] = Math.max(Number(sv2ApiLast[ch.name]) || 0, api.best);
  if (api.best > (Number(sv2ApiBase[ch.name]) || 0) && api.best > ch.best) ch.best = api.best;
}
const afterMerge=ch.best;
// later: a genuinely NEW record beats the old one
api={best:250e9};
if (api.best > 0) {
  sv2ApiLast[ch.name] = Math.max(Number(sv2ApiLast[ch.name]) || 0, api.best);
  if (api.best > (Number(sv2ApiBase[ch.name]) || 0) && api.best > ch.best) ch.best = api.best;
}
const afterNewRecord=ch.best;
console.log(JSON.stringify({afterReset,afterMerge,afterNewRecord}));
"""
    import re as _re
    # one-line functions: extract to end of line, not to first brace.
    # The stubs (path.join -> /dev/null, fs.readFileSync throws) make the real
    # persistence code safe to run as-is -- no fragile string surgery needed.
    base_block = _re.search(r"// Chris \(2026-07-18\):.*?function sv2SaveApiBase\(\)[^\n]*", SRC, _re.S).group(0)
    reset_fn = _extract_fn(SRC, "function sv2ApplyReset(")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js % (base_block, reset_fn)); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    try: d = _json.loads(r.stdout.strip().split("\n")[-1])
    except Exception:
        check("reset/merge functional run", False, (r.stdout + r.stderr)[:200]); return
    check("reset zeroes the SV2 worker best", d["afterReset"] == 0)
    check("the translator's old cumulative best does NOT come back", d["afterMerge"] == 0)
    check("a genuinely new record IS imported", d["afterNewRecord"] == 250e9)


def test_block_round_effort():
    """Chris: show the round effort each block was found at."""
    check("effort shares snapshotted BEFORE the round zeroes",
          SRC.index("sv2State.pendingEffortShares =") < SRC.index("sv2State.roundWork = 0; sv2State.roundDiff = 0;"))
    check("percentage attached once netDiff exists",
          "sv2State.pendingEffortShares / out.netDiff * 100" in SRC)
    check("only fresh solves are stamped (healed old blocks stay blank)",
          "nowS2 - (b.time || 0) < 900" in SRC)
    check("effort persisted with the block", "sv2State.pendingEffortShares = null;" in SRC)
    check("UI: Effort column in the header", ">Effort</div>" in HTML)
    check("UI: dash when unknown, colored like the round gauge",
          "b.effort!=null" in HTML and "b.effort<100?'var(--mint)'" in HTML)


def test_celebration_holds_for_screenshots():
    """Chris: the celebration closed too fast to screenshot."""
    check("no fast auto-close", "setTimeout(celStop,5800)" not in HTML)
    check("stays up ~60s (safety-close only)", "setTimeout(celStop,60000)" in HTML)
    check("hint tells users it stays", "stays up for screenshots" in HTML)



def test_sv1_effort_declaration_wins():
    """Chris's 2026-07-22 block: the pool logged "Block solved ... at 11.1%
    effort" but the dashboard stamped 0.1% -- detection lagged the solve, and
    by then asicseer had reset its own round, so the snapshot measured the NEW
    round. The pool's own declaration is authoritative and must win; the
    snapshot is only a fallback, and never within 10 min of process start
    (restart amnesia)."""
    check("SV1 effort reader exists", "function sv1SolveEffortFromLog(" in SRC)
    fn = _extract_fn(SRC, "function sv1SolveEffortFromLog(")
    check("reads the tail, not the whole log", "262144" in fn)
    check("matches by timestamp proximity", "bestDt" in fn and "< 900" in fn)
    check("declaration takes precedence over the snapshot",
          SRC.index("sv1SolveEffortFromLog(b.time)") < SRC.index("else if (snapshotTrusted)"))
    check("snapshot needs 10 min of process uptime",
          "(Date.now() - PROC_START) > 600000" in SRC)
    check("SV1 block files healed every poll (fast detection)",
          "healFromBlockFiles(); } catch (_) {}   // SV1 solves land as files instantly" in SRC)

    import shutil, subprocess, tempfile, os as _os, json as _json
    if not shutil.which("node"):
        print("SKIP  effort reader functional (node unavailable)"); return
    fxdir = _os.path.join(HERE, "fixtures")
    js = """
const fs = require('fs'); const path = require('path');
let sv1Decls = [];
const POOL_LOGDIR = %r;
%s
// block at the solve moment -> 11.1; a block hours away -> null
const t = Date.parse('2026-07-22T07:05:08Z')/1000;
console.log(JSON.stringify({near: sv1SolveEffortFromLog(t), far: sv1SolveEffortFromLog(t - 86400)}));
""" % (fxdir, _extract_fn(SRC, "function sv1SolveEffortFromLog("))
    js = js.replace("path.join(POOL_LOGDIR, 'pool', 'pool.log')",
                    "path.join(POOL_LOGDIR, 'pool_sv1_solved.log')")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    try: d = _json.loads(r.stdout.strip().split("\n")[-1])
    except Exception:
        check("effort reader functional run", False, (r.stdout + r.stderr)[:200]); return
    check("reads the pool's declared 11.1%% for the real block", d["near"] == 11.1)
    check("returns null when no solve line is near in time", d["far"] is None)



def test_round_watermark_survives_restart_replay():
    """Chris (2026-07-28): round effort "reset and came back" at 101% after a
    block -- the ingest replays the log tail on restart and the round
    accumulators had no timestamp guard, so the pre-block round resurrected.
    A persisted watermark now gates both accumulators."""
    check("watermark persisted to disk", "SV2_ROUND_FILE" in SRC and "sv2_round_start.json" in SRC)
    check("roundWork gated by the watermark",
          "if (tsMs / 1000 > sv2RoundStartTs) {\n      sv2State.roundWork += work;" in SRC)
    check("round share counter gated alongside", "ch.roundAcc = (ch.roundAcc || 0) + 1;" in SRC)
    check("roundDiff draws on round-scoped counters only",
          "const deltaRound = (ch.roundAcc || 0) - (ch.accountedRound || 0);" in SRC)
    check("allDiff keeps all-time semantics (separate delta)",
          "const deltaAll = ch.accepted - (ch.accounted || 0);" in SRC)
    check("block detection stamps the watermark (with the SV1 baseline)",
          "sv2SetRoundStart(Math.floor(Date.now() / 1000), sv2State._sv1AccRaw ?? null);" in SRC)
    check("per-channel round counters zeroed at reset",
          "c.roundAcc = 0; c.accountedRound = 0;" in SRC)

    import shutil, subprocess, tempfile, json as _json
    if not shutil.which("node"):
        print("SKIP  watermark functional (node unavailable)"); return
    js = """
// simulate: watermark set at block time T; tail replay carries shares
// from BEFORE the block (the resurrection) and after (legit new round)
let sv2RoundStartTs = 1000;
const sv2State = { roundWork: 0 };
const ch = {};
function ingestShare(tsMs, work) {
  if (tsMs / 1000 > sv2RoundStartTs) {
    sv2State.roundWork += work;
    ch.roundAcc = (ch.roundAcc || 0) + 1;
  }
}
// replayed pre-block round: enormous, must NOT count
for (let i = 0; i < 100; i++) ingestShare(900 * 1000, 5e9);
const afterReplay = sv2State.roundWork;
// genuinely new shares after the block: must count
ingestShare(1500 * 1000, 3e9); ingestShare(1600 * 1000, 4e9);
console.log(JSON.stringify({ afterReplay, final: sv2State.roundWork, roundAcc: ch.roundAcc }));
"""
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    try: d = _json.loads(r.stdout.strip().split("\n")[-1])
    except Exception:
        check("watermark functional run", False, (r.stdout + r.stderr)[:200]); return
    check("replayed pre-block round stays at zero", d["afterReplay"] == 0)
    check("post-block shares accumulate normally", d["final"] == 7e9 and d["roundAcc"] == 2)



def test_effort_units_and_races():
    """Chris (2026-07-31): pool declared 302.9%, dashboard stamped 3.4%.
    Block-file 'time' in epoch-ms slid past the freshness gate while making
    the declaration matcher's timestamp distance astronomical (null ->
    snapshot -> post-reset pool.status -> tiny number). Four defences:
    unit normalization, matcher tolerance, prev-poll snapshot high-water,
    and a declaration-heal for recent wrong stamps."""
    check("heal normalizes ms-epoch block-file times",
          "if (t > 1e12) t = Math.floor(t / 1000);" in SRC)
    check("matcher normalizes and tolerates missing blockTime",
          "if (bt > 1e12) bt = Math.floor(bt / 1000);" in SRC and
          "if (!(bt > 0)) bt = Math.floor(Date.now() / 1000);" in SRC)
    check("snapshot uses prev-poll high-water (pool resets its round instantly)",
          "sv2State.prevRoundTotal || 0);" in SRC and
          "sv2State.prevRoundTotal = out.roundShares;" in SRC)
    check("effort decisions are logged with both candidates",
          "declared=" in SRC and "' snapshot='" in SRC)
    check("declaration-heal corrects recent 2x-wrong snapshot stamps",
          "declared > b.effort * 2 || declared < b.effort / 2" in SRC and
          "effort healed for" in SRC)
    check("stamps carry their source", "b.effortSrc = 'pool'" in SRC and "b.effortSrc = 'snapshot'" in SRC)

    import shutil, subprocess, tempfile, json as _json, os as _os
    if not shutil.which("node"):
        print("SKIP  units functional (node unavailable)"); return
    fn = _extract_fn(SRC, "function sv1SolveEffortFromLog(")
    fxdir = _os.path.join(HERE, "fixtures")
    js = """
const fs = require('fs'); const path = require('path');
let sv1Decls = [];
const POOL_LOGDIR = %r;
%s
// the 2026-07-22 fixture solve is at 07:05:08Z = 1784790308
const tSec = Date.parse('2026-07-22T07:05:08Z')/1000;
console.log(JSON.stringify({
  sec: sv1SolveEffortFromLog(tSec),        // plain seconds
  ms: sv1SolveEffortFromLog(tSec * 1000),  // ms-epoch (the bug)
}));
""" % (fxdir, fn.replace("path.join(POOL_LOGDIR, 'pool', 'pool.log')",
                          "path.join(POOL_LOGDIR, 'pool_sv1_solved.log')"))
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    try: d = _json.loads(r.stdout.strip().split("\n")[-1])
    except Exception:
        check("units functional run", False, (r.stdout + r.stderr)[:200]); return
    check("seconds-epoch blockTime matches the declaration", d["sec"] == 11.1)
    check("MS-epoch blockTime now ALSO matches (the exact bug)", d["ms"] == 11.1)


def test_celebration_survives_refresh():
    """Chris: 'the block find celebration didn't show up. Maybe because I
    refreshed' -- correct: the first-poll seeder marked every existing block
    seen, including a 30-second-old solve."""
    check("seeder exempts blocks younger than 90s",
          "nowS-(b.time||0)>=90" in HTML)
    check("fresh blocks fall through and fire",
          "must not eat the party" in HTML)


def test_declarations_are_durable():
    """Chris (2026-07-31, second report): three blocks stamped by old code
    (107.1%%, 34.3%%, 302.9%% declared; 0.1%%, dash, 3.4%% stamped). The
    asicseer ticker writes ~1 line/sec, so a 256KB tail reaches ~1h back --
    a late updater would find the declarations scrolled away and nothing
    would heal. Declarations are now captured into a durable store within
    30s of appearing, matched store-first, and the heal fills nulls too."""
    check("durable store exists", "SV1_DECL_FILE" in SRC and "solve_declarations.json" in SRC)
    check("scanner runs every 30s and at boot",
          "setInterval(() => { try { sv1DeclScan(); } catch (_) {} }, 30000);" in SRC)
    check("store deduped by timestamp and bounded",
          "!sv1Decls.some((d) => d.ts === ts)" in SRC and "sv1Decls.slice(-500)" in SRC)
    _mfn = _extract_fn(SRC, "function sv1SolveEffortFromLog(")
    check("matcher consults the store FIRST",
          "durable store first" in _mfn and
          _mfn.index("durable store first") < _mfn.index("path.join(POOL_LOGDIR"))
    check("heal window extended to 7 days", "nowS2 - (b.time || 0) < 7 * 86400" in SRC)
    check("heal FILLS null stamps from declarations (the dash gets its 34.3%%)",
          "declared != null && b.effort == null" in SRC and "effort filled for" in SRC)


def test_ghost_workers_expire_fast():
    """Chris (2026-07-31): host-side connection churn presented every
    reconnect as a fresh incremented worker name; the list grew to 80-100
    entries of the same physical rigs. A worker that never contributed a
    share (no accepted, no best) now expires 3 minutes after last contact;
    contributors keep the configured TTL."""
    check("ghost = never contributed", "const ghost = !(ch.accepted > 0) && !(ch.best > 0);" in SRC)
    check("ghosts expire at 180s, contributors keep TTL",
          "const ttl = ghost ? 180 : (fb ? Math.min(TTL, 300) : TTL);" in SRC)
    check("no grace extension for ghosts",
          "(ghost ? ttl : ttl + 600)" in SRC)


def test_effort_machinery_runs_for_sv1_only():
    """Chris (2026-08-02): running pure SV1 (no translator), two blocks landed
    with no effort and old dashes never healed -- detection, round reset,
    snapshot, and (via the pendingEffortShares gate) the heal all lived
    inside the SV2-only branch. A found block is a pool event."""
    i_close = SRC.index("out.hashrate = { val: th.val, unit: th.unit };\n    }")
    i_detect = SRC.index("if (out.blockList.length !== sv2State.lastBlockCount)")
    i_reopen = SRC.index("if (s2.enabled || s2.workers) {\n      // aggregate: rental/proxy")
    check("detection sits between gate close and gate reopen (unconditional)",
          i_close < i_detect < i_reopen)
    check("round totals updated unconditionally",
          SRC.index("sv2State.prevRoundTotal = out.roundShares;") < i_reopen)
    check("heal gated only on netDiff, snapshot nested on pending",
          "if (out.netDiff > 0) {" in SRC and
          "if (sv2State.pendingEffortShares != null) {\n        const pct" in SRC)
    check("heal loop is OUTSIDE the pending gate",
          SRC.index("declaration-heal") > SRC.index("sv2State.pendingEffortShares = null;"))


def test_external_blocks_paid_not_found():
    """Reddit field report (2026-08): user mined the SAME payout address on a
    remote pool; the remote pool solved, and the chain scan -- which matches
    coinbase outputs by address -- celebrated the block as ours. Address
    match proves PAID, local solve evidence proves FOUND. External blocks
    are listed (they paid us) but never celebrated, never reset the local
    round, never get round effort, and are labeled."""
    check("scan demands local evidence for authorship",
          "const localEvidence = solveDiff ||" in SRC and
          "sv1Decls.some((d) => Math.abs(d.ts - (hit.time || 0)) < 900)" in SRC)
    check("external entries flagged and labeled",
          "external: ext || undefined" in SRC and "(ext ? 'external' : null)" in SRC)
    check("log line distinguishes PAID from FOUND",
          "BLOCK ${ext ? 'PAID (external solve)' : 'FOUND'}" in SRC)
    check("external-only additions do not reset the local round",
          "const localCount = out.blockList.filter((b) => !b.external).length;" in SRC)
    check("effort stamp skips external", "if (!b.external && b.effort == null" in SRC)
    check("effort heal skips external", "if (!b.external && b.effortSrc !== 'pool'" in SRC)
    check("client: external never celebrates",
          "if(b.external){celebSeen.add(celebKey(b));continue;}" in HTML)
    check("client: external labeled muted in the table",
          "b.external?'<span style=\"opacity:.55\">external</span>'" in HTML)


def test_block_best_is_chain_authoritative():
    """Chris (2026-08-08): SV2 block 963169 showed best 804K -- the vardiff
    CREDITED target from share_work -- against 435G netdiff. The solving
    share's achieved difficulty IS the block hash's difficulty; impossible
    bests (below the netdiff they beat) are recomputed from the hash."""
    check("hashAchievedDiff exists (BigInt over the D1 target)",
          "function hashAchievedDiff(hexHash)" in SRC)
    check("impossible bests healed from the block's own hash",
          "b.best < nd" in SRC and "best healed for" in SRC)
    import shutil, subprocess, tempfile, json as _json
    if not shutil.which("node"):
        print("SKIP  hashDiff functional"); return
    js = """
const SV2_D1 = 0xffffn << 208n;
%s
// Chris's block 963169 hash: 19 leading hex zeros
const d = hashAchievedDiff('000000000000000001bd2713e80864c01cbdacbf8366acaa06fe2a13192f6e0d');
console.log(JSON.stringify({ d, aboveNet: d > 435e9, sane: d < 1e18 }));
""" % _extract_fn(SRC, "function hashAchievedDiff(")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    try: d = _json.loads(r.stdout.strip().split("\n")[-1])
    except Exception:
        check("hashDiff functional run", False, (r.stdout + r.stderr)[:200]); return
    check("963169's real solve diff beats the 435G netdiff (not 804K)",
          d["aboveNet"] and d["sane"])


def test_round_samples_enable_late_effort():
    """The status merge is client-driven: page closed at solve = detection
    fires when the user next opens the dashboard, possibly an hour later,
    when the live snapshot is stale (963169: dash). A once-a-minute persisted
    round sample lets late detection read the round as it stood at solve."""
    check("samples persisted once a minute, bounded",
          "round_samples.json" in SRC and "now - sv2LastSampleTs < 60" in SRC and "slice(-2880)" in SRC)
    check("nearest-sample lookup bounded to 10 min", "dt < 600" in SRC)
    check("attach falls back to the sample store",
          "b.effortSrc = 'sample'" in SRC)
    check("null-fill heal learns the sample store too",
          "effort filled for" in SRC and "(round sample)" in SRC)


def test_identity_join_by_request_id():
    """Current pool build splits identity across Open (request_id, identity)
    and Success (request_id, channel_id). Joined by request_id."""
    check("pending map + both regexes", "sv2PendingIdent" in SRC and "SV2_RE_OPEN_REQ" in SRC and "SV2_RE_OPEN_OK2" in SRC)
    import re as _re, shutil, subprocess, tempfile, json as _json
    if not shutil.which("node"):
        print("SKIP  join functional"); return
    js = """
const sv2State = { channels: {} };
function sv2Chan(cid) { if (!sv2State.channels[cid]) sv2State.channels[cid] = { name: 'sv2-' + cid, best: 0 }; return sv2State.channels[cid]; }
const sv2PendingIdent = new Map();
const SV2_RE_OPEN_REQ = %s;
const SV2_RE_OPEN_OK2 = %s;
const lines = [
 'INFO pool: Received OpenExtendedMiningChannel: OpenExtendedMiningChannel(request_id: 34, user_identity: addr.miner25, nominal_hash_rate: 5e14)',
 'INFO pool: Sending OpenExtendedMiningChannel.Success (downstream_id: 1): OpenExtendedMiningChannelSuccess(request_id: 34, channel_id: 35, target: U256(00))',
];
for (const line of lines) {
  const oq2 = SV2_RE_OPEN_REQ.exec(line);
  if (oq2) sv2PendingIdent.set(oq2[1], oq2[2]);
  const ok2 = SV2_RE_OPEN_OK2.exec(line);
  if (ok2 && sv2PendingIdent.has(ok2[2])) {
    const idn = sv2PendingIdent.get(ok2[2]); sv2PendingIdent.delete(ok2[2]);
    if (/^[A-Za-z0-9:._\\-]{4,64}$/.test(idn)) {
      const dj = idn.lastIndexOf('.');
      sv2Chan(ok2[1] + ':' + ok2[3]).name = (dj > 0 && dj < idn.length - 1) ? idn.slice(dj + 1) : idn;
    }
  }
}
console.log(JSON.stringify({ name: (sv2State.channels['1:35']||{}).name }));
"""
    m1 = _re.search(r"const SV2_RE_OPEN_REQ = (/.*?/);", SRC).group(1)
    m2 = _re.search(r"const SV2_RE_OPEN_OK2 = (/.*?/);", SRC).group(1)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js % (m1, m2)); pth = f.name
    r = subprocess.run(["node", pth], capture_output=True, text=True)
    try: d = _json.loads(r.stdout.strip().split("\n")[-1])
    except Exception:
        check("join functional run", False, (r.stdout + r.stderr)[:200]); return
    check("channel 1:35 named miner25 via the request_id join", d["name"] == "miner25")


def test_cross_protocol_round_reset():
    """Chris (2026-08-31): an SV2 block reset the SV2 round but asicseer's
    SV1 round kept climbing (it only resets on its own solves). A persisted
    SV1 baseline makes any local block zero the whole fleet's round."""
    check("sv1 baseline persisted alongside round start",
          "sv1Base: sv1RoundBase" in SRC)
    check("SV1 round displayed relative to the baseline",
          "out.roundShares = Math.max(0, out.accepted - sv1RoundBase);" in SRC)
    check("asicseer self-reset re-zeroes the baseline",
          "if (out.accepted < sv1RoundBase) sv2SetRoundStart(sv2RoundStartTs, 0);" in SRC)
    check("local-block detection stamps the SV1 baseline",
          "sv2SetRoundStart(Math.floor(Date.now() / 1000), sv2State._sv1AccRaw ?? null);" in SRC)


def test_braiins_name_level_hashrate():
    """Chris (2026-08-31): a Braiins rental fans one worker name across
    hundreds of short-lived connections (downstream_id reached 578); the
    aggregated row's rate swung 500T<->1P as cold and dying channel windows
    entered and left the sum. The merged name's rate is now computed from
    the raw share ring across all its channels: one window, one truth."""
    check("name-level recompute over the raw share ring",
          "nameOfCid[c2] === m.name" in SRC and "work * 4294967296 / 300" in SRC)
    check("bounded to a 5-minute window", "nowMs2 - ts2 <= 300000" in SRC)


if __name__ == "__main__":
    print("unified worker schema regression tests:")
    test_both_protocols_emit_one_schema()
    test_accepted_present_for_sv2()
    test_sv1_rejects_are_honest()
    test_trend_means_the_same_thing()
    test_named_windows_exist()
    test_sv2_restart_persistence()
    test_reset_clears_counters()
    test_block_resets_best_fleet_wide()
    test_fleet_aggregation_sums_unified_fields()
    test_no_temporal_dead_zone()
    test_extranonce2_setting()
    test_ui_is_labelled()
    test_sv2_log_download()
    test_sv2_found_blocks_create_entries()
    test_addr_matching_normalized_by_node()
    test_sv2_best_diff_is_solve_not_network()
    test_sv2_reset_survives_translator_merge()
    test_block_round_effort()
    test_celebration_holds_for_screenshots()
    test_sv1_effort_declaration_wins()
    test_round_watermark_survives_restart_replay()
    test_effort_units_and_races()
    test_celebration_survives_refresh()
    test_declarations_are_durable()
    test_ghost_workers_expire_fast()
    test_effort_machinery_runs_for_sv1_only()
    test_external_blocks_paid_not_found()
    test_block_best_is_chain_authoritative()
    test_round_samples_enable_late_effort()
    test_identity_join_by_request_id()
    test_cross_protocol_round_reset()
    test_braiins_name_level_hashrate()
    if FAILURES:
        print(f"\n{len(FAILURES)} FAILURE(S): {FAILURES}")
        sys.exit(1)
    print("\nALL UNIFIED SCHEMA TESTS PASSED")
