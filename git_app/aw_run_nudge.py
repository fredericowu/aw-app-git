"""GitHub Maintenance: ``aw-run:<run id>`` tagged PRs (Kanban 41e5bf3b).

The gate (``GET /github/maintenance?nudge=true``) pulls tagged PRs out of the
Manager's queue and sends them to AP-MT's chain-idle nudge, which owns ALL
policy (chain discovery, idle threshold, wake cap, the wake itself). This module
only does the plumbing around it:

* live re-check — the scan cache is up to ~30 min old, and a merged PR must not
  wake anyone;
* the loopback client (copied from aw-app-notion's ``apmt.py``: an app cannot
  read another app's config, so the call goes through aw-app-agents-platform-
  runners, which owns the AP-MT address and identity token);
* a persisted verdict store, so a plain ``GET`` (the Manager re-reads the gate
  without ``nudge``) applies the LAST verdict instead of showing the tagged PRs
  again;
* the partition: which tagged PRs the Manager still sees.

Transport failure fails OPEN to today's behaviour for that call (tagged PRs go
through the normal attention filter) and says why in ``aw_run.error``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx

from . import github_maintenance as gm
from . import github_prs

log = logging.getLogger("aw_apps.git.aw_run_nudge")

RUNNERS_APP_ID = "agents-platform-runners"
API_KEY_VAR = "AW_WORKSPACE_API_KEY"
CONTAINER_DIR = os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace")
# Inside the dag http node's timeout (30s): live re-check (LIVE_RECHECK_BUDGET_S)
# + this call must fit. The runners proxy gives AP-MT 15s, so this stays above
# it: the proxy always expires first and we never report a transport failure
# for a nudge that actually completed.
TIMEOUT_S = 17.0
LIVE_RECHECK_BUDGET_S = 9.0
#: Wall-clock cap for the whole nudge (re-check + call). httpx applies its
#: timeout per phase (connect/write/read), so the client timeout alone does not
#: bound the total; ``run_nudge`` enforces this with ``asyncio.wait_for`` and
#: gives the call only what is left of it. Stays under the workflow's 30s.
TOTAL_DEADLINE_S = 25.0

STORE_REL = "data/git/github_maintenance_aw_run.json"
#: A persisted verdict older than this is not applied by a plain GET (the
#: nudge hasn't run for two 3h cadences) — tagged PRs fall back to the normal
#: attention filter rather than staying hidden on a dead verdict.
VERDICT_TTL_S = 6 * 3600.0

#: An ``active`` verdict (AP-MT sees a live run in the chain) is only trusted
#: for this long. AP-MT's own zombie cut-off is 24h, so a run that died without
#: its status being finalized reads "active" for a day; the gate expires the
#: verdict itself when the PR has also been idle (no push, label or comment
#: moved ``updatedAt``) for as long, and hands it to the Manager as
#: ``stuck_active``.
ACTIVE_VERDICT_MAX_AGE_S = float(os.environ.get("AW_GIT_ACTIVE_VERDICT_MAX_AGE_S", 2 * 3600))
#: Verdicts that mean "waiting on a timer" with no agent actually progressing.
#: On a green PR still carrying stale ``devteam:*`` labels they must not hide it.
PASSIVE_VERDICTS = frozenset({"recent", "waiting_ci", "active"})

#: Fallback for an AP-MT that predates the per-verdict ``hand_to_manager`` flag.
#: ``needs_human_label`` is legacy: the ``devteam:needs-human`` PR label is
#: retired (human-needed state lives on the Kanban card / ``ask_human``), kept
#: here only so a stray label or an older AP-MT still escalates, never nudges.
HAND_TO_MANAGER = frozenset({"escalate", "unresolvable", "needs_human_label", "stuck_active",
                             "awaiting_fixes", "out_of_repo"})
#: Labels meaning "the pipeline found blocking findings; a coder owes a push".
#: AP-MT answers ``waiting_human`` (reason null) when a stale ask_human question
#: is still open on the chain; with one of these labels that is NOT a human hold.
AWAITING_FIXES_LABELS = ("devteam:awaiting-fixes", "devteam:fix-cycle")

REVIEW_FIELDS = ("state,headRefOid,labels,isDraft,mergeable,mergeStateStatus,statusCheckRollup,"
                 "reviewDecision,updatedAt,headRefName")

#: Retired label; read only to tolerate a stray one on an old PR.
NEEDS_HUMAN_LABEL = "devteam:needs-human"
#: Labels meaning a review left blocking findings on the CURRENT head. The
#: shared workflow drops ``awaiting-fixes`` on the coder's push and keeps
#: ``devteam:fix-cycle`` only as round history, so ``fix-cycle`` alone is not a
#: pending-fix signal.
BLOCKING_FIX_LABELS = ("devteam:awaiting-fixes",)
#: AP-MT's ``unresolvable`` reason when the ``aw-run`` label id is unknown to it
#: (matched only as a fallback for an AP-MT that does not send
#: ``link_resolution: "unresolved"`` on the verdict).
UNLINKED_REASON_PREFIX = "run not found for aw-run label"
MERGE_ELIGIBLE_CONTEXT = "devteam/merge-eligible"
#: The shared workflow's own contexts (code-review-reusable.yml, merge-attempt's
#: ADVISORY set). They never gate a merge through raw pass/fail, so a red one
#: must not make ``ci`` red. Matched on the full name and on the segment after
#: the last ``/`` (a reusable job reports as "<caller job> / <job>").
ADVISORY_CONTEXTS = frozenset({
    "devteam/ai-review", "devteam/code-review", "devteam/merge-eligible",
    "devteam/provenance", "devteam/review-cap",
    "ai-review", "code-review", "merge-eligible", "provenance-gate", "gate",
    "fork-notice", "merged-event", "merge-attempt", "CodeRabbit",
})


class NudgeError(RuntimeError):
    """The nudge could not run (runners app missing, no token, AP-MT down)."""


def default_store_path() -> str:
    home = os.environ.get("AW_WORKSPACE_HOME") or os.path.expanduser("~/.aw-workspace")
    return os.path.join(home, STORE_REL)


# ── loopback client ──────────────────────────────────────────────────────

def _from_env_file(name: str) -> "str | None":
    home = os.environ.get("AW_WORKSPACE_HOME") or os.path.join(CONTAINER_DIR, ".aw-workspace")
    try:
        with open(os.path.join(home, ".env"), "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(f"{name}="):
                    return line.split("=", 1)[1].strip() or None
    except OSError:
        return None
    return None


def _runners_url(path: str) -> str:
    base = os.environ.get("AW_LOCAL_API_URL") or \
        f"http://127.0.0.1:{os.environ.get('AW_PORT', '9030')}"
    return f"{base.rstrip('/')}/api/apps/{RUNNERS_APP_ID}{path}"


def call_nudge(body: dict) -> dict:
    """Blocking; call via ``asyncio.to_thread``. Raises :class:`NudgeError`."""
    key = os.environ.get(API_KEY_VAR) or _from_env_file(API_KEY_VAR)
    if not key:
        raise NudgeError(f"{API_KEY_VAR} is not set — cannot reach {RUNNERS_APP_ID}")
    url = _runners_url("/github/aw-run-pr-nudge")
    try:
        with httpx.Client(timeout=TIMEOUT_S) as client:
            resp = client.post(url, json=body, headers={"X-Api-Key": key})
    except httpx.HTTPError as exc:
        raise NudgeError(f"{RUNNERS_APP_ID} unreachable: {type(exc).__name__}: {exc}") from exc
    if resp.status_code == 404:
        raise NudgeError(f"{RUNNERS_APP_ID} has no /github/aw-run-pr-nudge route (404)")
    if resp.status_code >= 400:
        raise NudgeError(f"nudge refused ({resp.status_code}): {resp.text[:200]}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise NudgeError("nudge returned a non-JSON body") from exc
    if not isinstance(data, dict) or not isinstance(data.get("verdicts"), list):
        raise NudgeError("nudge returned an unexpected body")
    return data


# ── facts for the AP-MT decision ─────────────────────────────────────────

def _is_advisory(name: str) -> bool:
    name = (name or "").strip()
    return name in ADVISORY_CONTEXTS or name.rsplit("/", 1)[-1].strip() in ADVISORY_CONTEXTS


def _rollup_entry(e: dict) -> "tuple[str, str]":
    """``(name, outcome)`` with outcome in green/red/pending for a check run or
    a commit status."""
    if e.get("__typename") == "StatusContext" or "context" in e:
        name = e.get("context") or ""
        state = (e.get("state") or "").upper()
        if state == "SUCCESS":
            return name, "green"
        if state in ("PENDING", "EXPECTED"):
            return name, "pending"
        return name, "red"
    name = e.get("name") or ""
    if (e.get("status") or "").upper() != "COMPLETED":
        return name, "pending"
    conclusion = (e.get("conclusion") or "").upper()
    if conclusion in ("SUCCESS", "NEUTRAL", "SKIPPED"):
        return name, "green"
    return name, "red"


def derive_ci(rollup) -> str:
    """green / red / pending / unknown over the NON-advisory checks only.
    No non-advisory check at all is green: ``mergeable``/``merge_state`` still
    has to be CLEAN, which is where required checks show up."""
    if rollup is None:
        return "unknown"
    seen = {"green": 0, "red": 0, "pending": 0}
    for e in rollup:
        if not isinstance(e, dict):
            continue
        name, outcome = _rollup_entry(e)
        if _is_advisory(name):
            continue
        seen[outcome] += 1
    if seen["red"]:
        return "red"
    if seen["pending"]:
        return "pending"
    return "green"


def merge_eligible(rollup) -> bool:
    for e in rollup or []:
        if isinstance(e, dict) and e.get("context") == MERGE_ELIGIBLE_CONTEXT \
                and (e.get("state") or "").upper() == "SUCCESS":
            return True
    return False


def parse_json_arrays(text: str) -> list:
    """``gh api --paginate`` prints one JSON array per page, back to back."""
    out: list = []
    dec = json.JSONDecoder()
    i, n = 0, len(text or "")
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        val, i = dec.raw_decode(text, i)
        out.extend(val if isinstance(val, list) else [val])
    return out


def needs_human_actor(events: list) -> str:
    """bot / human / unknown: who applied the CURRENT (stray, retired)
    ``devteam:needs-human``
    (the last ``labeled`` event for it). Anything unreadable is ``unknown``,
    which AP-MT treats like a human."""
    last = None
    for ev in events:
        if isinstance(ev, dict) and ev.get("event") == "labeled" \
                and (ev.get("label") or {}).get("name") == NEEDS_HUMAN_LABEL:
            last = ev
    actor = (last or {}).get("actor") or {}
    login = actor.get("login") or ""
    if not login:
        return "unknown"
    if actor.get("type") == "Bot" or login.endswith("[bot]"):
        return "bot"
    return "human"


async def read_needs_human_by(pr: dict, gh_runner) -> str:
    repo, number = pr.get("repo"), pr.get("number")
    if not repo or not number:
        return "unknown"
    code, out, _ = await gh_runner(
        ["api", "--paginate", f"repos/{repo}/issues/{number}/events?per_page=100"])
    if code != 0:
        return "unknown"
    try:
        return needs_human_actor(parse_json_arrays(out))
    except ValueError:
        return "unknown"


def build_facts(view: dict, labels: list, needs_human_by: "str | None") -> dict:
    """The ``facts`` block AP-MT's ``decide()`` consumes (PR 1 of card 41e5bf3b).
    ``needs_human_by`` is only read for a PR that carries the label."""
    rollup = view.get("statusCheckRollup")
    return {
        "ci": derive_ci(rollup if isinstance(rollup, list) else None),
        "mergeable": view.get("mergeable") or "",
        "merge_state": view.get("mergeStateStatus") or "",
        "merge_eligible": merge_eligible(rollup if isinstance(rollup, list) else None),
        "draft": bool(view.get("isDraft")),
        "needs_human_by": needs_human_by or "unknown",
    }


# ── live re-check ────────────────────────────────────────────────────────

async def live_recheck(tagged: list, gh_runner, *, concurrency: int = gm.GH_MAX_CONCURRENCY,
                       sink: "tuple[list, list] | None" = None) -> "tuple[list, list]":
    """``(still_open, dropped)``. Each kept PR gains ``head_sha`` and fresh
    ``labels`` (and ``run_id`` if the label moved). A PR whose state cannot be
    read is KEPT out of the nudge (it is neither woken nor handed over) and
    reported in ``dropped`` with the reason, never silently."""
    # ``sink`` lets the caller keep the checks that finished if the batch is cut
    # off by the shared deadline (one slow PR must not discard the others).
    still_open, dropped = sink if sink is not None else ([], [])

    async def _one(pr: dict) -> None:
        code, out, err = await gh_runner(["pr", "view", pr["url"], "--json", REVIEW_FIELDS])
        if code != 0:
            dropped.append({**_ident(pr), "reason": f"live re-check failed: {(err or out).strip()[:160]}"})
            return
        try:
            view = json.loads(out or "{}")
        except ValueError:
            dropped.append({**_ident(pr), "reason": "live re-check returned malformed output"})
            return
        if (view.get("state") or "").upper() != "OPEN":
            dropped.append({**_ident(pr), "reason": f"no longer open ({view.get('state')})"})
            return
        labels = gm.label_names(view.get("labels"))  # live labels only; never the cached ones
        run_id = gm.extract_aw_run_id(labels)
        if not run_id:
            dropped.append({**_ident(pr), "reason": "aw-run label removed"})
            return
        # Timeline only for a PR with a stray (retired) needs-human label: the one
        # extra call per PR, and the only row that needs the actor.
        by = await read_needs_human_by(pr, gh_runner) if NEEDS_HUMAN_LABEL in labels else None
        # Stale-label state is recomputed from the LIVE labels, CI and activity,
        # never carried over from the cached scan (a push may have cleared it).
        badge, ci = github_prs.classify_pr(view)
        updated_at = view.get("updatedAt") or pr.get("updated_at")
        at = _parse_ts(updated_at)
        age_hours = None if at is None else (time.time() - at) / 3600.0
        stale = gm.stale_devteam_labels(labels, ci, view.get("mergeable"), age_hours)
        still_open.append({**pr, "head_sha": view.get("headRefOid") or "", "labels": labels,
                           "run_id": run_id, "facts": build_facts(view, labels, by),
                           "head_ref": view.get("headRefName") or "",
                           "stale_labels": stale, "badge": badge, "updated_at": updated_at})

    await gm._bounded_gather(tagged, _one, concurrency=concurrency)
    return still_open, dropped


async def attach_live_labels(entries: list, gh_runner, *, budget_s: float = 6.0) -> None:
    """Set ``entry['aw_run']['labels']`` to the PR's live label names, read with
    the gate's authenticated ``gh`` (the Manager's own ``gh`` is unauthenticated
    and cannot see private PRs). ``None`` means the labels could not be read."""
    for e in entries:
        e.setdefault("aw_run", {})["labels"] = None

    async def _one(e: dict) -> None:
        code, out, _ = await gh_runner(["pr", "view", e["url"], "--json", "labels"])
        if code != 0:
            return
        try:
            e["aw_run"]["labels"] = gm.label_names(json.loads(out or "{}").get("labels"))
        except ValueError:
            pass

    try:
        await asyncio.wait_for(gm._bounded_gather(entries, _one, concurrency=gm.GH_MAX_CONCURRENCY),
                               timeout=budget_s)
    except asyncio.TimeoutError:
        pass


def _ident(pr: dict) -> dict:
    return {"repo": pr.get("repo"), "number": pr.get("number"), "url": pr.get("url")}


# ── persisted verdicts ───────────────────────────────────────────────────

def pr_key(pr: dict) -> str:
    return f"{pr.get('repo')}#{pr.get('number')}"


def load_store(path: str) -> dict:
    return gm.load_surfaced_store(path)


def save_store(path: str, store: dict) -> None:
    gm.save_surfaced_store(path, store)


def _parse_ts(value) -> "float | None":
    return gm._parse_iso_datetime(value) if value else None


def active_expired(verdict, active_since, updated_at, now: float) -> bool:
    """True when an ``active`` verdict has outlived :data:`ACTIVE_VERDICT_MAX_AGE_S`
    AND the PR itself shows no sign of life for as long."""
    if verdict != "active" or active_since is None:
        return False
    if now - float(active_since) <= ACTIVE_VERDICT_MAX_AGE_S:
        return False
    upd = _parse_ts(updated_at)
    return upd is None or now - upd > ACTIVE_VERDICT_MAX_AGE_S


def unlinked_fix_labels(verdict: dict, pr: dict) -> "list[str]":
    """Blocking-findings labels on a PR whose ``aw-run`` id AP-MT cannot link
    to any run, else ``[]``. No run means nobody to wake, and the PR would sit
    with findings no one is fixing."""
    if verdict.get("verdict") != "unresolvable":
        return []
    if NEEDS_HUMAN_LABEL in (pr.get("labels") or []):
        return []  # human hold takes precedence over a coder dispatch
    unlinked = verdict.get("link_resolution") == "unresolved" \
        or str(verdict.get("reason") or "").startswith(UNLINKED_REASON_PREFIX)
    if not unlinked:
        return []
    return [lb for lb in BLOCKING_FIX_LABELS if lb in (pr.get("labels") or [])]


def unlinked_fix_reason(labels: list, head_ref: str, run_id) -> str:
    branch = f"branch `{head_ref}`" if head_ref else "the PR's existing head branch"
    return (f"{', '.join(labels)} but aw-run id {run_id} is unknown to AP-MT, so no run can be woken. "
            f"Dispatch a fresh coder on {branch} of this open PR to fix the review findings. "
            "Push to the same branch: no new PR, no /aw-review, no merge")


def plan_nudge_result(tagged: list, verdicts: list, store: dict, now: float) -> dict:
    """Fold fresh verdicts into the persisted store (pure; returns the new
    store). A Manager-bound verdict goes through the SAME surfaced back-off as
    ordinary attention PRs, so the Manager is not re-fed every cadence: the
    stamp lives on the entry, ``handed_off`` records whether THIS call put it
    in front of the Manager (a plain GET replays that decision)."""
    by_key = {pr_key(p): p for p in tagged}
    new_store = dict(store)
    for v in verdicts:
        key = pr_key(v)
        pr = by_key.get(key) or {}
        prior = store.get(key) or {}
        verdict = v.get("verdict")
        entry = {"verdict": verdict, "reason": v.get("reason"), "wake_count": v.get("wake_count"),
                 "woken_run_id": v.get("woken_run_id"), "idle_s": v.get("idle_s"),
                 "run_id": v.get("run_id") or pr.get("run_id"), "at": now,
                 "stamp": prior.get("stamp"), "handed_off": False,
                 "repo": v.get("repo"), "number": v.get("number"), "url": v.get("url") or pr.get("url"),
                 "title": pr.get("title"), "updated_at": pr.get("updated_at")}
        if verdict == "active":
            # First time seen active starts the clock; consecutive actives keep it.
            # The clock belongs to a run: a different tagged run starts afresh.
            same_run = prior.get("run_id") == entry["run_id"]
            entry["active_since"] = prior.get("active_since") if same_run \
                and prior.get("verdict") in ("active", "stuck_active") \
                and prior.get("active_since") else now
        flag = v.get("hand_to_manager")
        handoff = flag if flag is not None else verdict in HAND_TO_MANAGER
        if verdict == "waiting_human":
            held = [l for l in AWAITING_FIXES_LABELS if l in (pr.get("labels") or [])]
            if held:
                # Not a human hold: the coder owes a push (AP-MT predating its own
                # awaiting_fixes verdict answers waiting_human with no reason).
                entry["verdict"] = "awaiting_fixes"
                entry["reason"] = (f"{held[0]} on the PR; aw-run {entry['run_id']} has an open "
                                   f"question but findings await a coder push, not a human")
                handoff = True
            elif not entry["reason"]:
                entry["reason"] = (f"open ask_human question on aw-run {entry['run_id']} chain; "
                                   f"a human was asked")
        if not handoff and active_expired(verdict, entry.get("active_since"), pr.get("updated_at"), now):
            entry["verdict"] = "stuck_active"
            entry["reason"] = (f"aw-run chain reads active for "
                               f"{_fmt_hours(now - entry['active_since'])} with no PR activity")
            handoff = True
        elif unlinked_fix_labels(v, pr):
            fix_labels = unlinked_fix_labels(v, pr)
            entry["underlying_verdict"] = verdict
            entry["verdict"] = "unlinked_fix_needed"
            entry["reason"] = unlinked_fix_reason(fix_labels, pr.get("head_ref") or "",
                                                  entry["run_id"])
            entry.update({"head_ref": pr.get("head_ref") or "", "blocking_labels": fix_labels})
            handoff = True
        elif not handoff and verdict in PASSIVE_VERDICTS and pr.get("stale_labels"):
            # Distinct verdict: the Manager routes it by the preserved badge and
            # author association (the ordinary per-PR route), not as an aw-run reason.
            entry["underlying_verdict"] = verdict
            entry["verdict"] = "stale_labels"
            entry["reason"] = "stale labels on green PR: " + ", ".join(pr["stale_labels"])
            entry.update({"stale_labels": list(pr["stale_labels"]), "badge": pr.get("badge"),
                          "author": pr.get("author"),
                          "author_association": pr.get("author_association")})
            handoff = True
        if handoff:
            state = gm.surfaced_backoff_state(pr.get("updated_at"), prior.get("stamp"), now)
            if state == "eligible":
                entry["handed_off"] = True
                entry["stamp"] = {"surfaced_at": now, "updated_at": pr.get("updated_at")}
        new_store[key] = entry
    return new_store


def _outage_path(store_path: str) -> str:
    return store_path + ".outage"


def mark_outage(store_path: str, now: float, reason: str, keys: "list | None" = None) -> None:
    """Persist that the last ``nudge=true`` failed, so the Manager's plain
    re-read does not replay a pre-outage verdict that hides a PR."""
    try:
        with open(_outage_path(store_path), "w") as fh:
            json.dump({"at": now, "reason": reason, "keys": keys}, fh)
    except OSError:
        log.warning("could not persist aw-run outage marker", exc_info=True)


def clear_outage(store_path: str) -> None:
    try:
        os.remove(_outage_path(store_path))
    except FileNotFoundError:
        pass
    except OSError:
        log.warning("could not clear aw-run outage marker", exc_info=True)


def recent_outage(store_path: str, now: float) -> "dict | None":
    """``{reason, keys}`` of the last failed nudge if no clean one has cleared
    it (and it is younger than :data:`VERDICT_TTL_S`), else ``None``. ``keys``
    is ``None`` for a whole-call failure, or the PRs whose live re-check failed."""
    try:
        with open(_outage_path(store_path)) as fh:
            data = json.load(fh)
        if now - float(data.get("at") or 0) > VERDICT_TTL_S:
            return None
        keys = data.get("keys")
        return {"reason": str(data.get("reason") or "nudge failed"),
                "keys": None if keys is None else [str(k) for k in keys]}
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def _fmt_hours(seconds: float) -> str:
    return f"{seconds / 3600.0:.1f}h"


def prune_store(store: dict, open_keys: "set[str]") -> dict:
    return {k: v for k, v in store.items() if k in open_keys}


def manager_prs(store: dict, tagged_keys: "set[str]", now: float) -> "tuple[list, set]":
    """``(prs for the Manager, keys whose verdict was applied)`` from the
    persisted store, ignoring entries past :data:`VERDICT_TTL_S`."""
    prs: list = []
    applied: set = set()
    for key in sorted(tagged_keys):
        e = store.get(key)
        if not e or now - float(e.get("at") or 0) > VERDICT_TTL_S:
            continue
        applied.add(key)
        reason = e.get("verdict") or ""
        detail = e.get("reason")
        if not e.get("handed_off"):
            # A plain GET replays the last decision, but an ``active`` verdict
            # must not hide the PR forever: expire it on read too.
            # Entries written before ``active_since`` existed have an unknown
            # start: judge them on the PR's own idleness alone.
            since = e.get("active_since") or 0.0
            if not active_expired(reason, since, e.get("updated_at"), now):
                continue
            reason = "stuck_active"
            detail = f"aw-run chain reads active with no PR activity for {_fmt_hours(now - (_parse_ts(e.get('updated_at')) or now))}"
        stale_row = reason == "stale_labels"
        prs.append({
            "repo": e.get("repo"), "number": e.get("number"), "url": e.get("url"),
            "title": e.get("title"), "author": e.get("author") if stale_row else None,
            "author_association": e.get("author_association") if stale_row else None,
            "badge": e.get("badge") if stale_row else None,
            "updated_at": e.get("updated_at"), "age_hours": None,
            "attention_reason": (detail if stale_row and detail else
                                 f"aw-run: {reason}" + (f" ({detail})" if detail else "")),
            "aw_run": {"verdict": reason, "run_id": e.get("run_id"),
                       "wake_count": e.get("wake_count"),
                       **({"head_ref": e.get("head_ref"), "blocking_labels": e.get("blocking_labels")}
                          if reason == "unlinked_fix_needed" else {})},
        })
    return prs, applied


def summarize(store: dict, tagged_keys: "set[str]", applied: "set[str]", now: float) -> dict:
    verdicts = []
    counts: dict = {}
    for key in sorted(applied):
        e = store[key]
        verdicts.append({"repo": e.get("repo"), "number": e.get("number"), "url": e.get("url"),
                         "verdict": e.get("verdict"), "reason": e.get("reason"),
                         "wake_count": e.get("wake_count"), "woken_run_id": e.get("woken_run_id"),
                         "handed_off": bool(e.get("handed_off")),
                         "age_s": int(now - float(e.get("at") or now))})
        counts[e.get("verdict")] = counts.get(e.get("verdict"), 0) + 1
    return {"tagged": len(tagged_keys), "applied": len(applied), "verdicts": verdicts,
            "counts": counts}


async def run_nudge(*, tagged: list, gh_runner, store_path: str, now: "float | None" = None,
                    dry_run: bool = False, client=None) -> dict:
    """The ``nudge=true`` half of the gate. Returns ``{error, dropped,
    store}``; ``error`` is set (and ``store`` untouched) when the transport
    failed, which the caller treats as fail-open."""
    now = now if now is not None else time.time()
    started = time.monotonic()
    result = await _run_nudge(tagged, gh_runner, store_path, now, dry_run, client, started)
    # A PR whose live state could not be read is not a confirmed closure, but it
    # must not blind the rest of the batch: only those PRs fail open.
    unverified = sorted({pr_key(d) for d in result.get("dropped") or []
                         if str(d.get("reason", "")).startswith("live re-check")})
    result["unverified"] = unverified
    if not dry_run:
        if result.get("error"):
            mark_outage(store_path, now, result["error"])
        elif unverified:
            mark_outage(store_path, now, f"live re-check failed for {len(unverified)} PR(s)", unverified)
        else:
            clear_outage(store_path)
    return result


async def _run_nudge(tagged, gh_runner, store_path, now, dry_run, client, started) -> dict:
    still_open: list = []
    dropped: list = []
    try:
        await asyncio.wait_for(live_recheck(tagged, gh_runner, sink=(still_open, dropped)),
                               timeout=LIVE_RECHECK_BUDGET_S)
    except asyncio.TimeoutError:
        done = {pr_key(p) for p in still_open} | {pr_key(d) for d in dropped}
        for p in tagged:
            if pr_key(p) not in done:
                dropped.append({**_ident(p), "reason": f"live re-check failed: exceeded {LIVE_RECHECK_BUDGET_S:.0f}s"})
    if dropped and not dry_run:
        # A merged/closed/untagged PR must not keep applying a stale verdict.
        with gm.locked_store(store_path):
            store = load_store(store_path)
            gone = {pr_key(d) for d in dropped if "no longer open" in d["reason"]
                    or "label removed" in d["reason"]}
            if gone & set(store):
                save_store(store_path, {k: v for k, v in store.items() if k not in gone})
    if not still_open:
        return {"error": None, "dropped": dropped, "nudged": 0}
    body = {"prs": [{"repo": p["repo"], "number": p["number"], "url": p["url"],
                     "title": p.get("title") or "", "run_id": p["run_id"],
                     "head_sha": p.get("head_sha") or "", "labels": p.get("labels") or [],
                     "facts": p.get("facts") or None}
                    for p in still_open],
            "dry_run": dry_run}
    remaining = min(TIMEOUT_S, TOTAL_DEADLINE_S - (time.monotonic() - started))
    if remaining <= 0.5:
        return {"error": "no time left for the nudge call", "dropped": dropped, "nudged": 0}
    try:
        data = await asyncio.wait_for(
            asyncio.to_thread(client or call_nudge, body), timeout=remaining)
    except asyncio.TimeoutError:
        return {"error": f"nudge call exceeded its {remaining:.0f}s wall-clock budget",
                "dropped": dropped, "nudged": 0}
    except NudgeError as exc:
        log.warning("aw-run nudge failed open: %s", exc)
        return {"error": str(exc), "dropped": dropped, "nudged": 0}
    except Exception as exc:  # never let the gate die on the nudge
        log.exception("aw-run nudge raised")
        return {"error": f"{type(exc).__name__}: {exc}", "dropped": dropped, "nudged": 0}
    if not dry_run:
        with gm.locked_store(store_path):
            store = load_store(store_path)
            store = plan_nudge_result(still_open, data["verdicts"], store, now)
            save_store(store_path, store)
    return {"error": None, "dropped": dropped, "nudged": len(still_open),
            "verdicts": data["verdicts"]}
