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

HAND_TO_MANAGER = frozenset({"escalate", "unresolvable", "needs_human_label", "stuck_active"})

REVIEW_FIELDS = "state,headRefOid,labels,isDraft"


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


# ── live re-check ────────────────────────────────────────────────────────

async def live_recheck(tagged: list, gh_runner, *, concurrency: int = gm.GH_MAX_CONCURRENCY) -> "tuple[list, list]":
    """``(still_open, dropped)``. Each kept PR gains ``head_sha`` and fresh
    ``labels`` (and ``run_id`` if the label moved). A PR whose state cannot be
    read is KEPT out of the nudge (it is neither woken nor handed over) and
    reported in ``dropped`` with the reason, never silently."""
    still_open: list = []
    dropped: list = []

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
        still_open.append({**pr, "head_sha": view.get("headRefOid") or "", "labels": labels,
                           "run_id": run_id})

    await gm._bounded_gather(tagged, _one, concurrency=concurrency)
    return still_open, dropped


def _ident(pr: dict) -> dict:
    return {"repo": pr.get("repo"), "number": pr.get("number"), "url": pr.get("url")}


# ── persisted verdicts ───────────────────────────────────────────────────

def pr_key(pr: dict) -> str:
    return f"{pr.get('repo')}#{pr.get('number')}"


def load_store(path: str) -> dict:
    return gm.load_surfaced_store(path)


def save_store(path: str, store: dict) -> None:
    gm.save_surfaced_store(path, store)


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
        if verdict in HAND_TO_MANAGER:
            state = gm.surfaced_backoff_state(pr.get("updated_at"), prior.get("stamp"), now)
            if state == "eligible":
                entry["handed_off"] = True
                entry["stamp"] = {"surfaced_at": now, "updated_at": pr.get("updated_at")}
        new_store[key] = entry
    return new_store


def _outage_path(store_path: str) -> str:
    return store_path + ".outage"


def mark_outage(store_path: str, now: float, reason: str) -> None:
    """Persist that the last ``nudge=true`` failed, so the Manager's plain
    re-read does not replay a pre-outage verdict that hides a PR."""
    try:
        with open(_outage_path(store_path), "w") as fh:
            json.dump({"at": now, "reason": reason}, fh)
    except OSError:
        log.warning("could not persist aw-run outage marker", exc_info=True)


def clear_outage(store_path: str) -> None:
    try:
        os.remove(_outage_path(store_path))
    except FileNotFoundError:
        pass
    except OSError:
        log.warning("could not clear aw-run outage marker", exc_info=True)


def recent_outage(store_path: str, now: float) -> "str | None":
    """Reason of the last failed nudge if no successful one has cleared it
    (and it is younger than :data:`VERDICT_TTL_S`), else ``None``."""
    try:
        with open(_outage_path(store_path)) as fh:
            data = json.load(fh)
        if now - float(data.get("at") or 0) > VERDICT_TTL_S:
            return None
        return str(data.get("reason") or "nudge failed")
    except (OSError, ValueError, TypeError, AttributeError):
        return None


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
        if not e.get("handed_off"):
            continue
        reason = e.get("verdict") or ""
        detail = e.get("reason")
        prs.append({
            "repo": e.get("repo"), "number": e.get("number"), "url": e.get("url"),
            "title": e.get("title"), "author": None, "badge": None,
            "updated_at": e.get("updated_at"), "age_hours": None,
            "attention_reason": f"aw-run: {reason}" + (f" ({detail})" if detail else ""),
            "aw_run": {"verdict": reason, "run_id": e.get("run_id"),
                       "wake_count": e.get("wake_count")},
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
    unverified = [d for d in result.get("dropped") or [] if str(d.get("reason", "")).startswith("live re-check")]
    if unverified and not result.get("error"):
        # A PR whose live state could not be read is not a confirmed closure:
        # keep the fail-open decision (and the outage marker) for the Manager.
        result["error"] = f"live re-check failed for {len(unverified)} PR(s): {unverified[0]['reason']}"
    if not dry_run:
        if result.get("error"):
            mark_outage(store_path, now, result["error"])
        else:
            clear_outage(store_path)
    return result


async def _run_nudge(tagged, gh_runner, store_path, now, dry_run, client, started) -> dict:
    try:
        still_open, dropped = await asyncio.wait_for(
            live_recheck(tagged, gh_runner), timeout=LIVE_RECHECK_BUDGET_S)
    except asyncio.TimeoutError:
        return {"error": f"live re-check exceeded {LIVE_RECHECK_BUDGET_S:.0f}s", "dropped": [],
                "nudged": 0}
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
                     "head_sha": p.get("head_sha") or "", "labels": p.get("labels") or []}
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
