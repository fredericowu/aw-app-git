"""aw-run PR chain-idle nudge plumbing in the maintenance gate (Kanban 41e5bf3b)."""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from git_app import aw_run_nudge, github_maintenance as gm, plugin  # noqa: E402
from tests.test_plugin_routes import FakeCtx  # noqa: E402

RID = "23aeb5c610f6426284ad6fe4ad9f7fa5"


def _lab(*names):
    return [{"name": n} for n in names]


def test_extract_aw_run_id_forms():
    assert gm.extract_aw_run_id(_lab("aw-card:x", f"aw-run:{RID}")) == RID
    assert gm.extract_aw_run_id(_lab("aw-run:23aeb5c6-10f6-4262-84ad-9e5e00000000")) == \
        "23aeb5c610f6426284ad9e5e00000000"
    assert gm.extract_aw_run_id(_lab("aw-run:nothex")) is None
    assert gm.extract_aw_run_id(_lab("aw-agent:coder")) is None
    assert gm.extract_aw_run_id(None) is None


def _search_row(repo, n, labels, draft=False, updated="2026-11-20T00:00:00Z"):
    return {"number": n, "repository": {"nameWithOwner": repo}, "title": "t",
            "url": f"https://github.com/{repo}/pull/{n}", "updatedAt": updated,
            "isDraft": draft, "createdAt": updated, "author": {"login": "a"},
            "labels": labels}


def _scan(rows, view=None):
    async def gh(args):
        if args[0] == "search":
            return 0, json.dumps(rows), ""
        if args[:2] == ["pr", "view"]:
            return 0, json.dumps(view or {"statusCheckRollup": [], "reviewDecision": "",
                                          "mergeable": "MERGEABLE", "isDraft": False,
                                          "updatedAt": "2020-01-01T00:00:00Z"}), ""
        return 1, "", "unexpected"
    store: dict = {}
    res = asyncio.run(gm.scan_pr_attention(
        now=time.time(), gh_runner=gh, cloned_full_names={"o/r"}, owners=["o"], store=store))
    return res, store


def test_scan_splits_tagged_prs_incl_drafts_before_draft_and_backoff_filters():
    rows = [_search_row("o/r", 1, _lab(f"aw-run:{RID}"), draft=True),
            _search_row("o/r", 2, _lab(f"aw-run:{RID}")),
            _search_row("o/r", 3, [])]
    res, store = _scan(rows)
    assert [p["number"] for p in res["aw_run_prs"]] == [1, 2]
    assert res["aw_run_prs"][0]["is_draft"] is True
    # untagged PR #3 still goes the old way and is stamped; tagged ones are NOT
    # in `attention` and NOT stamped.
    assert [p["number"] for p in res["attention"]] == [3]
    assert set(store) == {"o/r#3"}
    # fail-open material: the non-draft tagged PR has its own attention entry
    assert [p["number"] for p in res["aw_run_attention"]] == [2]


# ── route ────────────────────────────────────────────────────────────────

@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(gm, "default_ping_state_path", lambda: str(tmp_path / "state.json"))
    monkeypatch.setattr(aw_run_nudge, "default_store_path", lambda: str(tmp_path / "aw.json"))
    ctx = FakeCtx()
    ctx.has = lambda cap: False
    app = plugin.GitAppPlugin()
    tc = TestClient(app._build_routes(ctx))
    state = {"view": {"state": "OPEN", "headRefOid": "abc", "labels": _lab(f"aw-run:{RID}")},
             "calls": [], "verdict": "woken", "fail": None}

    async def gh(args):
        return 0, json.dumps(state["view"]), ""

    def client(body):
        state["calls"].append(body)
        if state["fail"]:
            raise aw_run_nudge.NudgeError(state["fail"])
        return {"ok": True, "verdicts": [
            {"repo": p["repo"], "number": p["number"], "url": p["url"], "run_id": p["run_id"],
             "verdict": state["verdict"], "reason": "woken 3x, head unchanged",
             "wake_count": 3} for p in body["prs"]]}

    monkeypatch.setattr(aw_run_nudge, "call_nudge", client)
    app._maintenance._gh_runner = gh
    return tc, app, state, tmp_path


def _warm(app, *, prs=(), tagged=(), attention=()):
    app._maintenance._cache = {
        "prs": list(prs), "aw_run_prs": list(tagged), "aw_run_attention": list(attention),
        "failing_flows": [], "counts": {}, "errors": [], "last_poll": 123.0, "error": None}


def _tagged(n=7):
    return {"repo": "o/r", "number": n, "url": f"https://github.com/o/r/pull/{n}", "title": "t",
            "run_id": RID, "labels": [f"aw-run:{RID}"], "is_draft": False,
            "updated_at": "2026-11-20T00:00:00Z"}


def _att(n=7):
    return {"repo": "o/r", "number": n, "url": f"https://github.com/o/r/pull/{n}", "title": "t",
            "author": "a", "badge": "tests-failed", "updated_at": "2026-11-20T00:00:00Z",
            "age_hours": 1.0, "attention_reason": "badge:tests-failed"}


def test_nudge_woken_keeps_tagged_pr_away_from_manager_and_no_work(env):
    tc, app, state, _ = env
    state["verdict"] = "woken"
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true").json()
    assert r["has_work"] is False and r["prs"] == []
    assert r["aw_run"]["counts"] == {"woken": 1} and r["aw_run"]["error"] is None
    assert state["calls"][0]["prs"][0]["head_sha"] == "abc"
    # plain GET (the Manager's re-read) applies the persisted verdict
    r2 = tc.get("/github/maintenance").json()
    assert r2["prs"] == [] and r2["has_work"] is False and len(state["calls"]) == 1


def test_escalate_hands_to_manager_with_aw_run_reason_then_backs_off(env):
    tc, app, state, _ = env
    state["verdict"] = "escalate"
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true").json()
    assert r["has_work"] is True and len(r["prs"]) == 1
    assert r["prs"][0]["attention_reason"].startswith("aw-run: escalate")
    # Manager re-reads with a plain GET: still handed over
    assert len(tc.get("/github/maintenance").json()["prs"]) == 1
    # next tick: surfaced back-off keeps the Manager from being re-fed
    r3 = tc.get("/github/maintenance?nudge=true").json()
    assert r3["prs"] == [] and r3["has_work"] is False


def test_untagged_prs_unchanged(env):
    tc, app, state, _ = env
    fresh = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())  # inside the idle_open grace
    _warm(app, prs=[_att(1)], tagged=[{**_tagged(7), "updated_at": fresh}], attention=[_att(7)])
    state["verdict"] = "recent"
    r = tc.get("/github/maintenance?nudge=true").json()
    assert [p["number"] for p in r["prs"]] == [1] and r["has_work"] is True


def test_transport_failure_fails_open_with_error(env):
    tc, app, state, _ = env
    state["fail"] = "runners app unreachable"
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true").json()
    assert [p["number"] for p in r["prs"]] == [7] and r["has_work"] is True
    assert "unreachable" in r["aw_run"]["error"]


def test_merged_pr_is_dropped_and_never_nudged(env):
    tc, app, state, _ = env
    state["view"] = {"state": "MERGED", "headRefOid": "x", "labels": _lab(f"aw-run:{RID}")}
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true").json()
    assert not state["calls"] and r["prs"] == [] and r["has_work"] is False
    assert "no longer open" in r["aw_run"]["dropped"][0]["reason"]


def test_plain_get_without_verdict_shows_ordinary_attention(env):
    tc, app, state, _ = env
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance").json()
    assert [p["number"] for p in r["prs"]] == [7] and not state["calls"]


def test_stale_persisted_verdict_is_not_applied(env, tmp_path):
    tc, app, state, _ = env
    _warm(app, tagged=[_tagged()], attention=[_att()])
    state["verdict"] = "woken"
    tc.get("/github/maintenance?nudge=true")
    p = tmp_path / "aw.json"
    d = json.loads(p.read_text())
    for e in d.values():
        e["at"] = time.time() - aw_run_nudge.VERDICT_TTL_S - 10
    p.write_text(json.dumps(d))
    r = tc.get("/github/maintenance").json()
    assert [x["number"] for x in r["prs"]] == [7]


def test_dry_run_persists_nothing_and_reports_verdicts(env, tmp_path):
    tc, app, state, _ = env
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true&nudge_dry_run=true").json()
    assert state["calls"][0]["dry_run"] is True
    assert r["aw_run"]["dry_run_verdicts"][0]["verdict"] == "woken"
    assert not (tmp_path / "aw.json").exists()


def test_store_pruned_to_open_tagged_keys(env, tmp_path):
    tc, app, state, _ = env
    _warm(app, tagged=[_tagged(7), _tagged(8)], attention=[])
    tc.get("/github/maintenance?nudge=true")
    assert len(json.loads((tmp_path / "aw.json").read_text())) == 2
    _warm(app, tagged=[_tagged(7)], attention=[])
    tc.get("/github/maintenance")
    assert list(json.loads((tmp_path / "aw.json").read_text())) == ["o/r#7"]


def test_call_nudge_without_api_key_raises(monkeypatch, tmp_path):
    monkeypatch.delenv("AW_WORKSPACE_API_KEY", raising=False)
    monkeypatch.setenv("AW_WORKSPACE_HOME", str(tmp_path))
    with pytest.raises(aw_run_nudge.NudgeError):
        aw_run_nudge.call_nudge({"prs": []})


def test_label_removed_live_never_wakes_with_cached_label(env):
    tc, app, state, _ = env
    state["view"] = {"state": "OPEN", "headRefOid": "x", "labels": []}
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true").json()
    assert not state["calls"]
    assert "label removed" in r["aw_run"]["dropped"][0]["reason"]


def test_previous_woken_verdict_does_not_hide_pr_during_nudge_outage(env):
    tc, app, state, _ = env
    state["verdict"] = "woken"
    _warm(app, tagged=[_tagged()], attention=[_att()])
    assert tc.get("/github/maintenance?nudge=true").json()["prs"] == []
    state["fail"] = "AP-MT down"
    r = tc.get("/github/maintenance?nudge=true").json()
    assert [p["number"] for p in r["prs"]] == [7] and r["has_work"] is True
    assert "AP-MT down" in r["aw_run"]["error"]
    # the Manager's plain re-read must see the same fail-open decision
    r2 = tc.get("/github/maintenance?limit=20").json()
    assert [p["number"] for p in r2["prs"]] == [7] and r2["has_work"] is True
    assert "AP-MT down" in r2["aw_run"]["error"]
    assert r2["prs"][0]["aw_run"]["fallback"] is True
    assert r2["prs"][0]["aw_run"]["labels"] == [f"aw-run:{RID}"]   # live, read by the gate
    # a later successful nudge clears the outage and replays verdicts again
    state["fail"] = None
    assert tc.get("/github/maintenance?nudge=true").json()["prs"] == []
    assert tc.get("/github/maintenance").json()["prs"] == []


def test_failed_live_recheck_keeps_outage_for_manager_reread(env):
    tc, app, state, _ = env
    state["verdict"] = "woken"
    _warm(app, tagged=[_tagged()], attention=[_att()])
    assert tc.get("/github/maintenance?nudge=true").json()["prs"] == []
    state["fail"] = "AP-MT down"
    tc.get("/github/maintenance?nudge=true")

    async def gh_fail(args):
        return 1, "", "gh: network error"
    app._maintenance._gh_runner = gh_fail
    r = tc.get("/github/maintenance?nudge=true").json()
    assert [p["number"] for p in r["prs"]] == [7] and "live re-check failed" in r["aw_run"]["error"]
    r2 = tc.get("/github/maintenance?limit=20").json()
    assert [p["number"] for p in r2["prs"]] == [7] and r2["has_work"] is True


def test_one_failed_recheck_does_not_hide_fresh_handoff_for_another_pr(env):
    tc, app, state, _ = env
    state["verdict"] = "needs_human_label"
    _warm(app, tagged=[_tagged(7), _tagged(8)], attention=[_att(8)])
    base_view = state["view"]

    async def gh(args):
        if args[2].endswith("/8"):
            return 1, "", "gh: boom"
        return 0, json.dumps(base_view), ""
    app._maintenance._gh_runner = gh
    r = tc.get("/github/maintenance?nudge=true").json()
    assert sorted(p["number"] for p in r["prs"]) == [7, 8]
    assert [c["body"] if False else c["prs"][0]["number"] for c in state["calls"]] == [7]
    r2 = tc.get("/github/maintenance?limit=20").json()
    assert sorted(p["number"] for p in r2["prs"]) == [7, 8]
    assert "live re-check failed" in r2["aw_run"]["error"]


def test_deadline_keeps_completed_checks_and_marks_only_unfinished(env, monkeypatch):
    tc, app, state, _ = env
    state["verdict"] = "needs_human_label"
    monkeypatch.setattr(aw_run_nudge, "LIVE_RECHECK_BUDGET_S", 0.3)
    _warm(app, tagged=[_tagged(7), _tagged(8)], attention=[_att(8)])
    view = state["view"]

    async def gh(args):
        if args[2].endswith("/8"):
            await asyncio.sleep(2)
        return 0, json.dumps(view), ""
    app._maintenance._gh_runner = gh
    r = tc.get("/github/maintenance?nudge=true").json()
    assert [c["prs"][0]["number"] for c in state["calls"]] == [7]
    assert sorted(p["number"] for p in r["prs"]) == [7, 8]
    assert all(p["aw_run"]["labels"] is not None for p in r["prs"] if p["number"] == 7)


def test_handed_off_entries_carry_live_labels_or_null(env):
    tc, app, state, _ = env
    state["verdict"] = "escalate"
    _warm(app, tagged=[_tagged()], attention=[_att()])
    state["view"] = {"state": "OPEN", "headRefOid": "x",
                     "labels": _lab(f"aw-run:{RID}", "devteam:needs-human")}
    r = tc.get("/github/maintenance?nudge=true").json()
    assert "devteam:needs-human" in r["prs"][0]["aw_run"]["labels"]

    async def boom(args):
        return 1, "", "no auth"
    app._maintenance._gh_runner = boom
    r2 = tc.get("/github/maintenance?limit=20").json()
    assert r2["prs"][0]["aw_run"]["labels"] is None


def test_call_exceeding_wall_clock_deadline_fails_open(env, monkeypatch):
    tc, app, state, _ = env
    monkeypatch.setattr(aw_run_nudge, "TIMEOUT_S", 1.0)
    monkeypatch.setattr(aw_run_nudge, "call_nudge", lambda body: __import__("time").sleep(2))
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true").json()
    assert "wall-clock" in r["aw_run"]["error"]
    assert [p["number"] for p in r["prs"]] == [7]


def test_slow_live_recheck_fails_open_within_budget(env, monkeypatch):
    tc, app, state, _ = env
    monkeypatch.setattr(aw_run_nudge, "LIVE_RECHECK_BUDGET_S", 0.05)

    async def slow(args):
        await asyncio.sleep(1)
        return 0, "{}", ""
    app._maintenance._gh_runner = slow
    _warm(app, tagged=[_tagged()], attention=[_att()])
    r = tc.get("/github/maintenance?nudge=true").json()
    assert "live re-check failed" in r["aw_run"]["error"] and not state["calls"]
    assert [p["number"] for p in r["prs"]] == [7]


def test_client_timeout_stays_above_runners_proxy_timeout():
    assert aw_run_nudge.TIMEOUT_S >= 15 + 2
    assert aw_run_nudge.LIVE_RECHECK_BUDGET_S + aw_run_nudge.TIMEOUT_S < 30
    assert aw_run_nudge.TOTAL_DEADLINE_S < 30


# ── facts for AP-MT's deterministic decision (Kanban 41e5bf3b, PR 2) ─────

def _chk(name, status="COMPLETED", conclusion="SUCCESS"):
    return {"__typename": "CheckRun", "name": name, "status": status, "conclusion": conclusion}


def _ctx(context, state="SUCCESS"):
    return {"__typename": "StatusContext", "context": context, "state": state}


def test_ci_ignores_advisory_contexts_incl_reusable_job_tail():
    roll = [_chk("test"), _ctx("devteam/code-review", "FAILURE"), _ctx("devteam/review-cap", "FAILURE"),
            _chk("review / code-review", conclusion="FAILURE"), _chk("merge-attempt", conclusion="CANCELLED")]
    assert aw_run_nudge.derive_ci(roll) == "green"


def test_ci_red_pending_green_unknown():
    assert aw_run_nudge.derive_ci([_chk("test", conclusion="FAILURE")]) == "red"
    assert aw_run_nudge.derive_ci([_chk("test", status="IN_PROGRESS", conclusion="")]) == "pending"
    assert aw_run_nudge.derive_ci([_ctx("ci/x", "PENDING")]) == "pending"
    assert aw_run_nudge.derive_ci([_chk("test"), _chk("lint", conclusion="SKIPPED")]) == "green"
    assert aw_run_nudge.derive_ci([_chk("a", conclusion="FAILURE"), _chk("b", status="QUEUED", conclusion="")]) == "red"
    assert aw_run_nudge.derive_ci(None) == "unknown"


def test_merge_eligible_only_on_success_status():
    assert aw_run_nudge.merge_eligible([_ctx("devteam/merge-eligible")]) is True
    assert aw_run_nudge.merge_eligible([_ctx("devteam/merge-eligible", "FAILURE")]) is False
    assert aw_run_nudge.merge_eligible([_chk("devteam/merge-eligible")]) is False
    assert aw_run_nudge.merge_eligible(None) is False


def _ev(label, login, typ="User", event="labeled"):
    return {"event": event, "label": {"name": label}, "actor": {"login": login, "type": typ}}


def test_needs_human_actor_uses_the_last_labeling_event():
    bot = _ev("devteam:needs-human", "github-actions[bot]", "Bot")
    human = _ev("devteam:needs-human", "fredericowu")
    assert aw_run_nudge.needs_human_actor([bot]) == "bot"
    assert aw_run_nudge.needs_human_actor([bot, human]) == "human"
    assert aw_run_nudge.needs_human_actor([human, bot]) == "bot"
    assert aw_run_nudge.needs_human_actor([_ev("other", "x")]) == "unknown"
    assert aw_run_nudge.needs_human_actor([{"event": "labeled", "label": {"name": "devteam:needs-human"},
                                            "actor": None}]) == "unknown"
    assert aw_run_nudge.needs_human_actor([]) == "unknown"


def test_parse_json_arrays_handles_paginated_output():
    assert aw_run_nudge.parse_json_arrays('[{"a":1}]\n[{"a":2}]') == [{"a": 1}, {"a": 2}]
    assert aw_run_nudge.parse_json_arrays("") == []


def _view(labels, **kw):
    v = {"state": "OPEN", "headRefOid": "abc123", "labels": _lab(*labels), "isDraft": False,
         "mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN",
         "statusCheckRollup": [_chk("test"), _ctx("devteam/code-review", "FAILURE")]}
    v.update(kw)
    return v


def _recheck(view, events=None, events_code=0):
    calls: list = []

    async def gh(args):
        calls.append(args)
        if args[:2] == ["pr", "view"]:
            return 0, json.dumps(view), ""
        if args[0] == "api":
            return events_code, json.dumps(events or []), "boom" if events_code else ""
        return 1, "", "unexpected"
    pr = {"repo": "o/r", "number": 6, "url": "https://github.com/o/r/pull/6", "title": "t"}
    kept, dropped = asyncio.run(aw_run_nudge.live_recheck([pr], gh))
    return kept, dropped, calls


def test_facts_for_a_clean_green_pr_and_no_timeline_read():
    kept, dropped, calls = _recheck(_view([f"aw-run:{RID}", "devteam:review-capped"]))
    assert not dropped
    assert kept[0]["facts"] == {"ci": "green", "mergeable": "MERGEABLE", "merge_state": "CLEAN",
                                "merge_eligible": False, "draft": False, "needs_human_by": "unknown"}
    assert not [c for c in calls if c[0] == "api"], "timeline must only be read for needs-human PRs"


def test_facts_merge_eligible_and_red_ci():
    v = _view([f"aw-run:{RID}"], statusCheckRollup=[_chk("test", conclusion="FAILURE"),
                                                   _ctx("devteam/merge-eligible")])
    kept, _, _ = _recheck(v)
    assert kept[0]["facts"]["ci"] == "red" and kept[0]["facts"]["merge_eligible"] is True


def test_needs_human_pr_reads_the_timeline_once_and_reports_actor():
    ev = [_ev("devteam:needs-human", "github-actions[bot]", "Bot")]
    kept, _, calls = _recheck(_view([f"aw-run:{RID}", "devteam:needs-human"]), ev)
    assert kept[0]["facts"]["needs_human_by"] == "bot"
    api = [c for c in calls if c[0] == "api"]
    assert len(api) == 1 and "repos/o/r/issues/6/events" in api[0][-1]
    kept, _, _ = _recheck(_view([f"aw-run:{RID}", "devteam:needs-human"]), [_ev("devteam:needs-human", "frederico")])
    assert kept[0]["facts"]["needs_human_by"] == "human"


def test_unreadable_timeline_is_unknown_not_bot():
    kept, _, _ = _recheck(_view([f"aw-run:{RID}", "devteam:needs-human"]), events_code=1)
    assert kept[0]["facts"]["needs_human_by"] == "unknown"


def test_facts_are_sent_to_ap_mt_and_draft_flag_propagates():
    sent: list = []

    def client(body):
        sent.append(body)
        return {"verdicts": [{"repo": "o/r", "number": 6, "verdict": "woken", "action": "rereview"}]}

    async def gh(args):
        return 0, json.dumps(_view([f"aw-run:{RID}"], isDraft=True)), ""
    pr = {"repo": "o/r", "number": 6, "url": "https://github.com/o/r/pull/6", "title": "t"}
    res = asyncio.run(aw_run_nudge.run_nudge(tagged=[pr], gh_runner=gh, store_path="/nonexistent/x.json",
                                             dry_run=True, client=client))
    assert res["error"] is None
    assert sent[0]["prs"][0]["facts"]["draft"] is True
    assert sent[0]["prs"][0]["facts"]["ci"] == "green"


def test_hand_to_manager_flag_from_ap_mt_wins_over_the_verdict_name():
    tagged = [{"repo": "o/r", "number": n, "url": f"u{n}", "run_id": RID, "updated_at": "2026-11-20T00:00:00Z"}
              for n in (1, 2, 3, 4)]
    verdicts = [
        {"repo": "o/r", "number": 1, "verdict": "ask_unroutable", "hand_to_manager": True},
        {"repo": "o/r", "number": 2, "verdict": "asked", "hand_to_manager": False},
        {"repo": "o/r", "number": 3, "verdict": "needs_human_label", "hand_to_manager": False},
        {"repo": "o/r", "number": 4, "verdict": "unresolvable"},  # old AP-MT: name fallback
    ]
    store = aw_run_nudge.plan_nudge_result(tagged, verdicts, {}, time.time())
    assert store["o/r#1"]["handed_off"] is True
    assert store["o/r#2"]["handed_off"] is False
    assert store["o/r#3"]["handed_off"] is False
    assert store["o/r#4"]["handed_off"] is True


def test_bot_detected_by_actor_type_alone():
    assert aw_run_nudge.needs_human_actor([_ev("devteam:needs-human", "devteam-app", "Bot")]) == "bot"


# ── expiry of an ``active`` verdict (dead run still reads active) ────────

H = 3600.0
NOWT = 1_800_000_000.0


def _iso_ago(hours):
    import datetime as dt
    return dt.datetime.fromtimestamp(NOWT - hours * H, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _tagged_pr(updated_hours_ago, **extra):
    return {"repo": "o/r", "number": 6, "url": "https://github.com/o/r/pull/6",
            "title": "t", "run_id": RID, "updated_at": _iso_ago(updated_hours_ago), **extra}


def _active_v():
    return {"repo": "o/r", "number": 6, "url": "https://github.com/o/r/pull/6",
            "verdict": "active", "run_id": RID}


def test_active_verdict_within_max_age_is_not_handed_over():
    pr = _tagged_pr(0.5)
    store = aw_run_nudge.plan_nudge_result([pr], [_active_v()], {}, NOWT)
    e = store["o/r#6"]
    assert e["verdict"] == "active" and e["handed_off"] is False and e["active_since"] == NOWT


def test_active_verdict_expires_to_stuck_active_when_pr_idle_too():
    pr = _tagged_pr(5)
    first = aw_run_nudge.plan_nudge_result([pr], [_active_v()], {}, NOWT - 3 * H)
    store = aw_run_nudge.plan_nudge_result([pr], [_active_v()], first, NOWT)
    e = store["o/r#6"]
    assert e["verdict"] == "stuck_active" and e["handed_off"] is True
    assert e["active_since"] == NOWT - 3 * H  # the clock is not reset by the rewrite
    mgr, applied = aw_run_nudge.manager_prs(store, {"o/r#6"}, NOWT)
    assert [p["attention_reason"].startswith("aw-run: stuck_active") for p in mgr] == [True]


def test_active_verdict_does_not_expire_while_pr_is_moving():
    pr = _tagged_pr(0.2)  # a push 12 minutes ago: the agent is alive
    first = aw_run_nudge.plan_nudge_result([pr], [_active_v()], {}, NOWT - 3 * H)
    store = aw_run_nudge.plan_nudge_result([pr], [_active_v()], first, NOWT)
    assert store["o/r#6"]["verdict"] == "active" and not store["o/r#6"]["handed_off"]


def test_plain_get_expires_a_stored_active_verdict_on_read():
    store = {"o/r#6": {"verdict": "active", "at": NOWT - 0.5 * H, "handed_off": False,
                       "active_since": NOWT - 3 * H, "repo": "o/r", "number": 6,
                       "url": "u", "updated_at": _iso_ago(4)}}
    mgr, applied = aw_run_nudge.manager_prs(store, {"o/r#6"}, NOWT)
    assert len(mgr) == 1 and "stuck_active" in mgr[0]["attention_reason"]
    store["o/r#6"]["active_since"] = NOWT - 0.5 * H
    assert aw_run_nudge.manager_prs(store, {"o/r#6"}, NOWT)[0] == []


@pytest.mark.parametrize("verdict", ["recent", "waiting_ci", "active"])
def test_passive_verdict_on_green_pr_with_stale_labels_goes_to_manager(verdict):
    pr = _tagged_pr(3, stale_labels=["devteam:awaiting-fixes"])
    v = {**_active_v(), "verdict": verdict}
    store = aw_run_nudge.plan_nudge_result([pr], [v], {}, NOWT)
    assert store["o/r#6"]["handed_off"] is True
    assert "stale labels" in store["o/r#6"]["reason"]


def test_passive_verdict_without_stale_labels_stays_hidden():
    store = aw_run_nudge.plan_nudge_result([_tagged_pr(0.05)], [{**_active_v(), "verdict": "recent"}], {}, NOWT)
    assert store["o/r#6"]["handed_off"] is False


def test_woken_verdict_is_never_overridden_by_stale_labels():
    pr = _tagged_pr(3, stale_labels=["devteam:fix-cycle"])
    store = aw_run_nudge.plan_nudge_result([pr], [{**_active_v(), "verdict": "woken"}], {}, NOWT)
    assert store["o/r#6"]["handed_off"] is False


def test_stale_label_handoff_has_distinct_verdict_and_keeps_badge_and_author():
    pr = _tagged_pr(3, stale_labels=["devteam:awaiting-fixes"], badge="ready-to-merge",
                    author="octo", author_association="OWNER")
    store = aw_run_nudge.plan_nudge_result([pr], [{**_active_v(), "verdict": "waiting_ci"}], {}, NOWT)
    e = store["o/r#6"]
    assert e["verdict"] == "stale_labels" and e["underlying_verdict"] == "waiting_ci"
    mgr, _ = aw_run_nudge.manager_prs(store, {"o/r#6"}, NOWT)
    assert mgr[0]["badge"] == "ready-to-merge" and mgr[0]["author_association"] == "OWNER"
    assert mgr[0]["author"] == "octo"
    assert not mgr[0]["attention_reason"].startswith("aw-run:")


def test_live_recheck_recomputes_stale_labels_from_live_state():
    import datetime as dt
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    green = {"statusCheckRollup": [_chk("test")], "updatedAt": old}
    kept, _, _ = _recheck(_view([f"aw-run:{RID}", "devteam:fix-cycle"], **green))
    assert kept[0]["stale_labels"] == ["devteam:fix-cycle"]
    # a push removed the label and CI is pending again: nothing stale any more
    kept, _, _ = _recheck(_view([f"aw-run:{RID}"], **green))
    assert kept[0]["stale_labels"] == []
    pending = {"statusCheckRollup": [_chk("test", status="IN_PROGRESS", conclusion="")], "updatedAt": old}
    kept, _, _ = _recheck(_view([f"aw-run:{RID}", "devteam:fix-cycle"], **pending))
    assert kept[0]["stale_labels"] == []


def test_active_timer_resets_when_tagged_run_changes():
    prior = {"o/r#6": {"verdict": "active", "run_id": "b" * 32, "active_since": NOWT - 3 * H}}
    store = aw_run_nudge.plan_nudge_result([_tagged_pr(4)], [_active_v()], prior, NOWT)
    assert store["o/r#6"]["active_since"] == NOWT
    assert store["o/r#6"]["verdict"] == "active"
    prior["o/r#6"]["run_id"] = RID
    store = aw_run_nudge.plan_nudge_result([_tagged_pr(4)], [_active_v()], prior, NOWT)
    assert store["o/r#6"]["active_since"] == NOWT - 3 * H


# ── unlinked PR with blocking findings: fresh-coder handoff ──────────────

def _unlinked_v(**extra):
    return {"repo": "o/r", "number": 6, "url": "https://github.com/o/r/pull/6",
            "verdict": "unresolvable", "reason": "run not found for aw-run label", **extra}


def test_unlinked_pr_with_blocking_labels_gets_a_fresh_coder_handoff():
    label = "devteam:awaiting-fixes"
    pr = _tagged_pr(0.5, labels=[f"aw-run:{RID}", label], head_ref="feat/x")
    store = aw_run_nudge.plan_nudge_result([pr], [_unlinked_v()], {}, NOWT)
    e = store["o/r#6"]
    assert e["verdict"] == "unlinked_fix_needed" and e["underlying_verdict"] == "unresolvable"
    assert e["handed_off"] is True and e["head_ref"] == "feat/x" and e["blocking_labels"] == [label]
    mgr, _ = aw_run_nudge.manager_prs(store, {"o/r#6"}, NOWT)
    reason = mgr[0]["attention_reason"]
    assert "fresh coder" in reason and "feat/x" in reason
    assert "no new PR" in reason and "no /aw-review" in reason and "no merge" in reason
    assert mgr[0]["aw_run"]["verdict"] == "unlinked_fix_needed"
    assert mgr[0]["aw_run"]["head_ref"] == "feat/x"


def test_unlinked_detected_from_link_resolution_field_too():
    pr = _tagged_pr(0.5, labels=["devteam:awaiting-fixes"])
    v = _unlinked_v(reason="whatever AP-MT words it as", link_resolution="unresolved")
    assert aw_run_nudge.plan_nudge_result([pr], [v], {}, NOWT)["o/r#6"]["verdict"] == "unlinked_fix_needed"


@pytest.mark.parametrize("labels", [["devteam:fix-cycle"],
                                    ["devteam:awaiting-fixes", "devteam:needs-human"],
                                    ["devteam:fix-cycle", "devteam:needs-human"]])
def test_unlinked_history_or_human_hold_is_not_a_coder_dispatch(labels):
    pr = _tagged_pr(0.5, labels=[f"aw-run:{RID}", *labels])
    e = aw_run_nudge.plan_nudge_result([pr], [_unlinked_v()], {}, NOWT)["o/r#6"]
    assert e["verdict"] == "unresolvable"


def test_unresolvable_without_blocking_labels_keeps_the_plain_verdict():
    pr = _tagged_pr(0.5, labels=[f"aw-run:{RID}"])
    e = aw_run_nudge.plan_nudge_result([pr], [_unlinked_v()], {}, NOWT)["o/r#6"]
    assert e["verdict"] == "unresolvable" and e["handed_off"] is True


def test_other_unresolvable_reasons_with_blocking_labels_are_not_reclassified():
    pr = _tagged_pr(0.5, labels=["devteam:awaiting-fixes"])
    v = _unlinked_v(reason="wake dispatch failed: boom")
    assert aw_run_nudge.plan_nudge_result([pr], [v], {}, NOWT)["o/r#6"]["verdict"] == "unresolvable"


def test_live_recheck_carries_head_ref():
    kept, _, _ = _recheck(_view([f"aw-run:{RID}", "devteam:awaiting-fixes"], headRefName="feat/y"))
    assert kept[0]["head_ref"] == "feat/y"


@pytest.mark.parametrize("label", ["devteam:awaiting-fixes"])
def test_waiting_human_with_awaiting_fixes_label_becomes_awaiting_fixes(label):
    """AP-MT answers waiting_human with reason=null for a stale open question;
    with a blocking-review label that is a coder lane, not a human hold."""
    pr = _tagged_pr(3, labels=[label, f"aw-run:{RID}"])
    v = {**_active_v(), "verdict": "waiting_human"}
    store = aw_run_nudge.plan_nudge_result([pr], [v], {}, NOWT)
    e = store["o/r#6"]
    assert e["verdict"] == "awaiting_fixes" and e["handed_off"] is True
    assert label in e["reason"] and RID in e["reason"]
    mgr, _ = aw_run_nudge.manager_prs(store, {"o/r#6"}, NOWT)
    assert mgr[0]["aw_run"]["verdict"] == "awaiting_fixes" and "None" not in mgr[0]["attention_reason"]


@pytest.mark.parametrize("labels,reason", [
    (["devteam:fix-cycle", "devteam:needs-human"], None),   # post-push round-two escalation
    (["devteam:fix-cycle"], None),                          # history label alone
    (["devteam:awaiting-fixes", "devteam:needs-human"], None),
    (["devteam:awaiting-fixes"], "wake cap reached"),       # explicit AP-MT escalation
])
def test_waiting_human_is_kept_for_history_human_hold_or_explicit_reason(labels, reason):
    pr = _tagged_pr(3, labels=[*labels, f"aw-run:{RID}"])
    v = {**_active_v(), "verdict": "waiting_human", "reason": reason}
    e = aw_run_nudge.plan_nudge_result([pr], [v], {}, NOWT)["o/r#6"]
    assert e["verdict"] == "waiting_human" and e["handed_off"] is False
    assert e["reason"] and (reason is None or e["reason"] == reason)


def test_genuine_waiting_human_keeps_verdict_but_gets_a_reason():
    pr = _tagged_pr(3, labels=[f"aw-run:{RID}"])
    store = aw_run_nudge.plan_nudge_result([pr], [{**_active_v(), "verdict": "waiting_human"}], {}, NOWT)
    e = store["o/r#6"]
    assert e["verdict"] == "waiting_human" and e["handed_off"] is False
    assert e["reason"] and "ask_human" in e["reason"]


@pytest.mark.parametrize("verdict", ["awaiting_fixes", "out_of_repo"])
def test_new_ap_mt_verdicts_hand_to_manager_without_the_flag(verdict):
    store = aw_run_nudge.plan_nudge_result([_tagged_pr(3)], [{**_active_v(), "verdict": verdict,
                                                              "reason": "r"}], {}, NOWT)
    assert store["o/r#6"]["handed_off"] is True


# ── idle_open: an open PR nobody is acting on must count as work ──────────

@pytest.mark.parametrize("verdict", ["recent", "waiting_ci", "ask_failed", "ask_deferred"])
def test_idle_tagged_pr_with_no_active_run_is_handed_to_manager(verdict):
    pr = _tagged_pr(0.25)  # 15min untouched, the PR #22 shape
    store = aw_run_nudge.plan_nudge_result([pr], [{**_active_v(), "verdict": verdict}], {}, NOWT)
    e = store["o/r#6"]
    assert e["verdict"] == "idle_open" and e["handed_off"] is True
    assert e["underlying_verdict"] == verdict and "no run acting" in e["reason"]
    prs, _ = aw_run_nudge.manager_prs(store, {"o/r#6"}, NOWT)
    assert prs[0]["attention_reason"].startswith("aw-run: idle_open")


@pytest.mark.parametrize("verdict,pr_kw,age", [
    ("active", {}, 0.25),                      # a live run IS acting
    ("woken", {}, 0.25),                       # a coder was just woken
    ("recent", {}, 0.05),                      # 3min: inside the grace window
    ("recent", {"is_draft": True}, 0.25),      # drafts are not review-ready
    ("waiting_human", {}, 0.25),               # genuine human hold
])
def test_idle_open_does_not_fire_when_something_is_acting(verdict, pr_kw, age):
    pr = _tagged_pr(age, **pr_kw)
    e = aw_run_nudge.plan_nudge_result([pr], [{**_active_v(), "verdict": verdict}], {}, NOWT)["o/r#6"]
    assert e["verdict"] != "idle_open" and e["handed_off"] is False
