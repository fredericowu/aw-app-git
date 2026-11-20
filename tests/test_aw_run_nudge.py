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
    _warm(app, prs=[_att(1)], tagged=[_tagged(7)], attention=[_att(7)])
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
