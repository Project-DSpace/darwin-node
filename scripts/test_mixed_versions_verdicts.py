#!/usr/bin/env python3
"""Tests the mixed-version harness's verdicts (scripts/test_mixed_versions.py)
on made-up results: what a mixed run's penalties, refused PUTs and message
families are measured against, with one all-old run and with several, and
what counts as a new failure reason; what the runs must show of their
neighbour sync: this tree's nodes paging to each other and sending the
previous release's nodes single messages, never pages, no penalty for a
busy answer, and what the paged and rollback runs must show besides, with
the releases the rolled-back node ran; and that the report and summary.json
list every count measured, with its excess and limit, within the limit or
not, and the audits made per exchange. The Test workflow runs it; run it by
hand with `python3 scripts/test_mixed_versions_verdicts.py`. It runs no node.
"""

import argparse
import datetime
import importlib.util
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parent / "test_mixed_versions.py"
spec = importlib.util.spec_from_file_location("test_mixed_versions", HARNESS)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)

INCUMBENTS_FAIL = ("incumbent", "incumbent", "application_failure")
JOINERS_TIME_OUT = ("joiner", "joiner", "connection_timeout")


def result(penalties=None, refusals=None, families=None, sync=None, single_sync=True,
           paging=None, rollback=None):
    """One run's analysis as verdicts() reads it: `penalties` by (observer,
    target, reason), `refusals` by role, the message families each role
    received, by role, its neighbour sync as the logs show it, what its nodes
    were given to page, and its rollback, if any; every role gets neighbour
    sync, in single messages unless `single_sync` is false."""
    traffic, roles = {}, {}
    for role in ("incumbent", "joiner"):
        received = ({"neighbor_sync_request": 1, "neighbor_sync_response": 1}
                    if single_sync else {})
        received.update((families or {}).get(role, {}))
        traffic[f"{role}-00"] = {f"{family}_rx_count": count
                                 for family, count in received.items()}
        roles[f"{role}-00"] = role
    return {
        "stalled_seconds": 0, "decode_failures": [], "audits": {}, "records": [],
        "checks": [], "full_evidence": {}, "traffic": traffic, "roles": roles,
        "releases": {}, "ran": {}, "unanswered": {}, "penalties_on_refusals": {},
        "penalties_on_full": {}, "between_releases": {}, "penalties_on_restart": {},
        "penalties": {key: (count, float(count)) for key, count in (penalties or {}).items()},
        "refusals": refusals or {},
        "sync": sync or {"paged": [], "single": [], "builds": {}, "served": {}, "busy": 0},
        "paging": paging or {}, "rollback": rollback,
    }


T0 = datetime.datetime(2026, 10, 25, 12, 0, tzinfo=datetime.timezone.utc)
ROLLED = "ab" * 32


def minutes(count):
    return T0 + datetime.timedelta(minutes=count)


def exchange(pages=2, peer_release="new", form="paged", peer=None, start=None,
             pages_answered=None, end="Complete", source="joiner-00"):
    """An exchange as the requester's log shows it: paged, each list over
    `pages` pages both ways, unless `form` is "single"."""
    found = {"from": source, "release": "new", "peer_release": peer_release, "form": form,
             "end": end, "start": start or minutes(1), "at": start or minutes(1)}
    if peer is not None:
        found["peer"] = peer
    if form == "paged":
        found.update({"pages_sent": pages * 2, "pages_received": pages * 2,
                      "pages_answered": pages * 2 if pages_answered is None else pages_answered})
        for kind in ("replica", "paid"):
            for way in ("sent", "received"):
                found[f"{kind}_pages_{way}"] = pages
    return found


def paged_sync(exchanges=None, builds=None, served=None, single=None, busy=0,
               busy_charged=None):
    """A run's neighbour sync: by default, one paged exchange with each list
    over two pages, its responder's hints built once and its four pages
    served in order."""
    return {"paged": [exchange()] if exchanges is None else exchanges,
            "single": single or [],
            "builds": {"joiner-01 a 7": 1} if builds is None else builds,
            "served": {"joiner-01 a 7": [0, 1, 2, 3]} if served is None else served,
            "busy": busy, "busy_charged": busy_charged or []}


# What a mixed run's nodes are given to page: this tree's nodes page to each
# other, beside nodes of a previous release that reads no pages.
MIXED = {"new_pages": True, "old_present": True, "old_reads_pages": False}

# The rollback run: its node ran the previous release from minute 10 to 30.
ROLLBACK = {"node": "joiner-00", "peer_id": ROLLED, "old_from": minutes(10),
            "old_until": minutes(30),
            "restarts": [[minutes(10), minutes(10.5)], [minutes(30), minutes(30.5)]]}


def rolled(form="paged", peer_release="new", at=35, pages_answered=None, source="joiner-01"):
    """An exchange of this tree's `source` with the rolled-back node that
    started at minute `at`."""
    return exchange(form=form, peer=ROLLED, peer_release=peer_release, start=minutes(at),
                    pages_answered=pages_answered, source=source,
                    end="Complete" if pages_answered != 0 else "Failed")


def rollback_sync(extra_paged=(), single=None):
    """The rollback run's neighbour sync: this tree's nodes paging to each
    other, a single message to the rolled-back node while it ran the previous
    release, and a page to it once it came back, unless told otherwise."""
    return paged_sync(
        exchanges=[exchange(), rolled(at=35)] + list(extra_paged),
        single=[rolled(form="single", peer_release="old", at=15)] if single is None else single)


def options(**overrides):
    """The harness's options, at their defaults unless given."""
    values = {"penalty_slack": harness.PENALTY_SLACK, "penalty_ratio": harness.PENALTY_RATIO,
              "new_reason_allowance": harness.NEW_REASON_ALLOWANCE, "new_only_family": []}
    values.update(overrides)
    return argparse.Namespace(**values)


FAILED = []


def expect(name, results, args, failing):
    """verdicts() must fail exactly once for each text in `failing`, each
    failure containing its text, and pass when `failing` is empty."""
    failures = harness.verdicts(results, args)
    if len(failures) != len(failing) or not all(
            any(text in failure for failure in failures) for text in failing):
        FAILED.append(name)
        print(f"FAILED: {name}: {failures}")
    else:
        print(f"ok: {name}")


def expect_equal(name, found, wanted):
    if found != wanted:
        FAILED.append(name)
        print(f"FAILED: {name}:")
        print(f"  found  {found}")
        print(f"  wanted {wanted}")
    else:
        print(f"ok: {name}")


def measured(kind, target, reason, count, all_old, limit, observer=None):
    """One entry of comparisons(), as it should be."""
    return {"kind": kind, "observer": observer, "target": target, "reason": reason,
            "count": count, "all_old": all_old, "highest": max(all_old),
            "excess": count - max(all_old), "limit": limit}


def compared_lines(text):
    """The report's lines of counts measured against the all-old runs, by
    the run they are in, each split into words."""
    runs, run, listing = {}, None, False
    for line in text.splitlines():
        if line.startswith("== "):
            run, listing = line.strip("= "), False
        elif line.startswith("measured against the all-old runs"):
            listing = True
            runs[run] = []
        elif listing and line.startswith("  "):
            runs[run].append(line.split())
        else:
            listing = False
    return runs


def listed(args):
    """What the report and summary.json say of the counts each run measured
    against the all-old runs, with two of them, every kind of count present,
    one within the slack and one over it."""
    two = {"all-old": result({INCUMBENTS_FAIL: 2, JOINERS_TIME_OUT: 8}, {"incumbent": 4}),
           "all-old-2": result({INCUMBENTS_FAIL: 9, JOINERS_TIME_OUT: 6}, {"incumbent": 1})}
    allowance = harness.NEW_REASON_ALLOWANCE
    slack = harness.PENALTY_SLACK
    results = {**two,
               "new-hosts-old": result(
                   {INCUMBENTS_FAIL: 12, JOINERS_TIME_OUT: 3,
                    ("joiner", "joiner", "dht_identity_exchange_failed"): allowance - 1,
                    ("incumbent", "joiner", "dht_identity_exchange_failed"): 1,
                    ("joiner", "devnet", "application_failure"): 400},
                   {"incumbent": 5}),
               "old-hosts-new": result({INCUMBENTS_FAIL: 10 + slack}),
               "full": result({INCUMBENTS_FAIL: 90})}
    baselines = [results["all-old"], results["all-old-2"]]
    expect_equal("every count measured is listed, within the limit or not, with the highest "
                 "all-old count, the excess and the limit; penalties on the devnet are not",
                 harness.comparisons(results["new-hosts-old"], baselines, args),
                 [measured("penalties", "incumbent", "application_failure", 12, [2, 9],
                           9 + slack, observer="incumbent"),
                  measured("penalties", "joiner", "connection_timeout", 3, [8, 6], 8 + slack,
                           observer="joiner"),
                  measured("new reason", "joiner", "dht_identity_exchange_failed", allowance,
                           [0, 0], allowance),
                  measured("refused PUTs", "incumbent", None, 5, [4, 1], 4 + slack),
                  measured("refused PUTs", "joiner", None, 0, [0, 0], slack)])

    text = harness.report(results, args)
    lines = compared_lines(text)
    expect_equal("the report lists the counts of the mixed runs only",
                 sorted(lines), ["new-hosts-old", "old-hosts-new"])
    expect_equal("the report lists each count, the highest all-old count, the excess and the "
                 "limit, a count under the highest with a negative excess",
                 [line[-4:] for line in lines.get("new-hosts-old", [])],
                 [["12", "9", "+3", str(9 + slack)], ["3", "8", "-5", str(8 + slack)],
                  [str(allowance), "0", f"+{allowance}", str(allowance)],
                  ["5", "4", "+1", str(4 + slack)], ["0", "0", "+0", str(slack)]])
    expect_equal("the report marks a count over its limit",
                 [line[-5:] for line in lines.get("old-hosts-new", [])][:1],
                 [[str(10 + slack), "9", f"+{1 + slack}", str(9 + slack), "OVER"]])
    expect_equal("the report names a new reason as new, counted over every observer",
                 [line[:4] for line in lines.get("new-hosts-old", []) if "(new)" in line],
                 [["any", "->", "joiner", "dht_identity_exchange_failed"]])

    summary = harness.summary_json(results, harness.verdicts(results, args), {}, args)
    expect_equal("summary.json records the same counts for each mixed run, and none for the "
                 "all-old runs and the full run",
                 {name: run.get("comparisons") for name, run in summary["runs"].items()},
                 {"all-old": None, "all-old-2": None, "full": None,
                  "new-hosts-old": harness.comparisons(results["new-hosts-old"], baselines, args),
                  "old-hosts-new": harness.comparisons(results["old-hosts-new"], baselines, args)})


def main():
    one = {"all-old": result({INCUMBENTS_FAIL: 5, JOINERS_TIME_OUT: 8})}
    expect("against one all-old run, a count PENALTY_SLACK above it passes",
           {**one, "new-hosts-old": result({INCUMBENTS_FAIL: 5 + harness.PENALTY_SLACK})},
           options(), [])
    expect("against one all-old run, a count more than PENALTY_SLACK above it fails",
           {**one, "new-hosts-old": result({INCUMBENTS_FAIL: 6 + harness.PENALTY_SLACK})},
           options(),
           ["application_failure penalties by incumbents on incumbents, against 5 in the "
            "all-old run"])
    expect("a doubling of a large count fails, with no ratio set",
           {"all-old": result({INCUMBENTS_FAIL: 40}),
            "new-hosts-old": result({INCUMBENTS_FAIL: 79})},
           options(), ["79 application_failure penalties"])
    expect("against one all-old run, a ratio set allows its share when that is more",
           {"all-old": result({INCUMBENTS_FAIL: 40}),
            "new-hosts-old": result({INCUMBENTS_FAIL: 79})},
           options(penalty_ratio=1.0), [])

    two = {"all-old": result({INCUMBENTS_FAIL: 2, JOINERS_TIME_OUT: 8}),
           "all-old-2": result({INCUMBENTS_FAIL: 9, JOINERS_TIME_OUT: 6})}
    expect("against two all-old runs, the highest count plus PENALTY_SLACK passes",
           {**two, "old-hosts-new": result({INCUMBENTS_FAIL: 9 + harness.PENALTY_SLACK})},
           options(), [])
    expect("against two all-old runs, more than the highest count plus PENALTY_SLACK fails",
           {**two, "old-hosts-new": result({INCUMBENTS_FAIL: 10 + harness.PENALTY_SLACK})},
           options(), ["against at most 9 in the 2 all-old runs (2, 9)"])
    expect("a pair of roles' zero is not a new reason when another pair charged the role "
           "with it",
           {**two, "old-hosts-new": result({("joiner", "incumbent", "application_failure"): 9})},
           options(), [])
    allowance = harness.NEW_REASON_ALLOWANCE
    expect("a new failure reason up to the allowance passes",
           {**two, "old-hosts-new": result(
               {("joiner", "joiner", "dht_identity_exchange_failed"): allowance})},
           options(), [])
    expect("a new failure reason above the allowance fails, named as new, counted over every "
           "observer",
           {**two, "old-hosts-new": result(
               {("joiner", "joiner", "dht_identity_exchange_failed"): allowance,
                ("incumbent", "joiner", "dht_identity_exchange_failed"): 1})},
           options(),
           [f"{allowance + 1} dht_identity_exchange_failed penalties on joiners, a new failure "
            "reason"])
    expect("application failures on joiners, which no all-old run charged them with, are new",
           {**two, "new-hosts-old": result(
               {("incumbent", "joiner", "application_failure"): allowance + 1})},
           options(), ["application_failure penalties on joiners, a new failure reason"])
    expect("penalties on the devnet's nodes are reported, not compared",
           {**two, "new-hosts-old": result({("joiner", "devnet", "application_failure"): 400})},
           options(), [])
    expect("refused PUTs are measured against the highest all-old count",
           {"all-old": result(refusals={"incumbent": 4}),
            "all-old-2": result(refusals={"incumbent": 1}),
            "new-hosts-old": result(refusals={"incumbent": 5 + harness.PENALTY_SLACK})},
           options(), [f"the incumbents refused {5 + harness.PENALTY_SLACK} paid PUTs, against "
                       "at most 4 in the 2 all-old runs (4, 1)"])
    expect("a family every all-old run received SHAPE_FLOOR times or more must arrive",
           {"all-old": result(families={"incumbent": {"fetch_request": harness.SHAPE_FLOOR}}),
            "all-old-2": result(families={"incumbent": {"fetch_request": 25}}),
            "new-hosts-old": result()},
           options(), ["the incumbents received no fetch_request"])
    expect("a family one all-old run received fewer times may be missing",
           {"all-old": result(families={"incumbent": {"fetch_request": harness.SHAPE_FLOOR - 1}}),
            "all-old-2": result(families={"incumbent": {"fetch_request": 25}}),
            "new-hosts-old": result()},
           options(), [])
    expect("the all-old runs and the full run are not measured against the all-old runs",
           {**two, "all-old-3": result({INCUMBENTS_FAIL: 90}),
            "full": result({INCUMBENTS_FAIL: 90})},
           options(), [])

    # -- what every run's neighbour sync must show --
    old_single = exchange(form="single", peer_release="old")
    expect("a mixed run in which this tree's nodes page to each other and send the previous "
           "release's single messages passes",
           {**two, "new-hosts-old": result(sync=paged_sync(single=[old_single]), paging=MIXED)},
           options(), [])
    expect("a mixed run in which this tree's nodes never paged to each other fails",
           {**two, "new-hosts-old": result(sync=paged_sync(exchanges=[], single=[old_single]),
                                           paging=MIXED)},
           options(), ["no node of this tree paged its hints to another"])
    expect("a mixed run with no single message to the previous release's nodes fails",
           {**two, "old-hosts-new": result(sync=paged_sync(), paging=MIXED)},
           options(), ["sent its hints to a node of the previous release in a single message"])
    reads = {**MIXED, "old_reads_pages": True}
    expect("a previous release that reads pages is paged to",
           {**two, "new-hosts-old": result(
               sync=paged_sync(exchanges=[exchange(), exchange(peer_release="old")]),
               paging=reads)},
           options(), [])
    expect("a previous release that reads pages, never paged to, fails",
           {**two, "new-hosts-old": result(sync=paged_sync(single=[old_single]), paging=reads)},
           options(), ["paged its hints to a node of the previous release, which reads pages"])
    expect("a requester that charged its responder during an exchange answered busy fails",
           {**two, "new-hosts-old": result(sync=paged_sync(
               single=[old_single], busy_charged=[{"from": "joiner-00", "peer": "cd" * 32,
                                                   "at": minutes(2),
                                                   "reason": "connection_timeout"}]),
               paging=MIXED)},
           options(), ["charged cdcdcdcdcdcdcdcd (connection_timeout) during an exchange it "
                       "answered busy"])

    # -- the rollback run --
    expect("a rollback run with a single message to its node while it ran the previous "
           "release, and pages to it after, passes",
           {**two, "rollback": result(sync=rollback_sync(), paging=MIXED, rollback=ROLLBACK)},
           options(), [])
    expect("a page to the rolled-back node left unanswered over its earlier connection, once "
           "from a node, passes",
           {**two, "rollback": result(
               sync=rollback_sync([rolled(peer_release="old", at=10.2, pages_answered=0)]),
               paging=MIXED, rollback=ROLLBACK)},
           options(), [])
    expect("two pages from one node to the rolled-back node while it ran the previous release "
           "fail",
           {**two, "rollback": result(
               sync=rollback_sync([rolled(peer_release="old", at=10.2, pages_answered=0),
                                   rolled(peer_release="old", at=25, pages_answered=0)]),
               paging=MIXED, rollback=ROLLBACK)},
           options(), ["joiner-01 paged 2 times to the rolled-back node"])
    expect("a page the rolled-back node answered while it ran the previous release fails",
           {**two, "rollback": result(
               sync=rollback_sync([rolled(peer_release="old", at=20, pages_answered=1)]),
               paging=MIXED, rollback=ROLLBACK)},
           options(), ["paged its hints to a node of the previous release, which can't read "
                       "pages"])
    expect("a rollback run with no single message to its node while it ran the previous "
           "release fails",
           {**two, "rollback": result(sync=rollback_sync(single=[]), paging=MIXED,
                                      rollback=ROLLBACK)},
           options(), ["no node of this tree sent the rolled-back node a single message"])
    expect("a rollback run with no page to its node once it came back fails",
           {**two, "rollback": result(
               sync=paged_sync(single=[rolled(form="single", peer_release="old", at=15)]),
               paging=MIXED, rollback=ROLLBACK)},
           options(), ["no node of this tree paged to the rolled-back node once it came back"])
    expect("the rollback run is measured against the all-old runs",
           {**two, "rollback": result({INCUMBENTS_FAIL: 10 + harness.PENALTY_SLACK},
                                      sync=rollback_sync(), paging=MIXED, rollback=ROLLBACK)},
           options(), ["application_failure penalties by incumbents on incumbents"])
    seconds = datetime.timedelta(seconds=1)
    expect_equal("the rolled-back node ran the previous release from its stop until it came "
                 "back",
                 [harness.release_of(ROLLBACK, {ROLLED: "new"}, ROLLED, when) for when in
                  (minutes(10) - seconds, minutes(10), minutes(30) - seconds, minutes(30))]
                 + [harness.release_of(ROLLBACK, {"ef": "new"}, "ef", minutes(15))],
                 ["new", "old", "old", "new", "new"])
    grace = datetime.timedelta(seconds=harness.RESTART_GRACE)
    expect_equal("penalties on the rolled-back node from its stop until it has been up for "
                 "the grace are set apart",
                 [harness.restarting(ROLLBACK, ROLLED, when) for when in
                  (minutes(10) - seconds, minutes(10), minutes(10.5) + grace,
                   minutes(10.5) + grace + seconds, minutes(30.2))]
                 + [harness.restarting(ROLLBACK, "ef", minutes(10.2)),
                    harness.restarting(None, ROLLED, minutes(10.2))],
                 [False, True, True, False, True, False, False])
    busy = exchange(end="Busy", peer="cd" * 32, start=minutes(2))
    busy["at"] = minutes(2) + datetime.timedelta(seconds=3)
    within = minutes(2) + datetime.timedelta(seconds=2)
    late = busy["at"] + datetime.timedelta(seconds=harness.BUSY_CHARGE_GRACE + 1)
    expect_equal("a penalty counts as charged for a busy answer when its requester charged its "
                 "responder during the exchange",
                 harness.busy_charges({"paged": [busy, exchange(peer="cd" * 32)]},
                                      [("joiner-00", "cd" * 32, within, "a"),
                                       ("joiner-00", "cd" * 32, late, "b"),
                                       ("joiner-01", "cd" * 32, within, "c"),
                                       ("joiner-00", "ef" * 32, within, "d")]),
                 [{"from": "joiner-00", "peer": "cd" * 32, "at": within, "reason": "a"}])
    line = ("2026-10-25T12:00:10.500000Z DEBUG ant_node::replication::neighbor_sync: "
            "Neighbor-sync paged exchange peer=" + "cd" * 32 + " cycle=7 end=Busy "
            "pages_answered=0 elapsed_ms=1500")
    found = harness.exchange_in(line)
    expect_equal("an exchange's log line gives its form, its peer, how it ended and when it "
                 "started",
                 (found["form"], found["peer"], found["end"], found["pages_answered"],
                  found["start"]),
                 ("paged", "cd" * 32, "Busy", 0,
                  T0 + datetime.timedelta(seconds=9)))

    expect("a paged run with lists over more than one page, hints built once and pages in "
           "order passes",
           {"paged": result(sync=paged_sync())}, options(), [])
    expect("a paged run whose nodes read only pages still received neighbour sync",
           {"paged": result(sync=paged_sync(), single_sync=False, families={
               role: {"neighbor_sync_page_request": 3, "neighbor_sync_page_response": 3}
               for role in ("incumbent", "joiner")})},
           options(), [])
    expect("a paged run in which no exchange took more than one page for each list fails",
           {"paged": result(sync=paged_sync(exchanges=[exchange(pages=1)]))},
           options(), ["no exchange took more than one page for each list"])
    expect("a responder that built its hints twice in one exchange fails the paged run",
           {"paged": result(sync=paged_sync(builds={"joiner-01 a 7": 2}))},
           options(), ["built its hints 2 times in one exchange",
                       "no responder served more than one page"])
    expect("pages served out of order fail the paged run",
           {"paged": result(sync=paged_sync(served={"joiner-01 a 7": [0, 2, 1]}))},
           options(), ["pages served out of order", "no responder served more than one page"])
    expect("a paged run in which no responder served more than one page from one build fails",
           {"paged": result(sync=paged_sync(served={"joiner-01 a 7": [0]}))},
           options(), ["no responder served more than one page"])
    expect("a node that paged its hints to a node of the previous release fails the run",
           {**two, "new-hosts-old": result(sync=paged_sync(
               exchanges=[exchange(peer_release="old")]))},
           options(), ["paged its hints to a node of the previous release"])
    expect("pages between nodes of this tree, in a mixed run, pass",
           {**two, "new-hosts-old": result(sync=paged_sync())}, options(), [])
    expect("the paged run is measured for new failure reasons only",
           {**two, "paged": result({INCUMBENTS_FAIL: 90}, sync=paged_sync())},
           options(), [])
    expect("a new failure reason fails the paged run",
           {**two, "paged": result(
               {("joiner", "joiner", "dht_identity_exchange_failed"): allowance + 1},
               sync=paged_sync())},
           options(), ["dht_identity_exchange_failed penalties on joiners, a new failure "
                       "reason"])
    expect("a paged run whose incumbents admit one request at once, with no page answered "
           "busy, fails",
           {"paged": result(sync=paged_sync(), paging={"busy_limit": 1})},
           options(), ["no page was answered busy"])
    expect("a paged run with pages answered busy, none charged for, passes",
           {"paged": result(sync=paged_sync(busy=3), paging={"busy_limit": 1})},
           options(), [])
    page_report = harness.report({"paged": result(sync=paged_sync())}, options())
    expect_equal("the report gives the paged and single exchanges by release, and the most "
                 "pages one took",
                 [line for line in page_report.splitlines()
                  if "new -> new" in line or "most pages" in line],
                 ["        new -> new           1     0",
                  "  most pages in an exchange: 4 sent, 4 received; 1 exchange(s) took more "
                  "than one page for each list both ways"])

    audited = result(sync=paged_sync(single=[exchange(form="single", peer_release="old")]))
    audited["audits"] = {"joiner-00": {"responsible_audit_passed": 3, "subtree_audit_failed": 2,
                                       "digest_received": 9},
                         "incumbent-00": {"responsible_audit_passed": 2}}
    expect_equal("the report gives the audits made per exchange, paged and single",
                 [line for line in harness.sync_lines(audited) if "audits made" in line],
                 ["  audits made: 7 over 2 exchange(s), per exchange: 3.500"])
    expect_equal("the report says when there were no exchanges to give audits per",
                 [line for line in harness.sync_lines(result(sync=paged_sync(exchanges=[])))
                  if "audits made" in line],
                 ["  audits made: 0 over 0 exchange(s), per exchange: no exchanges"])

    listed(options())

    names =["all-old", "all-old-2", "all-old-12", "all-old-1", "all-old-x", "new-hosts-old"]
    baselines = [name for name in names if harness.is_baseline(name)]
    if baselines != ["all-old", "all-old-2", "all-old-12"]:
        FAILED.append("all-old runs named")
        print(f"FAILED: all-old runs named: {baselines}")
    else:
        print("ok: all-old runs named")

    if FAILED:
        print(f"{len(FAILED)} case(s) failed", file=sys.stderr)
        return 1
    print("scripts/test_mixed_versions.py: every verdict case passes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
