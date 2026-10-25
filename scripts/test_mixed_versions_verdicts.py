#!/usr/bin/env python3
"""Tests the mixed-version harness's verdicts (scripts/test_mixed_versions.py)
on made-up results: what a mixed run's penalties, refused PUTs and message
families are measured against, with one all-old run and with several, and
what counts as a new failure reason; what the paged run must show of its
neighbour sync, and that no run pages to a node of the previous release;
and that the report and summary.json list every count measured, with its
excess and limit, within the limit or not. The Test workflow runs it; run it
by hand with `python3 scripts/test_mixed_versions_verdicts.py`. It runs no
node.
"""

import argparse
import importlib.util
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parent / "test_mixed_versions.py"
spec = importlib.util.spec_from_file_location("test_mixed_versions", HARNESS)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)

INCUMBENTS_FAIL = ("incumbent", "incumbent", "application_failure")
JOINERS_TIME_OUT = ("joiner", "joiner", "connection_timeout")


def result(penalties=None, refusals=None, families=None, sync=None, single_sync=True):
    """One run's analysis as verdicts() reads it: `penalties` by (observer,
    target, reason), `refusals` by role, the message families each role
    received, by role, and its neighbour sync as the logs show it; every role
    gets neighbour sync, in single messages unless `single_sync` is false."""
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
        "penalties_on_full": {}, "between_releases": {},
        "penalties": {key: (count, float(count)) for key, count in (penalties or {}).items()},
        "refusals": refusals or {},
        "sync": sync or {"paged": [], "single": [], "builds": {}, "served": {}, "busy": 0},
    }


def exchange(pages=2, peer_release="new"):
    """A paged exchange as the requester's log shows it, each list over
    `pages` pages both ways."""
    found = {"from": "joiner-00", "release": "new", "peer_release": peer_release,
             "end": "Complete", "pages_sent": pages * 2, "pages_received": pages * 2}
    for kind in ("replica", "paid"):
        for way in ("sent", "received"):
            found[f"{kind}_pages_{way}"] = pages
    return found


def paged_sync(exchanges=None, builds=None, served=None):
    """A paged run's neighbour sync: by default, one exchange with each list
    over two pages, its responder's hints built once and its four pages
    served in order."""
    return {"paged": [exchange()] if exchanges is None else exchanges, "single": [],
            "builds": {"joiner-01 a 7": 1} if builds is None else builds,
            "served": {"joiner-01 a 7": [0, 1, 2, 3]} if served is None else served,
            "busy": 0}


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
    expect("the paged run is not measured against the all-old runs",
           {**two, "paged": result({INCUMBENTS_FAIL: 90}, sync=paged_sync())},
           options(), [])
    page_report = harness.report({"paged": result(sync=paged_sync())}, options())
    expect_equal("the report gives the paged and single exchanges by release, and the most "
                 "pages one took",
                 [line for line in page_report.splitlines()
                  if "new -> new" in line or "most pages" in line],
                 ["        new -> new           1     0",
                  "  most pages in an exchange: 4 sent, 4 received; 1 exchange(s) took more "
                  "than one page for each list both ways"])

    listed(options())

    names = ["all-old", "all-old-2", "all-old-12", "all-old-1", "all-old-x", "new-hosts-old"]
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
