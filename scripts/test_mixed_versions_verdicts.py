#!/usr/bin/env python3
"""Tests the mixed-version harness's verdicts (scripts/test_mixed_versions.py)
on made-up results: what a mixed run's penalties, refused PUTs and message
families are measured against, with one all-old run and with several, and
what counts as a new failure reason. The Test workflow runs it; run it by hand
with `python3 scripts/test_mixed_versions_verdicts.py`. It runs no node.
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


def result(penalties=None, refusals=None, families=None):
    """One run's analysis as verdicts() reads it: `penalties` by (observer,
    target, reason), `refusals` by role, and the message families each role
    received, by role; every role gets neighbour sync."""
    traffic, roles = {}, {}
    for role in ("incumbent", "joiner"):
        received = {"neighbor_sync_request": 1, "neighbor_sync_response": 1}
        received.update((families or {}).get(role, {}))
        traffic[f"{role}-00"] = {f"{family}_rx_count": count
                                 for family, count in received.items()}
        roles[f"{role}-00"] = role
    return {
        "stalled_seconds": 0, "decode_failures": [], "audits": {}, "records": [],
        "checks": [], "full_evidence": {}, "traffic": traffic, "roles": roles,
        "releases": {},
        "penalties": {key: (count, float(count)) for key, count in (penalties or {}).items()},
        "refusals": refusals or {},
    }


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
