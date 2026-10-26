# ADR-0025: Records priced by size class

- **Status:** Proposed
- **Date:** 2026-10-26
- **Decision owners:** darwin-node maintainers
- **Reviewers:** <pending>
- **Supersedes:** none
- **Superseded by:** none
- **Related:** amends ADR-0004 (commitment-bound quote pricing: the exact price check
  becomes a band), ADR-0006 (the revenue floor's reference), ADR-0008 (the record as the
  unit of sale) and ADR-0016 (a pointer's price); meets a re-open trigger of ADR-0013
  (settlement versions); builds on the replication design's paid list
  (`docs/REPLICATION_DESIGN.md`)

## Context

ADR-0008 made the record the unit of sale: one chunk of up to 4 MiB, priced the same
whatever it carries. A 1 KiB chunk and a 4 MiB chunk pay the same, and a file of a few
kilobytes pays for several full chunks. A node's price comes from its committed key count K
(ADR-0004), so a node full of small records also prices as if every record were full.

The fixed part of a record's cost (routing, replication messages, audits, the paid-list
entry) doesn't depend on its size; the rest does. This release prices a record by both
parts, without a new curve, a new quote format or a new unit, and without refusing any
payment a client released before it makes.

## Decision Drivers

- No client ever pays more than today, for any node or size.
- A client that never updates keeps working: a full-price payment always clears.
- The quote format, the curve `calculate_price` with its ×32 multiplier, and every exact
  check of a quote's arithmetic stay as they are.
- The chain shows a record's size only to within a factor of two.
- A store check that doesn't know what it holds must fail to compile, not pass.

## Considered Options

1. Price by the exact byte count. Rejected: every payment is public, so a linear price
   shows each chunk's size, and with it roughly how big a private file is.
2. A new quote field carrying the size, checked exactly. Rejected: it changes the signed
   quote and every client's validation at once, and a wrong declared size would make an
   honest quote fail an exact check.
3. **Weight the amount paid, by size class, and quote at a weight total — chosen.** The
   size enters only checks that already accept "at least" an amount.

## Decision

### The weight of a record

`record_weight(data_type, len)` (vendored ant-protocol, `payment/pricing.rs`) gives the
weight of the top of the record's size class, as an exact fraction of a full chunk:

```text
w = φ + (1 − φ) × class top / 4 MiB,  φ = 1/4
```

There are nine classes, with power-of-two tops from 16 KiB to 4 MiB. In 1/1,024 of a full
chunk their weights are 259, 262, 268, 280, 304, 352, 448, 640 and 1,024. A full chunk
weighs exactly 1 and pays exactly what it paid before. A pointer weighs as its data type
at its fixed 5,303 bytes, whatever size a request declares: the 16 KiB class, 259/1,024.

### What a client pays, and what a storer requires

A client settling under version 2 pays `weighted_amount` = 3 × the median quote × w. Every
store check requires at least that, with the class computed from the bytes the storer
holds: `verify_payment` takes the held length as a required argument (`Held::Bytes(len)`),
so a store path that leaves it out does not compile. A payment for a larger class is
accepted, one for a smaller class is refused, and an overpayment always clears. The paid
amount is read through the payment facts of the proof's receipts and the vault record, as
before; a fact below the weighted amount is a miss that goes to the record read, never
"zero paid".

### Quotes: the weight total, and the band (amends ADR-0004)

A node's weight total m is ceil(Σ w) over its committed records, computed when a
commitment is built, and once for a commitment reloaded at start, from the records' stored
lengths. A length that can't be read counts as a full chunk. m is held beside the
commitment and never saved: the retention file and `RETENTION_FORMAT_VERSION` are
unchanged, so releases before and after this one read each other's files. An unchanged
root keeps its m, so an hourly tick that changes nothing reads no lengths. The tick line
logs `weight_total=` (a number, or `unknown` until it is read).

A quote request declaring settlement version 2 or later is quoted at
`calculate_price(min(m, K))`, or at `calculate_price(K)` while m is unknown. Every other
request — unversioned, or version 1 — is quoted at `calculate_price(K)`, today's price. The
quote's count field still carries K.

ADR-0004's check "price equals the formula applied to that count" becomes a band:

```text
calculate_price(floor(259 × K / 1024)) ≤ price ≤ calculate_price(K)
```

It is still two forward computations, never an inversion of the price. At K = 0 the band
is the baseline price alone. The shape and key-cap rules, and the cross-check of K against
the pinned commitment, are unchanged. The band is the one check behind the single-node and
merkle quote checks and the off-curve telemetry (`price_in_band`, `binding_violation`).

The bound is not a proof: any count in the band is accepted. A node that overstates m sits
above the median and gains little; where most of a close group overstates, a record pays
today's key-count price for its size, never more.

### Browser quotes

The browser listener accepts its protocol's current version N for every request, and N+1
for `quote_chunk` alone. An N+1 quote is asked as settlement version 2, at the weight
total; a plain N quote is asked unversioned, at today's price. `hello` lists
`size_priced_quote`. A node before this release refuses an N+1 frame in its transport's
parser, before any quote: `invalid_request` when it is the channel's first request, a
channel error otherwise.

### Payment notices and the paid class (amends the replication design)

A payment notice carries no bytes, so it is admitted at the smallest class's amount. The
class its settled amount covers is written as the paid-list entry's value: one byte, the
base-2 logarithm of the class top (14 for 16 KiB up to 22 for 4 MiB), in the same
transaction as the key. A value is only ever raised. An empty value reads as the full
class, which covers every key admitted before this release, every key admitted with its
bytes or learned through a presence quorum of holders, and every key admitted under an
older release. Older releases never read the value: membership, counts and key listings
ignore it, and pruning a key removes its value with it. A notice is dropped as already
known only when the full class is recorded, so a later notice can raise a smaller one.

A node answers a paid-list query with the class its entry records:
`VerificationResponse` gains `paid_classes`, one byte per result, appended at the very end
of the reply. An older node's decoder stops before it; a reply from an older node ends
before it and reads as recording the full class for every key, as its entries do. A node
that learns a key from a paid-list quorum alone records the class a quorum of those entries
record or exceed (the `ConfirmNeeded`-th largest), not the full class: a majority of paid
lists is not storage, so a payment noticed at a smaller class doesn't become a full one by
spreading.

When a fetched chunk arrives, the node re-reads the recorded class. Only a presence quorum
of holders, majority storage, accepts the bytes whatever the paid list records. Where the
fetch's authorisation is a paid-list entry (this node's own, or one written from a
paid-list quorum's classes) and the recorded class is below the class of the bytes, that
fetch ends: no other source is tried, no peer is charged, and the entry and any held copy
stay.

### The revenue floor's reference (amends ADR-0006)

The floor's reference is priced at the bottom of the median count's band, and weighted by
the record's class:

```text
settled_amount >= 3 × tolerance% × calculate_price(floor(259 × median K / 1024)) × w(len)
```

It still runs in shadow mode by default. An operator who enforces it
(`ANT_PRICE_FLOOR_ENFORCE`) never refuses a correct weighted payment, and a full-price
payment clears it as before.

### The unit of sale (amends ADR-0008)

The record is still the unit, and a full chunk's price is unchanged. A smaller record pays
its class's share of it.

### A pointer's price (amends ADR-0016)

Each pointer state is paid at the pointer's weight, `record_weight(DATA_TYPE_POINTER,
POINTER_WIRE_LEN)`, whatever size the request declares. Pay to create, pay to update and
the payment's `state_id` are unchanged.

### Settlement version 2 (ADR-0013)

`CURRENT_SETTLEMENT_VERSION` is 2 and `MIN_SUPPORTED_SETTLEMENT_VERSION` stays 1. A
version-1 or unversioned client is quoted today's price and its full payment always clears,
so nothing is refused that was accepted before. A node before this release answers a
version-2 quote with `StorerUpdateRequired`, and the client asks another node.

This meets ADR-0013's re-open trigger "any settlement change that is not a monotonic
increase": version 2 lowers what a client pays, so the upper bound is load-bearing for the
first time. That is what the bound is for, and it holds: an older node refuses before
quoting. Raising the minimum stays a separate decision.

An older node can't verify a version-2 payment in any other message either: its exact
check needs `calculate_price(K)` where the quotes are priced at m, and 3 × the median
where the client paid 3 × the median × w, at every size, a full chunk included. So it
refuses the fresh offers and drops the notices of a record paid by size, and where most
of its paid close group is older it never learns the key at all. The possession check
after the write and the audits then charge it for a record it doesn't hold, as they
charge any peer: the penalty for an unheld close-group chunk is never withheld. So
clients pay by size only once the node census shows no storer before this release; until
then they settle at version 1, which every release verifies. Nothing spares a peer by the
release it advertises: a release is the peer's own claim, so such an exemption would let
any node escape the penalty for every record paid by size.

The rx line of the chunk RPC traffic summary, every 300 s, splits versioned quote requests
by declared version: `quote_v2_settlement_{1,2,other}_count` and the same for merkle,
counted at the settlement gate, browser N+1 quotes included.

## Consequences

### Positive

- Small records pay less, down to 259/1,024 of a full chunk, and a full chunk pays the
  same as before.
- Nothing a released client does changes: it is quoted and paid as before, on every
  release.
- The chain shows a record's size only to within a factor of two.

### Negative / Trade-offs

- A node's weight total is bounded, not proven: any count in the band is accepted.
- A key recorded below the full class is verified again by later notices of it, up to the
  rest of its paid close group; the payment facts spare the chain read, not the signature
  and closeness checks.
- With an enforced floor, neighbourhoods of mostly small records get a looser floor than a
  reference at the median count would give, most of all at high fill, because the
  reference moves to the band's bottom.
- Until clients pay by size, nodes before this release refuse their quotes; the release
  goes to the network before clients switch.
- The switch to paying by size waits for the last storer before this release, which
  would otherwise be penalised for every record paid by size it can't verify. A storer
  that never updates holds the switch back.
- A paid-list reply carries one more byte per key.

## Tests

- The weight at every class, the band at both ends and K = 0, an honest m always in the
  band, a pointer at every declared size (`tests/record_weight.rs`, `tests/fork_pins.rs`).
- The weighted requirement at every class in every store context, read through the
  payment read path, a miss never taken as zero paid (`tests/fork_pins.rs`,
  `payment::verifier`).
- Quotes at m only for version 2, today's price otherwise, natively and from the browser
  (`storage::handler`, `web_rtc`); a reloaded commitment weighed once and never re-read;
  the retention file the same with or without m (`replication::commitment_state`,
  `replication::commitment_tick_tests`).
- The paid class raised and never lowered, an empty value read as full, the re-check only
  where the own entry is the sole authorisation (`replication::paid_list`,
  `paid_notify_tests`, `replication::tests`).
- A paid-list quorum's class: the class a quorum of entries record, an older node's
  reply read as full, and a key known only through smallest-class entries refused past
  16 KiB while one whose entries hold the empty value is repaired at any length
  (`replication::quorum`, `replication::tests`); the appended classes read by ds.15's
  decoder and ds.15's reply read without them (`tests/wire_format.rs`).
- No penalty lane reads the release a peer advertises
  (`replication::retention::census`).
- The floor at the band's bottom never refusing a correct weighted payment
  (`payment::verifier`).
- A live network storing a record paid at today's price at three sizes
  (`tests/e2e/settlement_version_gate.rs`).
- A census of every production mention of `calculate_price` and `committed_key_count`
  (`replication::retention::census`).
- The mixed-version harness (`scripts/test_mixed_versions.py`) against the previous
  release, its client declaring settlement version 1, and in every run with this tree's
  nodes also settlement version 2 at the weighted amount (`--settlement-version 2`, the
  default): older nodes answer those quotes `StorerUpdateRequired`, the records are stored
  and read back, and the penalties older nodes then take for them are reported apart
  (`scripts/test_mixed_versions_verdicts.py`).
