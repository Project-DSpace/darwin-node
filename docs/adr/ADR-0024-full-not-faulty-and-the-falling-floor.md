# ADR-0024: Full, not faulty, and the falling erase floor

- **Status:** Proposed
- **Date:** 2026-10-25
- **Decision owners:** darwin-node maintainers
- **Reviewers:** <pending>
- **Supersedes:** none
- **Superseded by:** none
- **Related:** amends ADR-0003 (full-node detection, penalisation and eviction) and
  ADR-0022 (the unheld-chunk penalty restored on every lane); builds on ADR-0005
  Amendment 1 and ADR-0003 Amendment 1 (records outside the retention width kept while the
  disk has room, erased under disk pressure beyond ten times the distance to the 7th-nearest
  peer)

## Context

Since ADR-0003 a node that can't take new records is treated as a dishonest one: its close
group checks it for each record written near it, it answers that it lacks the record, and
the unheld-chunk penalty is charged until the node is evicted from routing. ADR-0022
restored that penalty on every lane and removed the switch that had held it off. A node
whose disk is full is therefore penalised for being full, and evicted, however honestly it
keeps and proves what it holds.

The erase floor of ADR-0003 Amendment 1 lets a node under disk pressure erase records far
from it: beyond ten times the distance to its 7th-nearest peer, each after its current
close group proves possession. On a node with many peers nearby, little is that far, so a
disk can reach the full line while it still holds extra copies it could give up safely.
And the clock that starts when a record leaves the node's range lived only in memory: a
restart stopped the floor from erasing anything for the three-day out-of-range wait.

## Decision Drivers

- A node that is full keeps serving and proving what it holds; it should not be charged
  for refusing what it can't store.
- Copies stay at the close group's 7 while nodes fill: a full node's place is taken by the
  next nearest, not left empty.
- Nothing inside a node's retention width is ever erased, and every erasure keeps its
  possession check.
- Replies older nodes read keep their shape; a new message goes only to peers that read
  it.

## Considered Options

1. Keep ADR-0003: penalise and evict full nodes.
2. A grace period: let a full node answer "absent" for a while, then charge it.
3. A full node says so, renewing the notice while it stays full; its peers widen their
   windows around it and waive the absent-key penalty for it in one bounded case.

## Decision

Option 3, with a falling erase floor so a node gives up every extra copy it safely can
before it is full.

### The falling floor

Under disk pressure the erase floor falls as free space does:

| Free space below | Records may be erased beyond |
|---|---|
| 10% | 10 times the distance to the node's 7th-nearest peer |
| 7.5% | 5 times |
| 5% | 2 times |
| 2.5% | the edge of the node's own range: every out-of-range record past its wait |

- Each step also fires once free bytes are at or below the full line plus one 4 MiB chunk,
  so on a partition too small for a step's share to clear the line (under about 40 GiB for
  the last step with a 1 GiB line), the steps collapse in order onto the line and the last
  one always runs before the node is full.
- At the edge of the node's range no distance is kept, but nothing for which the node is
  among the retention width's nearest (9, more with full peers among them) is ever
  eligible.
- Free space is the smaller of the free share of bytes and the free share of inodes, where
  the filesystem counts inodes. Free inodes below 2.5% count as being at the full line.
- A failed proof of possession is not penalised for a key beyond ten times the distance
  to the 7th-nearest peer, whatever the step: the node's routing table may not know which
  nodes are nearest a key that far away.

### Erasure in two phases, in its own task

The prune passes run in a task of their own after each neighbour-sync cycle. An eligible
record is first selected, and from then on named in no replica hint this node sends; it
is erased only in a pass of a later cycle, after its possession check, and never while
this node answered "present" for it within the fetch window (the verification and fetch
timeouts plus the longest retry wait, 345 s), for chunks and pointers alike (a pointer
state or fetch answered with the record counts as "present"). A node therefore never hints
a record and then answers that it lacks it.

A record whose possession check fails leaves the selection at once and is hinted again,
since its close group lacks copies and this node's hint is what lets the group fetch it
back. It is not selected again for 3 sync epochs (one whole cycle plus the two of the
two-phase wait), so it is hinted for at least a whole cycle before it is hidden and tried
again.

The selection holds at most 1,024 records and 1,024 pointers, what one pass may audit at
most. A record found ready that the pass doesn't try, for lack of audit requests or because
the pressure ended first, leaves the selection too and is hinted again until a later pass
selects it, so no record stays unhinted for longer than its own wait however far the
selection runs ahead of what a pass can audit.

### Out-of-range clocks that survive restarts

Each held record's and pointer's out-of-range start time is kept in wall-clock seconds in
`{root}/out_of_range.mdb`, with the time of the write, written at the end of each prune
pass and loaded at start. A restored clock counts only the time the node saw before that
write: the time the node was down, or ran a release that doesn't keep the table (a
rollback), doesn't count toward the wait, since nothing checked then that the key stayed
out of range. A start after the write counts as now; a record back in range or no longer
held is cleared; a missing, unreadable or older-layout table starts every clock now, as
before.

### Full, not faulty

A node is **full** when it is at the full line and its last prune passes, at the floor's
last step, erased nothing and left nothing selected for erasure and not yet erased
(in its wait, deferred by a pass, or kept after an answer of "present"): a record retried
after a failed possession check is tried again later, and doesn't keep a node from being
full. Then it:

- prices no new data: a quote for data it doesn't hold is refused with the existing
  `QuoteFailed`. A chunk it holds is still answered as already stored, so nobody pays for
  it again, and an update of a pointer it holds is still quoted;
- refuses every new record, fresh offers with the existing `Rejected`, but takes the
  repair of a chunk it held and removed because its bytes were wrong, which replaces the
  space the removal freed: full or at the line, it needs room only for its temporary
  file, and only up to the bytes that removal freed (at most 1,024 such chunks
  remembered, each with its freed size); above the line it is admitted as any record;
- takes updates and repairs of pointers it holds, which use no new space;
- keeps serving and proving what it holds: its audit and possession replies keep their
  format, real digests for held keys and the absent digest for others;
- tells its peers with a one-way `FullNotice`, appended to the replication messages (body
  index 25: node-1b-sync's two page messages keep 23 and 24, and the notice, merged after
  them in node-1b, takes the next; the merge moved one constant,
  `BEFORE_FULL_NOTICE_LAST_INDEX`, from 22 to 24) and
  sent only to peers whose user agent names the release that reads it, or that sent one
  themselves, renewed every 10 minutes while it stays full; a peer once found to read it
  is remembered (at most 4,096) until it leaves the routing table, so a renewal still
  reaches it while it is disconnected;
- logs one line on entering ("Node full: since <unix seconds>") and one on leaving;
  the state isn't kept across a restart, so at start a node with room above the line
  logs "Node full: not at start", which `nodes status` reads as no longer full, and one
  that starts at the line tells its peers once, at its first renewal, if no prune pass
  has run by then, so their marks don't lapse in between; if that pass then doesn't make
  it full, the next renewal takes the notice back. Every notice is read and sent under
  one lock, so a renewal can't send "full" after the node has said it left;
- leaves "full" only with 256 MiB of free bytes above the line, and inodes, where
  counted, off their line; when inodes put it in "full", it also needs 65,536 inodes
  above their line, or a fortieth of the filesystem's inodes where that is fewer, so a
  filesystem with few inodes can always be left. The state doesn't flap.

### How other nodes treat a full node

- A peer is marked full only from its own notice. The mark lapses 40 minutes (two of the
  longest neighbour-sync intervals) after the last notice unless renewed.
- With f peers marked full among a key's nearest, replica hints and fresh offers (of
  chunks and pointers alike) go to the 7 + f nearest, and a node admits and keeps the key
  while it is among the 9 + f nearest,
  both at most 20. The full node stays responsible for what it holds; the nodes standing
  in for it are in range, not erase candidates.
- Erasing a copy near full nodes needs 6 proofs from the 7 nearest nodes not marked full.
  A full node that lacks the key is neither a proof nor a veto; one that holds it still
  proves it; the erasing node never counts itself. The marks are read again just before
  deleting. When 14 or more of a key's 20 nearest are marked full, the window's cap of 20
  holds fewer than 7 not marked full, and 6 proofs are still needed from the 20: peers
  announcing themselves full never lower the bar. Only a routing table that knows fewer
  peers than the close group (a tiny network) lowers it, as before.
- The possession check after a write probes only the 9 nearest, as before: stand-ins
  beyond them are offered the record, and take it, older nodes included, since a chunk
  receiver admits a fresh offer anywhere among the key's 20 nearest. They aren't
  possession-checked because no peer past the 9 nearest was before this release.
  Nothing is waived for them, and a stand-in that takes the record is, by intent, not
  recorded as seen holding it. A pointer's check after a write probes its 7 nearest, as
  before, and its stand-ins likewise aren't checked.

### The one waiver of the unheld-chunk penalty (amends ADR-0022)

The penalty stays on every lane, with no switch and no environment override. A checker
waives the per-key absent penalty only when all of these hold:

- the peer's own "full" notice is fresh (younger than 40 minutes);
- the peer answered that it lacks the key (the absent digest, "not found", or a denial);
  no answer, a wrong digest or any other failure is charged;
- the check isn't pinned to the peer's commitment (subtree audits are charged);
- the checker hasn't seen the peer prove it holds the key (a matched digest, a fetch
  served with the right bytes, or a block verified in a subtree audit's second round);
  the record of these proofs is a Bloom filter in two generations, each sized at 10 bits
  for each of 20 peers per record held (2 to 32 MiB), a new one started once the current
  holds as many proofs as it was sized for, so its false "seen" answers, which charge,
  stay under about 3.5% however long the node runs; the record, its hash seed and both
  generations are saved to `{root}/seen_holders.bin` at the end of each prune pass that
  changed it (a temporary file renamed over the old one) and loaded at start, so a
  restart or an upgrade forgets nothing; a missing or unreadable file starts it empty,
  logged;
- on the possession check after a write, the peer's own quote isn't among the paid quotes
  of the write's proof: a node paid for a record it then can't take is charged.

The same holds for the possession check after a pointer write (a seventh lane, beside the
six of chunks): a full peer that answers it holds no state of the pointer at all, a new
address it refused, is waived on the same conditions. One that holds an older state of
the pointer is charged, since a full node still takes updates of the pointers it holds.

Every lane asks one function, `FullView::waives_unheld_penalty`, at one reviewed site. The
bound on how long a node may answer "full" without the penalty is how long one notice
stays fresh: a node that keeps renewing it, prices nothing and passes audits of what it
holds is never penalised for being full.

### Amends ADR-0003

A full node is no longer treated as a dishonest one while it says it is full. Its close
group still checks it, and still charges it for anything outside the waiver above. Nodes
of earlier releases drop the notice, keep charging a full node and don't widen their
windows until they update.

## Consequences

### Positive

- A full node keeps its place, its reputation and its data; copies stay at 7 around it.
- Every extra copy that can be given up safely is given up before a node is full, on small
  partitions too.
- A restart no longer resets the out-of-range wait.
- A node out of inodes refuses new data before payment instead of failing the write after
  it.

### Negative / Trade-offs

- Capacity is still set by the smallest disks: filling is graceful until most of a key's
  nearest nodes are full.
- Stand-ins for a full node hold copies a little farther out; when the node leaves "full",
  those become ordinary out-of-range copies, erased only through the possession-checked
  prune.
- The first start into this release has no saved clocks: for up to three days a node at
  the line may be full while it holds copies it could erase later. No data is lost.
- The record of keys a peer was seen proving forgets a proof two generations later,
  unless the peer proves the key again: the waiver may then apply to a key the peer
  proved that long ago. A generation holds 10 bits' worth of proofs for each of 20 peers
  per record held, so this is at least one generation's worth of other proofs later.
  This turnover narrows "never seen" to "not seen within the two generations kept", and
  is an amendment to the S4 wording, for Dan to confirm. Restarts don't forget: the
  record is saved with its seed (up to 2 x 32 MiB on disk, written only when it changed).
  The first start into this release, or one after the file is lost, starts it empty.

### Neutral / Operational

- The prune-pass line reports the floor's step, the records selected for erasure, held
  records with a peer marked full in their window, the keys verification found with fewer
  than 4 holders, how full the record of keys peers were seen proving is and its estimated
  share of false "seen" answers, and whether the node is full.

## Validation

- Unit tests: the floor's steps and the clamp on 15, 20, 40 and 80 GB partitions
  (`retention.rs`); the inode share; leaving "full"; the widened groups and the six-proof
  rule with 1 to 4 of 7 peers full, a lapsed mark and a full holder (`pruning.rs`); the
  two phases and the clocks (`erasure.rs`); the marks, the waiver on each lane
  (`full.rs`, `tests/fork_pins.rs`); the notice's encoding and an older node dropping it
  (`protocol.rs`).
- The census pins each lane's one waiver site and the lane it names
  (`retention/census.rs`).
- Integration tests on simulated small partitions with the shipped line
  (`tests/small_disks.rs`), and the quote, pointer and possession paths of a full node.

## Notes for AI-assisted work

AI tools may help draft this ADR, but **must not mark it Accepted without human review**.
