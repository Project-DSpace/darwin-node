# BBRv2 — ported from cloudflare/quiche

Saorsa's BBRv2 congestion controller. The state machine (startup, drain,
probe_bw with UP/DOWN/CRUISE/REFILL phases, probe_rtt) and per-packet
bandwidth sampler are vendored from cloudflare/quiche and adapted for
saorsa-transport; the net-new code here is the shim types, the smoke tests
and the `Bbr2Adapter` that implements saorsa's streaming `Controller` trait
on top of quiche's batched `CongestionControl` interface.

## Usage

```rust
let config = P2pConfig {
    congestion_algorithm: CongestionAlgorithm::Bbr2,
    // ...
};
```

Or via the raw factory:

```rust
let mut cc = crate::congestion::Bbr2Config::default();
cc.initial_window(10 * 1200);
let factory: Arc<dyn ControllerFactory + Send + Sync> = Arc::new(cc);
```

## Licensing & attribution

All vendored files preserve their original copyright notices. The code
attributed to The Chromium Authors is under the BSD-3-Clause licence in
[`LICENSE`](LICENSE), and the code by Cloudflare, Inc. is under the
BSD-2-Clause licence in [`COPYING`](COPYING). Both files are unmodified
copies of the upstream licence files:

- `LICENSE` is the licence file of Google's QUICHE, whose C++ BBRv2,
  bandwidth sampler and windowed filter sources carry the same Chromium
  notice ("found in the LICENSE file"); Chromium's
  `net/third_party/quiche/README.chromium` names this file as QUICHE's
  licence. Taken from <https://quiche.googlesource.com/quiche> at commit
  `733621a62805d32997524d2e3997a690ff791e4e`.
- `COPYING` is cloudflare/quiche's licence file, taken from
  <https://github.com/cloudflare/quiche> at commit
  `ee20378bb2bd829baa77baa0da9a2a90c26fc17d`.

Saorsa's modifications to the vendored files and its own additions are
licensed under saorsa-transport's MIT OR Apache-2.0 terms. `mod.rs` and
`bandwidth.rs` state this under their notices; `adapter.rs`, `types.rs` and
`smoke_tests.rs` are saorsa-transport's own code under those terms.

## File inventory

| File | LoC | Origin | Role |
|---|---:|---|---|
| `mod.rs` | ~850 | `gcongestion/bbr2.rs` | Top-level `BBRv2` struct + `Params` + `DEFAULT_PARAMS` + `impl CongestionControl`. |
| `adapter.rs` | ~240 | saorsa original | `Bbr2Adapter` + `Bbr2Config` — implements saorsa's `Controller`. |
| `types.rs` | ~185 | saorsa original | Shim types (`Acked`, `Lost`, `BbrParams`, `RttStats`, `CongestionControl` trait, etc.). |
| `bandwidth.rs` | 355 | `recovery/bandwidth.rs` | `Bandwidth` newtype. |
| `bandwidth_sampler.rs` | ~860 | `gcongestion/bbr/bandwidth_sampler.rs` | Per-packet delivery rate sampler. Vendored tests stripped. |
| `network_model.rs` | 784 | `gcongestion/bbr2/network_model.rs` | `BBRv2NetworkModel` (bandwidth/rtt/inflight tracking). |
| `mode.rs` | ~300 | `gcongestion/bbr2/mode.rs` | Mode enum + `ModeImpl` trait (via `enum_dispatch`). |
| `startup.rs` | ~190 | `gcongestion/bbr2/startup.rs` | Startup mode with loss-aware exit. |
| `drain.rs` | ~120 | `gcongestion/bbr2/drain.rs` | Drain mode. |
| `probe_bw.rs` | ~610 | `gcongestion/bbr2/probe_bw.rs` | ProbeBW with UP/DOWN/CRUISE/REFILL cycle. |
| `probe_rtt.rs` | ~155 | `gcongestion/bbr2/probe_rtt.rs` | ProbeRTT mode. |
| `windowed_filter.rs` | 158 | `gcongestion/bbr/windowed_filter.rs` | Generic windowed max/min filter. |
| `smoke_tests.rs` | ~120 | saorsa original | Plumbing tests for the adapter. |

## Adapter semantics (saorsa's `Controller` → quiche's batched API)

saorsa calls into the controller per-packet:
- `on_sent(now, bytes, pn)` — for each sent packet
- `on_ack(now, sent, bytes, app_limited, rtt)` — for each acked packet
- `on_end_acks(now, in_flight, app_limited, largest_pn)` — at end of ack batch
- `on_congestion_event(now, sent, is_persistent, lost_bytes)` — for aggregated loss

The vendored BBRv2 wants a single batched call:
- `on_congestion_event(rtt_updated, prior_in_flight, bytes_in_flight, event_time, &[Acked], &[Lost], least_unacked, ...)`

The adapter buffers `Acked`/`Lost` entries and flushes at the two natural
batch boundaries in saorsa's flow:
1. `on_congestion_event` → flushes ack/loss batches when loss detection has
   just produced a congestion event.
2. `on_end_acks` → flushes any remaining ack batch and resyncs the adapter's
   shadow bytes-in-flight value with the connection's authoritative counter.

## Known fidelity gaps vs upstream quiche

The adapter mirrors bytes-in-flight by summing packet sends, acks, losses,
and abandoned packets, then resyncs from the connection's authoritative
counter in `on_end_acks`. The controller trait still does not pass
bytes-in-flight directly into `on_sent`, so this shadow counter is the main
remaining fidelity compromise versus quiche's native recovery manager.

## What this gets you over saorsa's BBRv1

1. **Loss-aware startup exit.** BBRv1 exits startup only on 3 rounds of
   no bandwidth growth; BBRv2 also exits on cumulative loss count
   (`startup_full_loss_count = 8`), which prevents the "startup
   overshoots on real wireless links" failure mode.
2. **`inflight_hi`/`inflight_lo` short-term caps** cap bytes-in-flight on
   loss events, preventing BBRv1's infamous bufferbloat-plus-tail-drop
   behaviour on shared queues.
3. **Reno coexistence** (`enable_reno_coexistence = true` by default):
   probe-max-rounds scales with BDP, so BBRv2 shares bottlenecks fairly
   with Reno/CUBIC flows.
4. **Per-packet delivery rate sampler** handles stretch-ACKs and ack
   coalescing correctly (modulo the pkt_num proxy gap above).

## Default

BBRv2 is the default as of this port (`CongestionAlgorithm::Bbr2`). Its
default initial congestion window is 10 packets, matching RFC 9002/quiche
style startup behavior. To opt back into BBRv1, set
`congestion_algorithm: CongestionAlgorithm::Bbr` in the `P2pConfig`; CUBIC is
available as `CongestionAlgorithm::Cubic` for comparison or when probing an
unknown path.
