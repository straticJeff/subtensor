# `optimised` branch

RPC read-throughput patches on top of upstream `main`, plus a self-contained CI
workflow that publishes an x86_64 image to ghcr.io.

**Every optimisation is off by default.** With no env vars set this build
behaves like upstream, so the image is a drop-in replacement and each change can
be A/B tested independently on one node.

## Measured results

Measured 2026-09-21 on a production archive node (144 cores, 504 GiB RAM,
4.4 TB RocksDB archive), node isolated and pinned to 96 cores, open-loop
generator on the other 48, workload weighted to metagraph/neuron/stake reads
plus historical-block reads. Cache-warming bias was controlled by repeating the
baseline an hour later: it landed within 0.2%.

Baseline (upstream binary, current production flags) at 200 rps offered:
**161 rps achieved, 21% CPU, p99 pinned at the 30 s timeout, 865 errors.**

| change | throughput | notes |
|---|---|---|
| `--db-cache 32768` (**flag only, no code**) | 161 → 194 rps | p50 787 ms → 17 ms, all timeouts gone |
| `SUBTENSOR_STATEDB_FAST_PIN=1` | 157 → 188 rps | archive mode only |
| `SUBTENSOR_RPC_CACHE_BYTES=4294967296` | 157 → 187 rps | p50 3888 ms → 3.5 ms, ~half the CPU/request |
| all of the above, at 400 rps offered | 130 → 366 rps | p50 11 s → 1.8 ms, 0 errors |

The single biggest win is a **deployment flag, not code**: the default
`--db-cache` gives RocksDB a ~341 MiB block cache for a 4.4 TB archive.

## Environment variables

| env var | suggested | effect |
|---|---|---|
| `SUBTENSOR_STATEDB_FAST_PIN` | `1` | Skip the state-db global write lock in `pin`/`unpin`. Under `ArchiveAll` these mutate nothing, but every RPC call takes the lock twice. Archive nodes only. |
| `SUBTENSOR_RPC_CACHE_BYTES` | `4294967296` | Per-block result cache for heavy read methods, keyed by (call, SCALE params, block hash), with single-flight so concurrent identical requests share one execution. Entries are pure functions of the key, so they cannot go stale. |
| `SUBTENSOR_WASM_OFFCHAIN_DYNAMIC` | **leave unset** | Dynamic heap for RPC runtime calls. **Do not enable.** wasmtime reallocates and copies the whole linear memory on each growth, so a call touching ~7.5 MB memcpys hundreds of MB. Measured under production EVM traffic: **10.74 cores with it, 0.32 without**, for identical work. |
| `SUBTENSOR_WASM_WARM_SLOTS` | `64` | wasmtime pooling slots kept warm. Upstream keeps 4, so past that it re-maps the copy-on-write image on every instantiation. |
| `SUBTENSOR_WASM_MAX_INSTANCES` | unset | Concurrent wasm calls (upstream: hard-coded 64). **Rate-dependent**: 144 measured −6% at 200 rps but +12% at 400 rps. Only raise it for genuinely high-concurrency nodes. |
| `SUBTENSOR_WASM_KEEP_RESIDENT` | unset | `memset` instead of `madvise` on instance teardown. **Leave at 0** — worse at every size tested, and redundant once the dynamic offchain heap is on. |

Recommended archive-node starting point:

```
SUBTENSOR_STATEDB_FAST_PIN=1
SUBTENSOR_RPC_CACHE_BYTES=4294967296
SUBTENSOR_WASM_WARM_SLOTS=64
```

plus `--db-cache 32768 --trie-cache-size 34359738368` on the node command line.

Watch memory after enabling the cache flags: `--db-cache` also sizes RocksDB
memtables (state column gets budget×0.9÷4 per memtable), so a 32 GiB budget
implies 7.2 GiB memtables. Expect 45–60 GiB steady state; drop to
`--db-cache 16384 --trie-cache-size 17179869184` if that is too close to the
container limit.

## What changed

- `pallets/subtensor/rpc/` — result cache + single-flight; `blocking` dispatch
  for the 22 heavy methods only (it costs a thread handoff, which makes cheap
  sub-millisecond methods ~35% *worse*).
- `node/src/service.rs` — builds the executor explicitly so the offchain heap
  strategy can differ from the on-chain one. The plumbing is kept, but the
  dynamic strategy it enables is a trap: see the warning above.
- `vendor/sc-state-db`, `vendor/sc-executor-wasmtime` — copies of two
  polkadot-sdk crates at the pinned rev, carrying the lock fast-path and the
  pooling knobs. Their dependencies point at the same git rev the workspace
  uses, so the graph keeps a single copy of `sc-executor-common`, `sp-core` and
  friends. Patching them as path crates inside a local polkadot-sdk checkout
  splits the graph and fails to compile.
- `.github/workflows/` — upstream's workflows need self-hosted runners and R2
  sccache credentials that do not exist in a fork, so they are replaced by one
  self-contained image build.

## Not included

Two of the worst costs are in the runtime, so they need a runtime upgrade rather
than a client patch. Both were measured against real state:

- `delegateInfo_getDelegates` — **times out at 30 s**, never completes.
- `delegateInfo_getDelegated` — **5.4 s for a single wallet**; it scans every
  delegate instead of iterating `StakingHotkeys(coldkey)`. This dominates the
  p99 tail of every run.


## A caution about the benchmark behind these numbers

The measurements above come from a workload weighted toward metagraph, neuron
and stake reads. Real production traffic on these nodes turned out to be ~95%
`eth_getTransactionReceipt` and `eth_getBlockByNumber`, which the result cache
does not cover at all: it wraps only the subtensor custom methods.

Two lessons, both learned the hard way:

- A benchmark whose method mix does not match production will rank
  optimisations wrongly. Check the real mix via `substrate_rpc_calls_finished`
  on the metrics endpoint before trusting any number here.
- `OFFCHAIN_DYNAMIC` looked mildly positive on a frozen chain and was
  catastrophic under production traffic. The frozen benchmark did show the
  anomaly, a patched config burning more CPU than an unpatched one, and it was
  misattributed to binary version drift rather than investigated.
