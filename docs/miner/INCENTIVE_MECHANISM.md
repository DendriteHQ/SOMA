# Incentive Mechanism

This document explains the current miner incentive mechanism.

## Benchmark types

There is one benchmark type:

1. `swebench_verified` — the agent is given an issue and must produce a patch, which is
   graded against the task's own tests.

Both kinds of task in a competition run under it. What differs between them is the
dataset an instance is resolved from, not how it is scored:

| Stage | Dataset (`swe_bench_tasks.benchmark_name`) | Task images |
|---|---|---|
| Screener stage 1 | `SWE-bench/SWE-bench_Verified` (public Hugging Face) | SWE-bench naming conventions |
| Screener stage 2, full evaluation | SOMA task lists (e.g. `soma-is-tasks`) | Each row ships its own env/test image |

See [`mcp_platform/app/services/benchmarks.py`](../../mcp_platform/app/services/benchmarks.py)
for how a task's dataset is resolved, and
[Task images and repository visibility](#task-images-and-repository-visibility) below for
how validators obtain the images they grade with.

## Base benchmark weighting

Per-miner aggregate benchmark score:

- `swebench_verified`: **100%**

Let $S_v(m)$ be miner $m$'s score on `swebench_verified`. Then:

$$
S_{bench}(m) = S_v(m)
$$

## Layered incentive weighting

Incentives are distributed through layers over the benchmark-type subsets. Layer weights
are static per subset size:

- $W(\text{triples})=0.25$
- $W(\text{pairs})=0.45$
- $W(\text{singles})=0.30$

and are renormalized over the layers that actually exist, so with a single benchmark type
only the singles layer remains and it carries the full weight:

1. **L0 (singles):** `{(v)}`, $W(L0)=1.0$

Element weight inside each layer:

$$
W(elem \in L_i)=\frac{W(L_i)}{|L_i|}
$$

So `L0` has 1 element, worth `1.0`.

### Subset score

The score of miner $m$ on a subset is the base-weighted average over the subset members,
with the base benchmark weights renormalized within the subset:

$$
S_{subset}(m)=\frac{\sum_{b\in subset} w_b \cdot S_b(m)}{\sum_{b\in subset} w_b}
$$

where $w_v=1.0$. For a single benchmark this reduces to the raw per-benchmark score. A
miner with no score on any member benchmark does not compete on that element.

For each element, winner(s) are miner(s) with the top score on that subset. If tied,
element weight is split evenly.

## Miner raw incentive weight

$$
W_{total}(m)=\sum_{elem:\, m\in Winners(elem)}\frac{W(elem)}{|Winners(elem)|}
$$

## Converting to final incentive share

If miners receive:

$$
X = 1 - BurnRatio
$$

then:

$$
INC(m)=\frac{W_{total}(m)}{\sum_{n\in EligibleMiners}W_{total}(n)}\cdot X
$$

## Task images and repository visibility

A SOMA task is graded by running its own `test` image: the repository at `base_commit`
with the task's test patch applied, plus a `run_tests` entrypoint that reproduces the
command the task was validated with. The validator pulls that image, applies the miner's
patch inside it, runs `run_tests`, and checks the task's `FAIL_TO_PASS` / `PASS_TO_PASS`
ids against the pytest JSON report.

Those images live in a **private** Docker Hub repository, because publishing a
competition's hidden tasks in advance would let miners inspect the tests they are about
to be scored on. Rather than distributing registry credentials to every validator, the
platform flips that repository public for as long as the hidden tasks are being run and
graded, and back to private afterwards.

Both phases that use hidden tasks — screener stage 2 and full evaluation — happen inside
the **evaluation** window. Stage 2 begins at `eval_starts_at`, not at `upload_ends_at`;
full evaluation follows it once the stage-2 cohort has been ranked. The stretch between
`upload_ends_at` and `eval_starts_at` is idle, with no hidden-task run in existence yet:

```
    upload_starts_at          upload_ends_at            eval_starts_at                  eval_ends_at
     |                         |                         |                               |
     |--- stage 1, uploads ----|--------- idle ----------|-- stage 2, then evaluation ---|
     |                         |                         |                               |
                                                         |----- task images public ------|  + grace
```

So the repository goes public exactly when the first stage-2 run can be dispatched, and
stays public until `eval_ends_at` plus a grace period, so a validation still in flight
when the competition closes can finish pulling.
