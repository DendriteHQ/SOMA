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

This is the complexity-blind total: one benchmark type, so it is the miner's plain
score over its scored tasks. It is what the dashboard ranks on and what the screener
stages are judged by. The layers below split the *incentive* over complexity, not this
score.

## Task complexity

Every task carries a complexity category, recorded per task in
`swe_bench_tasks.complexity`:

| Category | Meaning |
|---|---|
| `short` | least agent effort expected |
| `medium` | |
| `long` | most agent effort expected |
| *(empty)* | not classified |

Complexity does **not** change a miner's score. Scoring is complexity-blind: a task
scores the same whichever category it is in, and a miner's total is the same average
over all its scored tasks as before. What complexity decides is **which contests a
miner is in** — see the layers below.

## Layered incentive weighting

Incentives are distributed through layers over subsets of the complexity categories.
Layer weights are static per subset size:

- $W(\text{triples})=0.25$
- $W(\text{pairs})=0.45$
- $W(\text{singles})=0.30$

With all three categories present, all three layers exist:

| Layer | Elements | $W(L_i)$ | Weight per element |
|---|---|---|---|
| L0 (triple) | `{(s,m,l)}` | 0.25 | 0.250 |
| L1 (pairs) | `{(s,m),(s,l),(m,l)}` | 0.45 | 0.150 |
| L2 (singles) | `{(s),(m),(l)}` | 0.30 | 0.100 |

Element weight inside each layer:

$$
W(elem \in L_i)=\frac{W(L_i)}{|L_i|}
$$

Layer weights are renormalized over the layers that actually exist, so a competition
with only two categories keeps the remaining layers in the same proportion
(`0.45/0.75` and `0.30/0.75`).

### Subset score

The score of miner $m$ on a subset is the weighted average of its per-category scores,
with the category weights renormalized within the subset:

$$
S_{subset}(m)=\frac{\sum_{c\in subset} w_c \cdot S_c(m)}{\sum_{c\in subset} w_c}
$$

**The three categories weigh the same** ($w_{short}=w_{medium}=w_{long}=1.0$), so a
subset score is the plain average of the miner's scores on its members. No complexity
is worth more than another.

$S_c(m)$ is miner $m$'s score computed over that category's tasks only — the same
quantity as its total score, restricted. A miner with **no score in a member category
does not compete on that element**: it has not lost that contest, it is not in it. So
consistency across categories is what wins the pair and triple elements, while
dominating one category wins a single element worth `0.100`.

For each element, winner(s) are miner(s) with the top score on that subset. If tied,
element weight is split evenly.

### Unclassified tasks

A task with no complexity recorded still counts towards a miner's total score, but
feeds no layer element. If **nothing** in a competition is classified there are no
categories at all, and incentives fall back to a single, complexity-blind element
carrying the whole weight — the behaviour that predates the category.

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

The repository's *contents* are derived from `swe_bench_tasks` as well. Tasks are built
into a long-lived source repository that accumulates every task ever produced, and that
one can never be published - it holds future competitions' tasks too. So the platform
mirrors just the images of the SOMA tasks the current competitions reference into the
published repository, and deletes everything else from it. What goes public is therefore
exactly the set of tasks the competition is being scored on.

Deletions only run while the repository is private, which in practice is the upload
window: the repository is emptied there and refilled as a competition's tasks are
imported. A task whose images have not landed yet is not dispatched at all - its runs
wait in `pending` rather than failing in a sandbox that cannot pull them.

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
