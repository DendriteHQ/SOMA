# SWE Scoring

This document describes the current SWE scoring logic implemented in
`mcp_platform/app/api/routes/scoring.py`.

## Task types and scoring paths

There are three benchmark task types:

1. `swebench_verified`
2. `swe_explorer_edit`
3. `swe_explorer_explore`

Scoring is split into two paths:

- `swebench_verified` and `swe_explorer_edit` use the same SWE task-score path
  (`compute_swe_task_score`, `build_swe_miner_scores`,
  `build_swe_miner_total_score`).
- `swe_explorer_explore` uses a different explore-quality path
  (`compute_explore_task_score`, `compute_explore_miner_total_score`).

## Token counting

Token totals are weighted from the token-type breakdown:

$$
T_{\mathrm{weighted}}
=
w_{\mathrm{input}} T_{\mathrm{input}}
+
w_{\mathrm{cached}} T_{\mathrm{cached}}
+
w_{\mathrm{output}} T_{\mathrm{output}}
$$

Default weights:

| Token type | Weight |
|---|---:|
| Input, non-cached | $1.0$ |
| Cached input | $\frac{1}{10}$ |
| Output | $3.0$ |

Current behavior of `compute_weighted_tokens`:

- requires `input_tokens` and `output_tokens`,
- treats missing `cached_input_tokens` as `0`,
- returns `None` for missing required values or negative values.

## SWE Path (`swebench_verified` + `swe_explorer_edit`)

This path is used for:

- `swebench_verified`,
- `swe_explorer_edit`.

### Per-task scoring inputs

For each task:

- $x$: number of resolved baseline runs, counting resolved baselines only,
- $y$: number of resolved miner runs,
- $T_B$: average weighted baseline tokens across resolved baseline runs,
- $T_A$: average weighted miner tokens across miner runs with valid weighted
  token counts.

The compression-ratio term is:

$$
r
=
\operatorname{clamp}
\left(
\ln\left(\frac{T_B}{T_A}\right),
-2,
2
\right)
$$

If token inputs are invalid, then:

$$
r = 0
$$

The penalty threshold is:

$$
t = \left\lfloor 0.8x \right\rfloor
$$

### Per-task score

The per-task score is computed by `compute_swe_task_score`.

Constants:

- bonus cap: `3.0`,
- penalty floor: `-4.0`,
- penalty ceiling: `-2.0`.

### 1. Hard tasks

A task is considered hard when:

$$
x \leq 1
$$

If `y == 0`, the task is excluded:

- `score=None`,
- `pool=excluded`.

If `x == 1` and `y == 1`, the task is in the maintain zone:

$$
s = r
$$

Otherwise, the task uses the bonus path:

$$
s
=
\operatorname{clamp}
\left(
r + \frac{y-x}{5-x},
-2,
3
\right)
$$

These tasks go to `pool=hard_boost`.

Their hard-boost contribution is:

$$
h = \max(0, s)
$$

### 2. Standard tasks

A task is considered standard when:

$$
x \geq 2
$$

If `y < t`, the task is in the penalty zone:

$$
s
=
\operatorname{clamp}
\left(
-2 - 2\left(1-\frac{y}{t}\right),
-4,
-2
\right)
$$

If `t <= y <= x`, the task is in the maintain zone:

$$
s = r
$$

If `y > x`, the task is in the bonus zone:

$$
s
=
\operatorname{clamp}
\left(
r + \frac{y-x}{5-x},
-2,
3
\right)
$$

These tasks go to `pool=main`.

## Miner aggregation

Miner-level aggregation is performed by `build_swe_miner_scores`.

### Main score

The `main_score` is a weighted average over tasks in `pool=main`.

For each task, the weight is:

$$
w_i = x_i^{1/3}
$$

The main score is:

$$
S_{\mathrm{main}}
=
\frac{
\sum_i s_i x_i^{1/3}
}{
\sum_i x_i^{1/3}
}
$$

If there are no main tasks:

$$
S_{\mathrm{main}} = 0
$$

### Hard boost

The `hard_boost` is the sum of hard-task contributions divided by the total
number of scored main and hard-boost tasks:

$$
B_{\mathrm{hard}}
=
\frac{
\sum_i h_i
}{
N_{\mathrm{main}} + N_{\mathrm{hard}}
}
$$

where:

- $N_{\mathrm{main}}$ is the number of tasks in `pool=main`,
- $N_{\mathrm{hard}}$ is the number of tasks in `pool=hard_boost`.

If there are no hard-boost contributions:

$$
B_{\mathrm{hard}} = 0
$$

### Raw miner total

The raw miner total is:

$$
S_{\mathrm{raw}}
=
S_{\mathrm{main}}
+
B_{\mathrm{hard}}
$$

## Final normalized score

Final normalization is performed by `build_swe_miner_total_score`.

First, the raw total is clamped to:

$$
[-4, 3]
$$

Let:

$$
S_{\mathrm{clamped}}
=
\operatorname{clamp}
\left(
S_{\mathrm{raw}},
-4,
3
\right)
$$

It is then linearly normalized to:

$$
[-1, 1]
$$

Using linear interpolation, the normalized score can be written as:

$$
S_{\mathrm{normalized}}
=
2\left(
\frac{S_{\mathrm{clamped}} + 4}{7}
\right)
-1
$$

Equivalently:

$$
S_{\mathrm{normalized}}
=
\frac{2S_{\mathrm{clamped}} + 1}{7}
$$

This normalized value is what downstream category and leaderboard scoring
consume.

## Explore Path

This path is used for:

- `swe_explorer_explore`.

Explore uses a different objective: preserve exploration quality while reducing
weighted tokens.

### Per-task explore score

The per-task score is computed by `compute_explore_task_score`.

The quality margin is:

$$
m = q_A - q_B
$$

where:

- $q_A$ is miner quality,
- $q_B$ is baseline quality.

Quality is defined as:

$$
q
=
\operatorname{hitFileRate}
-
\operatorname{noiseFileRate}
$$

The default quality threshold is:

$$
\delta = 0.20
$$

If:

$$
m \leq -\delta
$$

then the score is set to the hard floor:

$$
s_{\mathrm{explore}} = -2
$$

Otherwise, a smooth quality gate:

$$
g \in [0,1]
$$

is applied to the token term.

The token term is:

$$
\tau
=
\operatorname{clamp}
\left(
2\log_2\left(\frac{T_B}{T_A}\right),
-2,
2
\right)
$$

The per-task explore score is:

$$
s_{\mathrm{explore}} = g\tau
$$

### Explore miner aggregation

Miner aggregation is performed by `compute_explore_miner_total_score`.

The aggregate:

1. starts from the mean of per-task explore scores,
2. mixes toward the score floor based on the total token-savings ratio,
3. saturates token-savings influence at $\pm 20\%$,
4. normalizes the final value to $[-1,1]$.
