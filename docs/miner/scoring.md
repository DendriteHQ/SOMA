# SWE Scoring

This document describes the current SWE scoring logic implemented in
`mcp_platform/app/api/routes/scoring.py`.

## Task types and scoring paths

There are three benchmark task types:

1. `swebench_verified`
2. `swe_explorer_edit`
3. `swe_explorer_explore`

Scoring is split into two paths:

- `swebench_verified` and `swe_explorer_edit` use the same SWE task-score path:
  `compute_swe_task_score`, `build_swe_miner_scores`, and
  `build_swe_miner_total_score`.
- `swe_explorer_explore` uses a separate explore-quality path:
  `compute_explore_task_score` and `compute_explore_miner_total_score`.

## Token counting

Token totals are computed from a weighted token-type breakdown.

Let:

- `T_i` be non-cached input tokens,
- `T_c` be cached input tokens,
- `T_o` be output tokens.

```math
T = w_i T_i + w_c T_c + w_o T_o
```

Default weights:

| Token type | Weight |
|---|---:|
| Input, non-cached | `1.0` |
| Cached input | `0.1` |
| Output | `3.0` |

Current behavior of `compute_weighted_tokens`:

- `input_tokens` and `output_tokens` are required.
- Missing `cached_input_tokens` is treated as `0`.
- The function returns `None` if a required value is missing or any supplied
  token count is negative.

## SWE path

This path is used for:

- `swebench_verified`
- `swe_explorer_edit`

### Per-task scoring inputs

For each task:

- `x` is the number of resolved baseline runs. Only resolved baselines count.
- `y` is the number of resolved miner runs.
- `T_B` is the average weighted token count across resolved baseline runs.
- `T_A` is the average weighted token count across miner runs with valid
  weighted token counts.

### Compression ratio

The compression-ratio term is the base-2 logarithm of the baseline-to-miner
weighted-token ratio, clamped to `[-2, 2]`:

```math
r = \max\left(-2,\min\left(\log_2\left(\frac{T_B}{T_A}\right),2\right)\right)
```

If the token inputs are invalid:

```math
r = 0
```

### Penalty threshold

```math
t = \left\lfloor 0.8x \right\rfloor
```

### Per-task score

The per-task score is calculated by `compute_swe_task_score`.

| Constant | Value |
|---|---:|
| Bonus cap | `3.0` |
| Penalty floor | `-4.0` |
| Penalty ceiling | `-2.0` |

### Hard tasks

A task is hard when `x <= 1`.

#### Excluded task

If `y == 0`:

- `score=None`
- `pool=excluded`

The task does not contribute to the miner aggregate.

#### Maintain zone

If `x == 1` and `y == 1`:

```math
s = r
```

#### Bonus zone

All other non-excluded hard tasks use:

```math
s = \max\left(-2,\min\left(r+\frac{y-x}{5-x},3\right)\right)
```

Hard tasks are assigned to `pool=hard_boost`.

Their hard-boost contribution is:

```math
h = \max(0,s)
```

Only the positive part of the hard-task score contributes to the boost.

### Standard tasks

A task is standard when `x >= 2`.

#### Penalty zone

If `y < t`:

```math
s = \max\left(-4,\min\left(-2-2\left(1-\frac{y}{t}\right),-2\right)\right)
```

#### Maintain zone

If `t <= y <= x`:

```math
s = r
```

#### Bonus zone

If `y > x`:

```math
s = \max\left(-2,\min\left(r+\frac{y-x}{5-x},3\right)\right)
```

Standard tasks are assigned to `pool=main`.

## SWE miner aggregation

Miner-level aggregation is performed by `build_swe_miner_scores`.

### Main score

For every task in `pool=main`, the aggregation weight is:

```math
w_i = x_i^{1/3}
```

The `main_score` is the weighted average of main-task scores:

```math
S_M = \frac{\sum_i s_i x_i^{1/3}}{\sum_i x_i^{1/3}}
```

If the miner has no tasks in `pool=main`:

```math
S_M = 0
```

### Hard boost

The `hard_boost` is the sum of positive hard-task contributions divided by the
number of scored main and hard tasks:

```math
B_H = \frac{\sum_i h_i}{N_M+N_H}
```

Where:

- `N_M` is the number of tasks in `pool=main`.
- `N_H` is the number of tasks in `pool=hard_boost`.
- `h_i` is the hard-boost contribution of hard task `i`.

If there are no hard-boost contributions:

```math
B_H = 0
```

### Raw miner total

```math
S_R = S_M + B_H
```

## Final normalized SWE score

Final normalization is performed by `build_swe_miner_total_score`.

First, the raw total is clamped to `[-4, 3]`:

```math
S_C = \max\left(-4,\min\left(S_R,3\right)\right)
```

The clamped value is then linearly normalized from `[-4, 3]` to `[-1, 1]`:

```math
S_N = 2\left(\frac{S_C+4}{7}\right)-1
```

Equivalently:

```math
S_N = \frac{2S_C+1}{7}
```

This normalized value is consumed by downstream category and leaderboard
scoring.

## Explore path

This path is used for:

- `swe_explorer_explore`

Explore scoring has a different objective: preserve exploration quality while
reducing weighted token usage.

### Per-task explore score

The per-task score is calculated by `compute_explore_task_score`.

### Exploration quality

Let:

- `f` be the hit-file rate,
- `n` be the noise-file rate.

Quality is:

```math
q = f-n
```

The quality margin is:

```math
m = q_A-q_B
```

Where:

- `q_A` is miner quality.
- `q_B` is baseline quality.

The default quality threshold is:

```math
\delta = 0.20
```

### Hard quality floor

If `m <= -delta`, the task receives the hard-floor score:

```math
s_E = -2
```

### Quality-aware token score

Otherwise, a smooth quality gate `g` in `[0, 1]` is computed:

```math
g = 3r^2 - 2r^3
```

where:

```math
r = \max\left(0,\min\left(\frac{m+\delta}{2\delta},1\right)\right)
```

This gate is used asymmetrically:

- for token savings, it unlocks reward as quality improves;
- for token overspend, it softens the penalty as quality improves.

The token term is:

```math
\tau = \max\left(-2,\min\left(2\log_2\left(\frac{T_B}{T_A}\right),2\right)\right)
```

Let:

```math
\eta = 0.25
```

The final per-task explore score is:

```math
s_E =
\begin{cases}
g\tau & \text{if } \tau \ge 0 \\
\left(\eta + (1-\eta)(1-g)\right)\tau & \text{if } \tau < 0
\end{cases}
```

This means:

- when the miner saves tokens, better quality is required to unlock the reward;
- when the miner uses more tokens than baseline, better quality reduces the
  size of the penalty, but does not remove it entirely;
- once the miner quality margin drops to `m <= -delta`, the hard floor still
  applies.

## Explore miner aggregation

Miner-level explore aggregation is performed by
`compute_explore_miner_total_score`.

The aggregate:

1. starts from the mean of the per-task explore scores,
2. applies the hard floor only if the miner is worse than baseline on both
   average quality margin and total weighted token usage,
3. otherwise keeps the per-task mean unchanged,
4. normalizes the resulting score to `[-1, 1]`.
