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

Token totals are calculated from the token-type breakdown using weighted token
counts:

```math
Tok
=
w_{\mathrm{input}} T_{\mathrm{input}}
+
w_{\mathrm{cached}} T_{\mathrm{cached}}
+
w_{\mathrm{output}} T_{\mathrm{output}}
```

Default weights:

| Token type | Weight |
|---|---:|
| Input, non-cached | `1.0` |
| Cached input | `0.1` (`1/10`) |
| Output | `3.0` |

Current behavior of `compute_weighted_tokens`:

- `input_tokens` and `output_tokens` are required.
- A missing `cached_input_tokens` value is treated as `0`.
- The function returns `None` when a required value is missing or any supplied
  token count is negative.

## SWE path

This scoring path is used for:

- `swebench_verified`
- `swe_explorer_edit`

### Per-task scoring inputs

For each task:

- $x$ is the number of resolved baseline runs. Only resolved baselines are
  counted.
- $y$ is the number of resolved miner runs.
- $Tok_B$ is the average weighted token count across resolved baseline runs.
- $Tok_A$ is the average weighted token count across miner runs that have valid
  weighted token counts.

### Compression ratio

The token-compression term is:

```math
r
=
\operatorname{clamp}
\left(
\ln\left(\frac{Tok_B}{Tok_A}\right),
-2,
2
\right)
```

If the token inputs are invalid:

```math
r = 0
```

### Penalty threshold

The penalty threshold is:

```math
t = \left\lfloor 0.8x \right\rfloor
```

### Per-task score

The per-task score is calculated by `compute_swe_task_score`.

Constants:

| Constant | Value |
|---|---:|
| Bonus cap | `3.0` |
| Penalty floor | `-4.0` |
| Penalty ceiling | `-2.0` |

### Hard tasks

A task is considered hard when `x <= 1`.

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

All other non-excluded hard tasks use the bonus formula:

```math
s
=
\operatorname{clamp}
\left(
r + \frac{y-x}{5-x},
-2,
3
\right)
```

Hard tasks are assigned to `pool=hard_boost`.

Their hard-boost contribution is:

```math
h = \max(0, s)
```

Only the positive part of the hard-task score contributes to the boost.

### Standard tasks

A task is considered standard when `x >= 2`.

#### Penalty zone

If `y < t`:

```math
s
=
\operatorname{clamp}
\left(
-2 - 2\left(1-\frac{y}{t}\right),
-4,
-2
\right)
```

#### Maintain zone

If `t <= y <= x`:

```math
s = r
```

#### Bonus zone

If `y > x`:

```math
s
=
\operatorname{clamp}
\left(
r + \frac{y-x}{5-x},
-2,
3
\right)
```

Standard tasks are assigned to `pool=main`.

## SWE miner aggregation

Miner-level aggregation is performed by `build_swe_miner_scores`.

### Main score

For every task in `pool=main`, the aggregation weight is:

```math
w_i = x_i^{1/3}
```

The `main_score` is the weighted average of the main-task scores:

```math
S_{\mathrm{main}}
=
\frac{
\sum_i s_i x_i^{1/3}
}{
\sum_i x_i^{1/3}
}
```

If the miner has no tasks in `pool=main`:

```math
S_{\mathrm{main}} = 0
```

### Hard boost

The `hard_boost` is the sum of positive hard-task contributions divided by the
total number of scored main and hard tasks:

```math
B_{\mathrm{hard}}
=
\frac{
\sum_i h_i
}{
N_{\mathrm{main}} + N_{\mathrm{hard}}
}
```

Where:

- $N_{\mathrm{main}}$ is the number of tasks in `pool=main`.
- $N_{\mathrm{hard}}$ is the number of tasks in `pool=hard_boost`.
- $h_i$ is the hard-boost contribution for hard task $i$.

If there are no hard-boost contributions:

```math
B_{\mathrm{hard}} = 0
```

### Raw miner total

The raw miner total is:

```math
S_{\mathrm{raw}}
=
S_{\mathrm{main}}
+
B_{\mathrm{hard}}
```

## Final normalized SWE score

Final normalization is performed by `build_swe_miner_total_score`.

First, the raw total is clamped to the range $[-4,3]$:

```math
S_{\mathrm{clamped}}
=
\operatorname{clamp}
\left(
S_{\mathrm{raw}},
-4,
3
\right)
```

The clamped value is then linearly normalized from $[-4,3]$ to $[-1,1]$:

```math
S_{\mathrm{normalized}}
=
2\left(
\frac{S_{\mathrm{clamped}} + 4}{7}
\right)
-1
```

Equivalently:

```math
S_{\mathrm{normalized}}
=
\frac{2S_{\mathrm{clamped}} + 1}{7}
```

This normalized value is consumed by downstream category and leaderboard
scoring.

## Explore path

This scoring path is used for:

- `swe_explorer_explore`

Explore scoring has a different objective: preserve exploration quality while
reducing weighted token usage.

### Per-task explore score

The per-task score is calculated by `compute_explore_task_score`.

### Exploration quality

Quality is defined as:

```math
q
=
\operatorname{hitFileRate}
-
\operatorname{noiseFileRate}
```

The quality margin is:

```math
m = q_A - q_B
```

Where:

- $q_A$ is miner quality.
- $q_B$ is baseline quality.

The default quality threshold is:

```math
\delta = 0.20
```

### Hard quality floor

If the quality margin is at or below the negative threshold:

```math
m \leq -\delta
```

the task receives the hard-floor score:

```math
s_{\mathrm{explore}} = -2
```

### Quality-gated token score

Otherwise, a smooth quality gate is calculated:

```math
g \in [0,1]
```

The token term is:

```math
\tau
=
\operatorname{clamp}
\left(
2\log_2\left(\frac{Tok_B}{Tok_A}\right),
-2,
2
\right)
```

The final per-task explore score is:

```math
s_{\mathrm{explore}} = g\tau
```

## Explore miner aggregation

Miner-level explore aggregation is performed by
`compute_explore_miner_total_score`.

The aggregate:

1. starts from the mean of the per-task explore scores,
2. mixes that mean toward the score floor based on the total token-savings
   ratio,
3. saturates the token-savings influence at $\pm 20\%$,
4. normalizes the resulting score to $[-1,1]$.
