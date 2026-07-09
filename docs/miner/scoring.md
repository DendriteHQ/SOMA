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
  (`compute_swe_task_score`, `build_swe_miner_scores`, `build_swe_miner_total_score`).
- `swe_explorer_explore` uses a different explore-quality path
  (`compute_explore_task_score`, `compute_explore_miner_total_score`).

## Token counting

Token totals are weighted from token-type breakdown:

$$
Tok = w_{\text{input}} \cdot T_{\text{input}} + w_{\text{cached}} \cdot T_{\text{cached}} + w_{\text{output}} \cdot T_{\text{output}}
$$

Default weights:

| Token type | Weight |
|---|---|
| Input (non-cached) | $1.0$ |
| Cached input | $\frac{1}{10}$ |
| Output | $3.0$ |

Current behavior of `compute_weighted_tokens`:
- requires `input_tokens` and `output_tokens`,
- treats missing `cached_input_tokens` as `0`,
- returns `None` for missing required values or negative values.

## SWE Path (`swebench_verified` + `swe_explorer_edit`)

### Per-task scoring inputs

For each task:
- $x$: number of resolved baseline runs (resolved baselines only),
- $y$: number of resolved miner runs,
- $Tok_B$: average weighted baseline tokens across resolved baseline runs,
- $Tok_A$: average weighted miner tokens across miner runs with valid weighted token counts.

Compression ratio term:

$$
r = \text{clamp}\left(\ln\left(\frac{Tok_B}{Tok_A}\right), -2, 2\right)
$$

If token inputs are invalid, $r=0$.

Penalty threshold:

$$
t = \lfloor 0.8x \rfloor
$$

### Per-task score (`compute_swe_task_score`)

Constants:
- bonus cap: `3.0`,
- penalty floor: `-4.0`,
- penalty ceiling: `-2.0`.

### 1) Hard tasks (`x <= 1`)

- If `y == 0`: task is excluded (`score=None`, `pool=excluded`).
- Else if `x == 1 and y == 1`: `score = r` (maintain zone).
- Else: bonus path

$$
score = \text{clamp}\left(r + \frac{y-x}{5-x}, -2, 3\right)
$$

These tasks go to `pool=hard_boost`, with contribution:

$$
\text{hard\_boost\_contribution} = \max(0, score)
$$

### 2) Standard tasks (`x >= 2`)

- If `y < t`: penalty zone

$$
score = \text{clamp}\left(-2 - 2\left(1 - \frac{y}{t}\right), -4, -2\right)
$$

- Else if `y <= x`: maintain zone, `score = r`.
- Else (`y > x`): bonus zone

$$
score = \text{clamp}\left(r + \frac{y-x}{5-x}, -2, 3\right)
$$

These tasks go to `pool=main`.

### Miner aggregation (`build_swe_miner_scores`)

Given all task scores for a miner:

1. `main_score` is weighted average over `pool=main` tasks:

$$
main\_score = \frac{\sum score_i \cdot x_i^{1/3}}{\sum x_i^{1/3}}
$$

If there are no main tasks, `main_score = 0`.

2. `hard_boost` is average hard contribution over total scored tasks:

$$
hard\_boost = \frac{\sum \text{hard\_boost\_contribution}}{\#main\_tasks + \#hard\_boost\_tasks}
$$

If there are no hard-boost contributions, `hard_boost = 0`.

3. Raw miner total:

$$
raw\_total = main\_score + hard\_boost
$$

### Final normalized score (`build_swe_miner_total_score`)

Raw total is clamped to range:

$$
[-4, 3]
$$

then linearly normalized to:

$$
[-1, 1]
$$

This normalized value is what downstream category/leaderboard scoring consumes.

## Explore Path (`swe_explorer_explore`)

Explore uses a different objective: preserve exploration quality while reducing
weighted tokens.

Per task (`compute_explore_task_score`):

- Quality margin:

$$
margin = miner\_quality - baseline\_quality
$$

where quality is `(hit_file_rate - noise_file_rate)`.

- If `margin <= -delta` (`delta=0.20`), score is hard floor `-2.0`.
- Otherwise, a smooth quality gate in `[0,1]` is applied, then multiplied by
  token term:

$$
\tau = \text{clamp}\left(2\log_2\left(\frac{Tok_B}{Tok_A}\right), -2, 2\right)
$$

so per-task score is `gate * tau`.

Miner aggregate (`compute_explore_miner_total_score`):

- Starts from mean of per-task scores,
- mixes toward floor based on total token savings ratio (with saturation at
  `±20%`),
- then normalizes to `[-1,1]`.
