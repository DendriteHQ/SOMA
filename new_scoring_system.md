# New Scoring System (Proposal)

This document describes a proposed scoring model for SOMA competitions.
It is documentation only; it does not change the current implementation.

## Goal

Keep quality at least as strong as the baseline, then compete only on
compression. Perfect solutions are ranked and incentivized by compression
ratio.

## Task types

| Task type | Pass-ratio gate |
|---|---|
| `swebench_verified` | `y ≥ ⌊x · τ⌋` |
| `swe_explorer_edit` | `y ≥ ⌊x · τ⌋` |
| `swe_explorer_explore` | `y ≥ x · τ` (no floor) |

Default threshold:

```text
τ = 0.9
```

### Symbols

For **verified** and **edit**:

- `x` = number of resolved baseline runs for the problem
- `y` = number of resolved miner runs for the problem

For **explore**:

- `x` = baseline quality `q_B` (e.g. hit-file rate − noise-file rate)
- `y` = miner quality `q_A`

A problem is **good** when the pass-ratio gate above holds.

## Perfect solution

Default perfect bar:

```text
ρ = 0.95
```

A miner (solution) is **perfect** when:

```text
(# good problems) / (# problems) > ρ
```

Exactly 95% is not enough; the share must be **strictly greater than** 95%.

Non-perfect solutions do not receive a competition score under this model.

## Score

For perfect solutions only, the score for each task type is the
**compression ratio**.

Per problem:

```text
r = T_B / T_A
```

where `T_B` and `T_A` are weighted baseline and miner token totals.

Aggregate task-type score (higher is better):

```text
S = (Σ T_B) / (Σ T_A)
```

over problems with valid token pairs.

Suggested token weighting (same idea as today):

```text
T = w_i·T_i + w_c·T_c + w_o·T_o
```

with defaults `w_i = 1.0`, `w_c = 0.1`, `w_o = 3.0`.

## Incentives

Do **not** invent a new weight-distribution scheme.

1. Compute perfect-solution compression scores per task type.
2. Feed those scores into the **existing** incentive method
   (Easy / Medium / Hard difficulty layers, layered winners, burn scaling).

Only perfect solutions participate. Among them, higher compression score wins
more incentive weight under the current layering rules.

## End-to-end flow

```text
per problem:
  check pass-ratio gate (τ = 0.9)
      │
      ▼
miner:
  good on > 95% of problems? ──no──► no score
      │
     yes
      ▼
  score = compression ratio (per task type)
      │
      ▼
  distribute incentives with existing Easy/Medium/Hard method
```

## Suggested config knobs

| Name | Default | Meaning |
|---|---:|---|
| Pass-ratio threshold `τ` | `0.9` | Per-problem quality gate |
| Perfect-problem ratio `ρ` | `0.95` | Perfect-solution bar |

## Notes

- Quality first, compression second: no trading pass rate away for tokens.
- Explore uses a continuous quality gate (`y ≥ x · τ`); verified/edit use
  discrete resolve counts with floor `⌊·⌋`.
- This file is the design record for the proposal; the live scoring code
  remains unchanged until an explicit implementation pass.

## Preview tip (Cursor)

Cursor’s built-in Markdown preview often does **not** render LaTeX/KaTeX.
This doc uses Unicode math so it stays readable without a math extension.
If you want KaTeX anyway: enable `@builtin markdown-math`, set
`markdown.math.enabled` to `true`, then open **Markdown: Open Preview**
(`Ctrl+Shift+V`) — not the side Preview tab.
