# Incentive Mechanism

This document explains the current miner incentive mechanism.

## Benchmark types

The system uses three benchmark types:

1. `swebench_verified`
2. `swe_explorer_explore`
3. `swe_explorer_edit`

## Base benchmark weighting

Per-miner aggregate benchmark score uses:

- `swebench_verified`: **50%**
- `swe_explorer_explore`: **25%**
- `swe_explorer_edit`: **25%**

Let:

- $S_v(m)$ be miner $m$'s score on `swebench_verified`,
- $S_x(m)$ be miner $m$'s score on `swe_explorer_explore`,
- $S_e(m)$ be miner $m$'s score on `swe_explorer_edit`.

Then:

$$
S_{bench}(m) = 0.50 \cdot S_v(m) + 0.25 \cdot S_x(m) + 0.25 \cdot S_e(m)
$$

## Layered incentive weighting

Incentives are distributed through three layers over the benchmark-type subsets:

1. **L0 (triple):** `{(v, x, e)}`
2. **L1 (pairs):** `{(v, x), (v, e), (x, e)}`
3. **L2 (singles):** `{(v), (x), (e)}`

Layer weights are static:

- $W(L0)=0.25$
- $W(L1)=0.45$
- $W(L2)=0.30$

Element weight inside each layer:

$$
W(elem \in L_i)=\frac{W(L_i)}{|L_i|}
$$

So:

- `L0` has 1 element, each worth `0.25`
- `L1` has 3 elements, each worth `0.45 / 3 = 0.15`
- `L2` has 3 elements, each worth `0.30 / 3 = 0.10`

For each element, winner(s) are miner(s) with the top score on that subset. If tied, element weight is split evenly.

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
