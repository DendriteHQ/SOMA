# SWE Scoring

This document explains how SWE miner scores are computed. This logic is implemented in `mcp_platform/app/api/routes/scoring.py`.

The scoring has two layers:

1. A raw score is computed from the current run-vs-baseline formula.
2. The final total score adds one extra multiplier based on total token savings.

## 1. Raw Run Score

1. Every miner run is compared against baseline runs of the same task. Each comparison uses:

$$
Score(T_{type}) + \lambda(T_{type}) \cdot Trim\left(\text{ln}\left(\frac{Tok_B}{Tok_A}\right), -2, 2\right)
$$

where:

- $Tok_B$ is the number of tokens used by the baseline run,
- $Tok_A$ is the number of tokens used by the miner run,
- $Trim(x, -2, 2)$ keeps $x$ in the interval $[-2, 2]$.

2. If either token count is missing, non-positive, or otherwise invalid, the token component is treated as `0`.

3. The base score and the coefficient $\lambda$ depend on the pass/fail outcome
of the baseline run and the miner run:

Case A: the baseline run passes and the miner run passes.

$$
Score(T_{type}) = 1.0, \qquad \lambda(T_{type}) = 0.5
$$

Case B: the baseline run passes and the miner run fails.

$$
Score(T_{type}) = -4.0, \qquad \lambda(T_{type}) = 0.0
$$

Case C: the baseline run fails and the miner run passes.

$$
Score(T_{type}) = 4.0, \qquad \lambda(T_{type}) = 0.5
$$

Case D: the baseline run fails and the miner run fails.

$$
Score(T_{type}) = 0.0, \qquad \lambda(T_{type}) = 0.1
$$

4. If either the baseline validation result or the miner validation result is unknown, that baseline-miner comparison does not contribute to the score.

5. A miner run may be compared against multiple baseline variants of the same task. In that case, the miner run score is the arithmetic average of all valid baseline-miner comparison scores for that run:

$$
RunScore(r)=\frac{1}{N_r}\sum_{i=1}^{N_r}PairScore(r, b_i)
$$

where $N_r$ is the number of valid baseline comparisons for run $r$.

6. Each task may contain multiple miner runs. The task score is the arithmetic average of all valid run scores for that task:

$$
TaskScore(t)=\frac{1}{M_t}\sum_{j=1}^{M_t}RunScore(r_j)
$$

where $M_t$ is the number of scored miner runs for task $t$.

7. The miner raw total score is the arithmetic average of all valid run scores across all tasks, including screener tasks:

$$
RawTotalScore(m)=\frac{1}{K_m}\sum_{r \in AllRuns(m)}RunScore(r)
$$

where $K_m$ is the number of all scored runs of miner $m$.

8. The miner raw screener score is computed separately, using only screener tasks:

$$
RawScreenerScore(m)=\frac{1}{S_m}\sum_{r \in ScreenerRuns(m)}RunScore(r)
$$

where $S_m$ is the number of scored screener runs of miner $m$.

## 2. Miner-Level Token Savings Multiplier

1. After the raw total score is computed, one extra multiplier is applied from the miner's total token usage across the whole dataset. This is meant to penalize miners who do not compress, or who use more tokens than the baseline:

$$
s = 1 - \frac{Tok_C}{Tok_B}
$$

where:

- $Tok_C$ is the total number of tokens over all compressed runs by that miner,
- $Tok_B$ is the total number of tokens over all baseline runs for the same miner dataset slice.

2. The savings ratio is then normalized and clamped:

$$
x = \min\left(1, \max\left(\frac{s + 0.20}{0.40}, 0\right)\right)
$$

3. The smooth multiplier is:

$$
m(s) = -2x^3 + 3x^2
$$

4. This means:

- if the miner saves at least $20\%$, then $m(s)=1$ and the raw total score stays the same,
- if the miner increases token usage by at least $20\%$, then $m(s)=0$ and the final score becomes $-4$,
- between those points, the score is adjusted smoothly toward $-4$.

5. The final total score is:

$$
FinalTotalScore(m) = -4 + (RawTotalScore(m) + 4) \cdot m(s)
$$

6. The screener score is not changed by this penalty. Screener tasks still count inside $RawTotalScore(m)$, but the separate screener score remains:

$$
ScreenerScore(m) = RawScreenerScore(m)
$$

7. If the token totals are missing or invalid, the multiplier is $1$, so the raw total score stays unchanged.
