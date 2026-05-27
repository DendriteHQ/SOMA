# SWE Scoring

This document explains how SWE miner scores are computed. This logic is implemented in `mcp_platform/app/api/routes/scoring.py`.

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

   a. If the baseline run passes and the miner run passes:

      $$
      Score(T_{type}) = 1.0, \qquad \lambda(T_{type}) = 0.5
      $$

   b. If the baseline run passes and the miner run fails:

      $$
      Score(T_{type}) = -4.0, \qquad \lambda(T_{type}) = 0.0
      $$

   c. If the baseline run fails and the miner run passes:

      $$
      Score(T_{type}) = 4.0, \qquad \lambda(T_{type}) = 0.5
      $$

   d. If the baseline run fails and the miner run fails:

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

7. The miner total score is the arithmetic average of all valid run scores across all tasks, including screener tasks:

$$
TotalScore(m)=\frac{1}{K_m}\sum_{r \in AllRuns(m)}RunScore(r)
$$

where $K_m$ is the number of all scored runs of miner $m$.

8. The miner screener score is computed separately, using only screener tasks:

$$
ScreenerScore(m)=\frac{1}{S_m}\sum_{r \in ScreenerRuns(m)}RunScore(r)
$$

where $S_m$ is the number of scored screener runs of miner $m$.
