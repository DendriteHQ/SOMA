# Incentive Mechanism

This document explains how miner rewards are determined under a new layer-based incentive system.

1. After the baseline runs without compression are evaluated, tasks are divided into three predefined categories: **Easy**, **Medium**, and **Hard**.
2. The incentive system is organized into layers, and each layer is assigned its own incentive weight:
   1. **L_0 - General:** `{(Easy, Medium, Hard)}`
   2. **L_1 - Pairs:** `{(Easy, Medium), (Easy, Hard), (Medium, Hard)}`
   3. **L_2 - Single categories:** `{(Easy), (Medium), (Hard)}`
3. For each element in each layer, miner scores are computed using only the tasks that belong to that subset. Any miner with the highest achieved score wins that element.
   Example: for the (Easy, Medium)` element in layer **L_1**, the winner is the miner with the best average score across all tasks in the **Easy** and **Medium** categories.
4. Once the winners of all elements are known, weights are distributed according to the following formulas:

$$
W(L_i)=\frac{1}{2^i}
$$

$$
W(e \in L_i)=\frac{W(L_i)}{|L_i|}
$$

where:

- $L_i$ denotes any layer,
- $e$ denotes an element belonging to that layer.

5. The total weight of each miner is then computed as:

$$
W_{total}(m)=\sum_{e:\, m\in Winners(e)}\frac{W(e)}{|Winners(e)|}
$$

where $Winners(e)$ is the set of all winners of element $e$.

6. If miners are meant to receive a total of $X\%=1-Burn%$ of the full incentive pool, then the incentive assigned to miner $m$ is:

$$
INC(m)=\frac{W_{total}(m)}{\sum_{n\in Miners}W_{total}(n)}\cdot X\%
$$

#### Example

Suppose three miners **A**, **B**, and **C** participate in the competition. Their scores on **Easy**, **Medium**, and **Hard** are:

1. **A**: `(1, 0, -1)`
2. **B**: `(1, 0, 0)`
3. **C**: `(0, 0.5, 1)`

Assume miners receive a total of **50% of the incentive pool**.

The layer results are then:

1. **L_0:** `INC(L_0)=1`
   1. `(Easy, Medium, Hard)`: `score(A)=0`, `score(B)=0.33`, `score(C)=0.5`
2. **L_1:** `INC(L_1)=1/2`
   1. `(Easy, Medium)`: `score(A)=1`, `score(B)=1`, `score(C)=0.5`
   2. `(Easy, Hard)`: `score(A)=0`, `score(B)=1`, `score(C)=0.5`
   3. `(Medium, Hard)`: `score(A)=-0.5`, `score(B)=0`, `score(C)=0.75`
3. **L_2:** `INC(L_2)=1/4`
   1. `(Easy)`: `score(A)=1`, `score(B)=1`, `score(C)=0`
   2. `(Medium)`: `score(A)=0`, `score(B)=0`, `score(C)=0.5`
   3. `(Hard)`: `score(A)=-1`, `score(B)=0`, `score(C)=1`

This gives:

1. **C** receives weight `1` from `L_0` + `1/6` from `L_1` + `2/12` from `L_2`, for a total of `4/3`.
2. **B** receives weight `0` from `L_0` + `3/12` from `L_1` + `1/24` from `L_2`, for a total of `7/24`.
3. **A** receives weight `0` from `L_0` + `1/12` from `L_1` + `1/24` from `L_2`, for a total of `1/8`.

The sum of weights is:

$$
\frac{4}{3}+\frac{7}{24}+\frac{1}{8}=\frac{32}{24}+\frac{7}{24}+\frac{3}{24}=\frac{42}{24}
$$

After converting this to incentive, we obtain:

$$
INC(C)=\frac{32}{42}\cdot 50\%=\frac{16}{42}=\frac{8}{21}\approx 38.10\%
$$

$$
INC(B)=\frac{7}{42}\cdot 50\%=\frac{1}{12}\approx 8.33\%
$$

$$
INC(A)=\frac{3}{42}\cdot 50\%=\frac{1}{28}\approx 3.57\%
$$