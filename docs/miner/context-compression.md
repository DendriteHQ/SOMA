# Compression

## Evaluation Process

The evaluation pipeline measures how well a miner preserves task-relevant information under constrained compression in the presence of injected noise.

---

### 1️⃣ Dataset Preparation (Preprocessing Stage)

Before the miner receives any input:

- A **base text** is selected.
- A set of **questions and ground-truth answers** is generated based solely on the base text.
- The base text is augmented with **irrelevant injected sentences**.

The injected sentences:
- Are unrelated to the evaluation questions
- Introduce noise and increase compression difficulty
- Should ideally be removed by an effective compression algorithm

The resulting **injected text** (base text + noise) becomes the miner's input.

---

### 2️⃣ Miner Script Input

The miner receives:

- The **injected text**
- A specified **compression ratio**

The miner does *not* have access to:
- The clean base text
- The ground-truth answers
- Any annotation indicating which sentences were injected

---

### 3️⃣ Compression Stage

The miner must:

- Compress the injected text
- Strictly follow the provided **compression ratio constraint**
- Preserve information necessary to answer the predefined questions

The objective is to:
- Remove irrelevant injected content
- Retain semantically important information from the original text

---

### 4️⃣ LLM-Based Answer Generation

- The **compressed text** is passed to a language model
- The model answers the predefined questions
- The model has access only to the compressed representation

---

### 5️⃣ Answer Validation & Scoring

- Model-generated answers are compared against **ground-truth answers**
- Evaluation is using **token-level F1** with compression ratio weighting

This produces a score $S(b)$ for a given compression level $b$.

---

### Multi-Level Compression Scoring

Evaluation is performed independently for multiple compression levels:

- **20%**
- **40%**
- **60%**

Each level produces its own score $S(b)$.

The final aggregated score is computed as:

$$
\text{Score} = \frac{\sum_b w(b)\ S(b)}{\sum_b w(b)}
$$

Where:

- $S(b)$ — score at compression level $b$
- $w(b) = \frac{1}{\sqrt{b}}$ — weight inversely proportional to the square root of the compression ratio
- Weights are normalized by dividing by their sum

---

### Weighting Rationale

This weighting scheme:

- Emphasizes **aggressive compression**, which is harder to perform without losing information
- Rewards solutions that preserve accuracy at **extreme compression ratios**
- Reflects real-world value, where stronger compression typically has higher practical impact


