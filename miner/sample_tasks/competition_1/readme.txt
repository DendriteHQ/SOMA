Competition 1 Sample Tasks - File Guide
======================================

This folder contains exported CSV data for competition 1.

Files
-----

1) challenges.csv
- Purpose: Master list of challenges.
- Rows (CSV records): 201 including header (200 challenge rows).
- Physical lines in file: 23,383 (higher because `challenge_text` contains embedded newlines).
- Columns:
  - challenge_id
  - challenge_name
  - challenge_text
- Notes:
  - One row per challenge.
  - `challenge_text` contains the full passage/task content.

2) challenge_QA.csv
- Purpose: Questions and reference answers linked to each challenge.
- Rows (CSV records): 1,001 including header (1,000 QA rows).
- Columns:
  - challenge_id
  - question_id
  - question_text
  - answer_id
  - answer_text
- Notes:
  - Multiple rows can exist for the same `challenge_id` (one per question/answer pair).
  - Use this file together with `challenges.csv` to get both passage and QA for each challenge.

How files relate
----------------
- Join key: `challenge_id`
- Typical flow:
  1. Read challenge metadata/content from `challenges.csv`.
  2. Attach all matching QA rows from `challenge_QA.csv`.
