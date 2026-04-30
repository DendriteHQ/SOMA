Sample Tasks - File Guide
=========================

This directory contains per-competition task dumps.

Competition folders
-------------------

- Context-Compression-1
- Context-Compression-2
- Context-Compression-3
- Context-Compression-4
- Context-Compression-5
- Context-Compression-6

Each competition folder contains:

1) challenges.csv
- Master list of challenges.
- Columns:
  - challenge_id
  - challenge_name
  - challenge_text

2) challenge_QA.csv
- Questions and reference answers for challenge scoring.
- Columns:
  - challenge_id
  - question_id
  - question_text
  - answer_id
  - answer_text

How files relate
----------------

- Join key: `challenge_id`
- Typical flow:
  1. Read challenge text and metadata from `challenges.csv`.
  2. Attach QA rows from `challenge_QA.csv`.
