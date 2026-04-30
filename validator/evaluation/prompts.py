import textwrap

ANSWERS_GENERATION_PROMPT = textwrap.dedent("""
You are answering questions based strictly on a provided document.

You will be given:
1) DOCUMENT — a text that is the ONLY authoritative source of information
2) QUESTIONS — a list of questions

CRITICAL RULES (non-negotiable):
- Treat DOCUMENT as complete and authoritative.
- Answer each question using ONLY information explicitly stated in DOCUMENT.
- Do NOT use prior knowledge, general knowledge, or assumptions.
- Do NOT infer, extrapolate, or reconstruct missing information.
- If DOCUMENT does not explicitly contain enough information to answer a question with certainty, you MUST say so.

This is NOT a test of general knowledge.
This is a test of whether the information is present in the document.

For each question, choose exactly ONE of the following outcomes:
- ANSWERABLE
- NOT_ANSWERABLE_FROM_DOCUMENT

Definitions:
- ANSWERABLE:
  The document explicitly contains all information required to answer the question with certainty.
- NOT_ANSWERABLE_FROM_DOCUMENT:
  The document does not explicitly contain sufficient information to answer the question.

STRICT GUIDELINES:
- Do NOT guess.
- Do NOT rely on what is “typically true” or “commonly known”.
- Numeric ranges, thresholds, qualifiers, lists, conditions, and exceptions must be fully present.
- Partial information is NOT sufficient — treat it as NOT_ANSWERABLE_FROM_DOCUMENT.
- If a question has multiple required components, ALL must be supported by the document.
- If you cannot point to a specific sentence in DOCUMENT that directly supports your answer, you MUST choose NOT_ANSWERABLE_FROM_DOCUMENT.
- Question includes an answer format hint such as [word], [number], [digit], or [letter], treat it as a description of the expected shape of the answer.
- Never copy bracketed format hints literally into the answer unless the document itself literally contains those bracketed characters.

For each question:
- If ANSWERABLE:
  - Provide the answer derived from DOCUMENT that satisfies the requested answer format.
  - Provide an EXACT verbatim quote from DOCUMENT that supports the answer.
- If NOT_ANSWERABLE_FROM_DOCUMENT:
  - State explicitly that the required information is not present in DOCUMENT.
  - Do NOT provide a quote.

Output JSON only:

{{
  "results": [
    {{
      "id": "Q1",
      "status": "ANSWERABLE | NOT_ANSWERABLE_FROM_DOCUMENT",
      "answer": "...",
      "supporting_quote": "...",
      "notes": "Brief justification (1–2 sentences)"
    }}
  ]
}}

Inputs:
<<<DOCUMENT
{document_text}
DOCUMENT>>>

<<<QUESTIONS
{questions}
QUESTIONS>>>
""")

ANSWER_SCORING_PROMPT = textwrap.dedent("""
You are grading candidate answers against reference answers.

You will be given a JSON array named ITEMS. Each item contains:
- id
- question
- reference_answer
- candidate_answer

Your job is to score how well candidate_answer matches reference_answer for the given question.

Rules:
- Use the question to interpret the expected meaning.
- Compare candidate_answer only to reference_answer.
- Do not use outside knowledge.
- Treat an empty or whitespace-only reference_answer as an intentionally empty expected answer, not as missing data.
    - You must use only one of these scores: 0.0, 0.5, 0.75, 1.0.
    - Do not invent intermediate values such as 0.2, 0.6, 0.8, or 0.9.
    - An answer that merely restates the question, echoes its wording, gives only the topic, or gives a vague paraphrase without the key resolving fact must score 0.0.
    - To receive any positive score, candidate_answer must contain the core fact that makes the answer actually correct rather than just related.
    - If candidate_answer is irrelevant, contradicted, materially false, or missing the core resolving fact, score it 0.0.
    - If candidate_answer is fully correct and semantically equivalent to the reference answer, score it 1.0.
    - Use 0.5 only when the core resolving fact is present, but a major required detail is missing.
    - Use 0.75 only when the core resolving fact and the main required detail are present, but a qualifier, condition, exception, or precise scope is missing.
- Be strict about missing qualifiers, dates, counts, names, negations, and conditions.
    - High lexical overlap is not enough for credit.
    - If candidate_answer introduces a material contradiction to the reference answer, score it 0.0 even if some parts overlap.
    - First reason using these checks:
        - core_fact_present: does the answer include the central fact needed to resolve the question?
        - major_detail_present: does it include the main required supporting detail?
        - qualifiers_preserved: are important qualifiers, conditions, exceptions, counts, and negations preserved?
        - no_material_error: does it avoid materially false or contradictory claims?
    - Then assign score deterministically:
        - 0.0 = no core fact, irrelevant, contradicted, or materially false
        - 0.5 = core fact present, but a major required detail is missing
        - 0.75 = core fact and major detail present, but an important qualifier/condition/scope is missing
        - 1.0 = fully correct and semantically equivalent
    - Verdict mapping:
        - 0.0 -> INCORRECT
        - 0.5 or 0.75 -> PARTIAL
        - 1.0 -> CORRECT
- Output valid JSON only.

Output format:

{{
    "results": [
    {{
        "id": "Q1",
        "score": 0.0,
        "verdict": "CORRECT | PARTIAL | INCORRECT",
        "reasoning": "Short justification"
    }}
    ]
}}

ITEMS:
{items}
""")