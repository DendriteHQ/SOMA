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
      "answer": "...",              // present only if ANSWERABLE
      "supporting_quote": "...",    // exact verbatim quote from DOCUMENT; present only if ANSWERABLE
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
