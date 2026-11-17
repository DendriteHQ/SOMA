# Miner Prompt Editing Rules

This document defines what prompt and context edits are allowed in miner
submissions.

Goal: allow token reduction by removing content, while never putting anything
into the agent's context that was not there before.

## Core Rules

These rules take precedence over everything else in this document.

1. **No injection.** A submission must never inject anything into the agent's
   context. See [Rule 1](#rule-1-no-injection).
2. **Anything may be removed, except:**
   - **(a)** Non-encrypted text that carries information about the agent's
     next step, except thinking blocks and their signatures. See
     [Rule 2a](#rule-2a-protected-next-step-information).
   - **(b)** removals that create an injection on their own. See
     [Rule 2b](#rule-2b-no-injection-by-removal).

Removal is the default tool. Messages, chain-of-thought, tool calls, tool
arguments, tool results, file contents and command output may all be shortened
or removed unless Rule 2a or Rule 2b protects them. If a removal makes the
agent redo work later (for example, read a file again), that is allowed. The
cost shows up in the token score, not as a rule violation.

---

## Rule 1: No injection

Injection is anything in the compressed context that tells the agent, or lets
it conclude, something the original context did not.

Not allowed, among others:

- New instructions, hints, strategies, plans, priorities or constraints.
- Information about the task or its solution that is not in the original
  context: file paths, line numbers, root causes, patches, expected test
  results.
- Signals about progress or completion: "the task is done", "tests pass",
  "stop here", "submit now", "you are looping", "you are running out of
  budget".
- Rewording that changes meaning, strength or scope.

Removing first-person framing as allowed in Rule 2a is not rewording.
- Moving content so that its meaning changes: attaching a tool result to a
  different tool call, or changing the order of steps or instructions.

The only text a miner may add is neutral compression markup: omission markers,
marker definitions, and references to identical content elsewhere in the
context. An additional exception allows provenance tags identifying plain text
converted from the agent's earlier thinking (Rule 2a). See
[Compression Markers](#compression-markers).

Rephrasing or summarizing counts as removing the original text and writing new
text in its place. The new text must pass Rule 1 on its own: it may say only
what the replaced fragment already said, with no additions, interpretations or
conclusions. When in doubt, remove instead of rewriting.

---

## Rule 2a: Protected next-step information

Non-encrypted text that carries information about the next step must not be
removed, except for thinking blocks and their signatures as described below.
Protected next-step information outside thinking blocks must stay in context,
subject to the shortening and paraphrasing rules below.

### Explicit exception: thinking blocks

Thinking text and its associated signature may be removed, even if the thinking
text contains a plan or other next-step information. It is also allowed to
convert a thinking block into a plain text block in the same position, dropping
the signature and thinking label without adding any words to the retained
reasoning. The plain text may contain exactly the original thinking text or only
a verbatim portion of it (for example, its first paragraph). Provenance tags,
such as `<reasoning_recap about="the assistant's own earlier reasoning…">`,
may be added solely to mark that this text came from thinking; they must not
introduce any new instructions or task information. Otherwise, no new wording
may be inserted, and the remaining text must comply with Rule 1 and Rule 2b.


### Editing narration (plain-text) blocks

Narration blocks may be changed by removing, shortening, or paraphrasing their
text, subject to Rules 1, 2a, and 2b. This includes narration of completed or
abandoned steps, which may be removed. A narration block containing an
unfinished plan, commitment, or other next-step information remains protected:
its step information must be preserved, although it may be shortened or
paraphrased without changing its meaning. A block that mixes finished and
unfinished steps may be edited to remove only the finished parts, provided the
remaining text is not misleading.

Unlike thinking blocks, narration blocks have no blanket exception to Rule 2a.
Changing a block's wording or format does not allow new instructions, facts, or
progress claims to be introduced. The thinking-to-plain-text conversion
permitted above is a separate, explicit exception.

### What counts as information about the next step

Anything that says what the agent should do, intends to do, or still has to do:

1. The task itself and the instructions that govern it: what to do, how to do
   it, constraints, tool policy, safety policy, output format, completion
   criteria.
2. The agent's own plan for upcoming steps: the current to-do list, hypotheses
   it intends to check, and the step it announced it is about to take ("Now
   I'll run the tests to confirm the fix").
3. Unfinished commitments and open feedback: a step the agent said it would
   take but has not taken yet, or a user correction or request not yet acted
   on.
4. Corrections and changes of plan ("Actually the bug is in `parser.py`, not
   `lexer.py`"). When a later statement supersedes an earlier one, the later
   one is protected.

Text about steps that the context shows as finished or explicitly abandoned is
no longer next-step information and may be removed. This includes a visible
announcement of a tool call when that call has been executed and its result is
already in the context. Narration of past actions ("I opened the file and looked
at line 40") may also be removed.

When a statement mixes finished and unfinished plans (for example, "I'll run
the suite, then remove the scratch files"), the finished portion may be
removed while the unfinished portion must remain, with its original meaning,
conditions, and ordering preserved. Rule 2b still applies: removing the
finished portion must not turn the remaining text into a misleading statement.

**Shortening instead of removing.** Text that may be removed entirely may also
be shortened instead, provided the remaining text does not say something the
original did not (Rule 2b).

Allowed (each sentence announces the tool call made in the same turn, and its
result is in the context):

- Before: "I'll start by finding where the NATS AsyncAPI channel name and address are generated."
  After: "start by finding where the NATS AsyncAPI channel name and address are generated."
- Before: "All four tests pass. I'll run the full suite to check for regressions."
  After: "All four tests pass. run the full suite to check for regressions."
- Before: "Confirmed. I'll fix the typo."
  After: "fix the typo."

Not allowed:

- Before: "The scratch checks pass. Some tests fail, so I need to see whether my change caused them."
  After: "The scratch checks pass."
  (it reads as if everything passes)
- Before: "The likely culprit is `_detect_horizontal_flip`. I'll reproduce it before changing anything."
  After: "The culprit is `_detect_horizontal_flip`. reproduce it before changing anything."
  (a hypothesis becomes a fact)

### How protected text may be shortened

Protected text may be shortened **and/or paraphrased**. Shortening may remove
parts that carry no step information, such as whitespace, formatting, filler
words, and exact duplicates when one copy stays. Paraphrasing must preserve the
original meaning without altering or omitting step information.

**First-person framing.** In the agent's own messages, the first-person framing
in front of a step counts as filler words: "I'll", "I will", "Let me", "Let's",
"I need to", "I'm going to", "I want to". It may be removed. The step itself,
its target, its conditions, negations and modal words ("should", "might",
"could", "won't", "don't") must stay. Text shortened this way is still the
agent's own plan, not an instruction to the agent, and does not change its
strength.

Allowed:

- Before: "I'll list them and run my new tests on their own, then check whether the failures predate my change."
  After: "list them and run my new tests on their own, then check whether the failures predate my change."
- Before: "Before changing it, I'll check callers and tests so I don't break handle-based usage."
  After: "Before changing it, check callers and tests so I don't break handle-based usage."
- Before: "Git refuses to run inside the sandbox, so I won't stash. I'll read the failing test directly instead."
  After: "Git refuses to run inside the sandbox, so I won't stash. read the failing test directly instead."

Not allowed:

- Before: "If the build fails, I'll revert the change."
  After: "revert the change."
  (the condition is lost)
- Before: "I won't touch the tests."
  After: "touch the tests."
  (the negation is lost)
- Before: "I might need to update the docs."
  After: "update the docs."
  (the modal word is lost)

**Sequence words.** Sequence words at the start of a sentence ("First", "Next",
"Then", "Now", "Also", "Finally") may be removed when the remaining text
still lists the steps in the order they are meant to happen. A sequence word
that places a step before text written earlier must stay. Words that set an
order different from the text order ("before", "after", "once", "until") always
stay.

Allowed:

- Before: "Now I'll add tests and run them through `swe-exec`."
  After: "I'll add tests and run them through `swe-exec`."
- Before: "Next I'll confirm the new test fails without the fix, then run the full suites with the fix in place."
  After: "I'll confirm the new test fails without the fix, then run the full suites with the fix in place."
- Before: "I'll correct the docstring. Then I'll run the tests."
  After: "I'll correct the docstring. I'll run the tests."

Not allowed:

- Before: "Next I'll add tests. First I'll check the existing test files for conventions."
  After: "I'll add tests. I'll check the existing test files for conventions."
  (the remaining text reverses the order)
- Before: "I'll fix the parser, but first I'll reproduce the bug."
  After: "I'll fix the parser, but I'll reproduce the bug."
  (the remaining text reverses the order)

Every step, target, condition, constraint, and ordering in protected text must
survive unchanged in meaning. If the text cannot be shortened or paraphrased
without losing any of these, keep it as is. The explicit thinking-block
exception above still applies.

---

## Rule 2b: No injection by removal

A removal is not allowed when the remaining content says something the original
did not. Removal must not be used to build an injection out of the original's
own words.

Examples of removals that are not allowed:

- Removing a negation or qualifier: "Do not modify the tests" becoming "modify
  the tests".
- Removing a condition: "If the build fails, revert the change" becoming
  "revert the change".
- Cutting failure lines from test output so that it reads as passing, for
  example keeping "12 passed" out of "12 passed, 2 failed".
- Trimming an error message so that it points to a different file, line or
  cause.
- Emptying a tool result without an omission marker, so that the agent reads it
  as "no output" (for example, a `grep` that appears to find nothing).
- Removing a later correction so that an earlier, superseded statement looks
  current.
- Removing the text between two fragments so that they join into a new sentence
  or instruction.

Rule of thumb: when the absence of content could itself be read as
information, replace that content with an omission marker instead of deleting
it silently.

---

## Compression Markers

Compression markers are the only text a miner may add to the context. They show
where content was removed and, optionally, where it can be found again.

Allowed:

- Omission markers that describe the removed content in neutral terms: its
  location (file and line range, message, tool call), size or kind, for example
  `[omitted: tool output, 240 lines]` or `[omitted: 40 PASSED lines]`.
  Marker names and wording are up to the miner, including custom markers,
  provided they accurately describe the removed material and do not introduce
  new rules, tips on how to work, or other information prohibited by Rule 1.
- A source line reference (a single line or an inclusive range) inside any
  marker that replaces code or file content, so that the omitted lines remain
  locatable.
- `Same response as in ...` references, only when the referenced block is
  explicit, unique and identical.
- Marker aliases, provided they are defined before first use.

Every marker must have clear, balanced and unambiguous boundaries.

Not allowed:

- Markers that judge, rank or comment on the omitted content, such as
  `[omitted: irrelevant]`, `[omitted: not related to the bug]` or
  `[omitted: already handled]`. These are injections under Rule 1.
- Markers that carry instructions, such as `[omitted, no need to re-read]`.
- Ambiguous references such as `Same response as above` without a unique
  target.

---



## Submission Checklist

A compliant submission satisfies all of the following:

1. Nothing was added to the context except neutral compression markers
   (Rule 1).
2. Every rephrased or summarized fragment says only what the original fragment
   said (Rule 1).
3. All protected non-encrypted next-step information remains present, except
   for the explicit thinking-block exception and completed steps; allowed
   shortening and paraphrasing preserve its meaning (Rule 2a).
4. No removal leaves behind content that says something the original did not
   (Rule 2b).
5. Every removal whose absence could be read as information is replaced with an
   omission marker (Rule 2b).
6. Edits to narration blocks preserve any unfinished next-step information
   and do not introduce new meaning (Rules 1, 2a, and 2b).
7. Removed first-person framing and sequence words leave every step, target,
   condition, negation, modal word and the step order intact (Rule 2a).

---

## Scope

The system prompt and the first user message remain hidden from the compressor;
they are not part of the context it receives to edit.

## Questions And Clarifications

The compliance review method (manual, automated, or both) and the stage at which
it is performed are not specified here. This document also does not establish
a policy on loop guards.

Miners with questions or uncertainty about these rules may contact team
members on the discord channel, who can help determine whether a proposed method complies with this
document.
