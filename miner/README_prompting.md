# Miner Prompt Editing Rules

This document defines what prompt edits are allowed in miner submissions.

Goal: allow token optimizations while preserving behavioral equivalence as
defined below.

## Hard Rule

Compression changes are allowed when they reduce content and satisfy the
Behavioral-Equivalence Requirement below. They must preserve the agent's
immediate next steps and task outcome. Later execution steps may differ only
as permitted by that requirement. Edits must not cause the agent to stop early,
skip required work, or give up.

---

## 1) Compression Marker Definitions

Compression markers are optional wrappers around compressed prompt content.

Allowed marker behavior:

- Preserve the original instruction meaning, priority, conditions, and
  operational effect.
- Preserve all requirements, tool policy, safety policy, role policy, and
  output contract.
- Use clear, balanced, and unambiguous marker boundaries. Marker names and aliases may be chosen by the miner, but must be defined before reuse.
- Omission markers may use miner-defined wording, provided their boundaries and the omitted content they refer to remain unambiguous.
- When compressing code, include a source line reference inside the marker so omitted lines remain locatable (single line or an inclusive range).
- `Same response as in ...` references are allowed only when the referenced block is explicit and unambiguous.

Not allowed for compression:

- Changing or deleting instruction requirements.
- Adding new behavior constraints, strategy, or completion criteria.
- Changing instruction ordering when it changes priority or operational effect.
- Changing output format/schema requirements, tool policy, or safety policy.
- Ambiguous references such as `Same response as above` without a unique target.

---

## 2) Loop Detection Definitions

Loop detection may identify or record objective repeated/no-progress behavior,
but it must not terminate, fail, or otherwise end a run early.

Allowed loop guard behavior:

- Trigger only on objective repeated/no-progress conditions.
- Keep normal successful execution behavior unchanged.
- Do not alter scoring logic.
- Do not alter final-output criteria.
- A detected loop must not be used as a reason to stop the run, return early, fail the run, or skip remaining work.
- If loop detection emits a diagnostic log or metadata reason, it must use one of the permitted loop-detection reason strings.

Not allowed for loop guards:

- Prompt edits that change strategy, reasoning policy, or tool-use policy.
- Prompt edits that force shortcuts to reduce token usage.
- Any change that affects non-loop successful behavior.
- Terminating, failing, returning early, or skipping work because a loop was detected.

---

## 3) Allowed And Disallowed Changes

Prompt and context compression is allowed when it reduces content and
satisfies the Behavioral-Equivalence Requirement below.

### Allowed

- Shorten or rephrase prompt and context text when its meaning, priority,
  conditions, tool policy, safety policy, and output contract are unchanged.
- Change formatting, whitespace, headings, marker names, and reference syntax.
- Replace repeated static instruction text with an explicit, unambiguous
  reference.
- Edit, remove, summarize, or reorder messages, chain-of-thought content,
  tool calls, tool arguments, and tool results only when the resulting agent
  behavior satisfies the Behavioral-Equivalence Requirement below.

### Behavioral-Equivalence Requirement

Messages, chain-of-thought content, tool calls, tool arguments, and tool
results are not protected by an exact-string or whole-block rule. They may be
compressed, removed, or rewritten.

However, their modification must not change the agent's immediate next steps,
including its reasoning path, tool selection, tool-call arguments, or use of
tool results.

Compression may affect later execution steps when those differences result
from working with compressed context. For example, compressing file contents
may require the agent to read that file again later. Such downstream differences
are allowed, provided they do not change instruction meaning, priority,
conditions, task requirements, tool policy, safety policy, output contract,
completion criteria, or final response. They must not cause early stopping,
skipped required work, giving up, or failure to complete the task. This exception
does not permit changes to instruction requirements or the introduction of
new strategies or constraints.

### Disallowed

- Adding, removing, changing, or reordering instructions in a
  way that changes operational effect.
- Adding new strategy, reasoning, tool-use, safety, or completion constraints.
- Changing output schemas or contracts.
- Editing messages, chain of thought, tool calls, tool arguments, or tool
  results in a way that violates the Behavioral-Equivalence Requirement,
  including early stopping, skipped required work, changed immediate tool
  decisions, or changed completion behavior.

---

## 4) Submission Definition Checklist

A compliant submission satisfies all of the following:

1. Every shortened instruction preserves its original operational effect.
2. No instruction was added, removed, or reordered in a way that changes behavior.
3. Loop detection only targets repeated/no-progress cycles.
4. Non-loop successful behavior satisfies the Behavioral-Equivalence Requirement.
5. Output format contract is unchanged.
6. Safety and tool-use policies are unchanged.
7. Any edits to messages, chain of thought, tool calls, tool arguments, or tool
   results satisfy the Behavioral-Equivalence Requirement and do not cause
   early stopping.

---

## 5) Loop Detection Reason Strings

The following are the only allowed exact loop-detection reason strings:

- `loop_detected: repeated assistant response`
- `loop_detected: repeated tool call signature`

---

## 6) Questions And Clarifications

Miners with questions or uncertainty about these rules may contact team
members, who can help determine whether a proposed method complies with
this document.
