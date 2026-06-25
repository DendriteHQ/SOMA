# Miner Prompt Editing Rules

This document defines what prompt edits are allowed in miner submissions.

Goal: allow token optimizations and loop safety without changing agent behavior.

## Hard Rule

Only these two categories are allowed:

1. Compression markers.
2. Loop detection guards.

Everything else is disallowed.

---

## 1) Compression Marker Definitions

Compression markers are metadata wrappers around existing prompt content.

Allowed marker behavior:

- Preserve the original instruction meaning exactly.
- Preserve instruction order.
- Preserve all requirements.
- Preserve tool policy, safety policy, role policy, and output contract.
- Use clear marker boundaries such as `Compressed text starts here` and `Compressed text ends here`.
- Marker aliases may be defined once and reused; `[[CMP]]` is the start marker and `[[/CMP]]` is the end marker.
- `Same response as in ...` references are allowed only when the referenced block is explicit and unambiguous.

Not allowed for markers:

- Rewriting, weakening, strengthening, or deleting instructions.
- Adding new behavior constraints.
- Changing output format/schema requirements.
- Ambiguous references such as `Same response as above` without a unique target.

---

## 2) Loop Detection Definitions

Loop detection is a runtime safety guard that terminates repeated no-progress behavior.

Allowed loop guard behavior:

- Trigger only on objective repeated/no-progress conditions.
- Keep normal successful execution behavior unchanged.
- Do not alter scoring logic.
- Do not alter final-output criteria.
- Fail fast with a clear loop reason when a loop is detected.

Not allowed for loop guards:

- Prompt edits that change strategy, reasoning policy, or tool-use policy.
- Prompt edits that force shortcuts to reduce token usage.
- Any change that affects non-loop successful behavior.

---

## 3) Global Disallowed Changes

The following are disallowed under this policy:

- Any prompt semantic change beyond marker metadata.
- Any new instruction that changes agent behavior.
- Any reordering or removal of policy blocks.
- Any schema/contract change for outputs.
- Any safety or tool-policy change.

---

## 4) Submission Definition Checklist

A compliant submission satisfies all of the following:

1. Prompt meaning is unchanged except metadata markers.
2. No instruction was added, removed, or reordered.
3. Loop detection only targets repeated/no-progress cycles.
4. Non-loop successful behavior is unchanged.
5. Output format contract is unchanged.
6. Safety and tool-use policies are unchanged.

---

## 5) Allowed Prompts

If miners want to use any prompt format or exact string outside this list, they should discuss it first on the public channel.

### 5.1 Markers

Allowed exact strings:

- `Compressed text starts here`
- `Compressed text ends here`
- `[[CMP]]`
- `[[/CMP]]`
- `[[BLOCK X]]`
- `[[/BLOCK X]]`
- `Same response as in [[BLOCK X]].`

### 5.2 Loop Detection

Allowed exact loop-detection reason strings:

- `loop_detected: repeated assistant response`
- `loop_detected: repeated tool call signature`
