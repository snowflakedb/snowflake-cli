---
name: author-review-rule
description: >-
  Use when authoring, adding, or refining an arcticowl review rule — e.g.
  "add a rule for X", "create a rule that catches Y", "write a review rule",
  or "I want a rule for PR Z". Guides the full workflow: inspect existing rules,
  write/update the ruleset YAML, generate synthetic examples, run the model, and
  iterate until results are consistent.
---

# Authoring an arcticowl review rule

This skill walks through the full workflow for adding or improving a rule in
`.ai/review/`. Follow each phase in order; do not skip phases.

## Phase 0 — gather the requirement

You need:
1. **What the rule should enforce** — a plain-language description from the user.
2. **A concrete violation example** (optional) — a PR number/URL where the
   problem occurred. If given, read the diff to extract the bad pattern.

If the user gave a PR number/URL, fetch the diff now and identify the specific
lines that would have been flagged.

## Phase 1 — inspect existing rules

Before writing anything, read every ruleset file in `.ai/review/`:

```
find .ai/review -maxdepth 1 -name "*.yaml" | sort
```

For each file, read it. Then answer:
- Is this violation already covered by an existing rule?
  - If **yes and it would've caught the violation**: tell the user which rule
    covers it and why no new rule is needed. Stop here unless the user disagrees.
  - If **yes but the rule is too weak**: note the gap; you will strengthen the
    existing rule rather than create a new one.
  - If **no**: identify which ruleset file the new rule belongs in (by
    file-extension scope and domain), or create a new ruleset file.

## Phase 2 — write or update the rule

### Choosing the right ruleset file

From Phase 1 you already read every ruleset file. Pick the one whose
`allowed_file_extensions`, `allowed_folders`, and `friendly_name` best match
the files the new rule should apply to. If no existing file fits, create a new
`snowflake-cli-<domain>-rules.yaml` with appropriate scope fields:

```yaml
id: snowflake-cli-<domain>-rules
friendly_name: "Snowflake CLI - <Domain> Rules"
status: shadow
allowed_folders:
  - "<target directory, e.g. src/>"
excluded_folders: []
allowed_file_extensions:
  - ".<ext>"
rules: []
```

### Rule authoring checklist

Every rule entry **must** have:
- `id`: starts with `snowflake-cli-`, kebab-case, unique across all rulesets.
- `status: shadow` for brand-new rules (never `enabled` on first write).
- `rule:` block with all four sections below.

#### Rule block structure

```
**SCOPE** (include when the rule applies to a subset of files within the
ruleset's extension/folder filter — e.g. only `RELEASE-NOTES.md`, only files
under `src/`, only test files): One sentence stating exactly which files this
rule covers. If the ruleset filter already fully scopes the rule, omit this
section.

**OBJECTIVE**: One sentence — the concrete harm this rule prevents or the
invariant it enforces. No vague phrases ("ensure good practices", "be careful").

**REQUIREMENT**: Specific, actionable instructions with no ambiguity.
Name the exact functions, imports, or patterns to use or avoid. State
what to do when *touching* existing code that violates the rule
(opportunistic migration) if applicable.

❌ **BAD:**
<code block showing the exact pattern that triggers the rule>

✅ **GOOD:**
<code block showing the correct replacement, including any required imports>
```

**Writing tips:**
- The BAD example must be minimal — only lines that trigger the rule.
- The GOOD example must be complete enough to copy-paste, including imports.
- Avoid adjectives like "properly", "correctly", "appropriately" — describe the
  mechanic, not the quality.
- Do not add a rule that overlaps with an existing one; instead extend it.

## Phase 3 — generate synthetic test examples

Run the generate command scoped to the rule(s) you are working on:

```
sf ai rules test generate \
  --config .ai/review/<ruleset_file>.yaml \
  --rules <rule-id>,<rule-id>,...
```

This writes:
- `1_autogen.py` — code that **violates** the rules (should trigger comments).
- `2_autogen_noop.py` — code that is **correct** (must not trigger comments).

After it runs:

1. Merge any pre-existing hand-crafted examples back into the generated files —
   do not discard them.
2. Only remove an old example if the new rule explicitly invalidates it (i.e.,
   the old "good" pattern is now the bad pattern).
3. The generated `1_autogen.py` is often minimal (one violation). Enrich it
   with additional cases to get broader coverage.
4. **Do not add hint comments** (e.g. `# BAD`, `# violates rule`) — see Phase 5.

## Phase 4 — run the model and inspect results

```
sf ai rules test comments \
  --config .ai/review/<ruleset_file>.yaml \
  --rules <rule-id>,<rule-id>,... \
  --models claude-sonnet-4-6
```

This produces two files per rule under the test directory:
- `1_autogen.claude-sonnet-4-6.json` — comments on the violation file.
- `2_autogen_noop.claude-sonnet-4-6.json` — comments on the no-op file.

**Quality signals to check:**

| Signal | Meaning |
|--------|---------|
| `1_autogen` has a comment for every injected violation | Rule is specific enough to trigger |
| `2_autogen_noop` is `[]` | Rule doesn't fire on correct code (no false positives) |
| Comment `message` is actionable (names the fix, not just the problem) | Rule text is clear |
| `rule_id` in comments matches the rule you wrote | Correct attribution |

**Failure modes and fixes:**

| Symptom | Fix |
|---------|-----|
| `1_autogen` misses some violations | Rule REQUIREMENT is too vague — add more specifics |
| `2_autogen_noop` is non-empty | Rule triggers on valid code — tighten the BAD example or add a negative condition to the REQUIREMENT |
| Comments reference wrong rule_id | Check rule `id` field in YAML |
| Comments are vague ("consider using X") | Add explicit wording to REQUIREMENT: "raise X instead of Y" |

**Do not** make the test examples simpler to pass — fix the rule.

## Phase 5 — iterate for consistency

Repeat **Phase 4** two more times on the **same unchanged fixtures** (three
total runs). The goal is to detect model nondeterminism, so the input must stay
fixed across those three runs — do not re-run Phase 3 between them.

Goal: the comments in `1_autogen.*.json` should be **semantically consistent**
across all three runs — same line numbers, same rule_id, substantially the same
message. Minor wording variation is acceptable; a comment appearing in one run
but not another is not.

If results are inconsistent, the fix depends on the cause:
- **REQUIREMENT is ambiguous** — make it more precise, then go back to Phase 3
  to regenerate examples and restart the three Phase 4 runs from scratch.
- **BAD example is too subtle** — enrich the fixture with additional cases (Phase
  3), then restart the three Phase 4 runs. **Do not add hint comments**
  (e.g. `# BAD`, `# violates rule`) — file contents are sent verbatim to the
  model and hint comments artificially inflate detection, masking that the rule
  text is too weak.
- **Noop fires intermittently** — tighten the REQUIREMENT or add a negative
  condition, regenerate (Phase 3), and restart the three Phase 4 runs.

In all cases: three *consecutive* Phase 4 runs on the same fixtures must be
consistent before declaring the rule done.

## Phase 6 — backtest (optional)

If a PR number was provided in Phase 0, verify the rule fires on the real
violation by running a backtest against that PR:

```
sf ai review backtest --pr <PR number>
```

Check that the output contains a comment attributed to your rule on the lines
you identified in Phase 0. If the rule doesn't fire, the REQUIREMENT needs more
specificity — iterate on the rule text and re-run Phases 3–5 before retesting.

---

## Reference

- Ruleset files: `.ai/review/*.yaml`
- Test directories: `.ai/review/*-test/<rule-id>/`
- Generate command: `sf ai rules test generate --config .ai/review/<file>.yaml --rules <id>,...`
- Comment command: `sf ai rules test comments --config .ai/review/<file>.yaml --rules <id>,... --models claude-sonnet-4-6`
- Backtest command: `sf ai review backtest --pr <PR number>`
