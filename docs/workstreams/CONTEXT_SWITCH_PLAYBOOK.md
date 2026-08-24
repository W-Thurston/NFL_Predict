# CONTEXT_SWITCH_PLAYBOOK.md — How to move between threads without losing context

> Procedural companion to `AI_BOOTSTRAP.md`. Contains **no project state** — only
> the steps for each kind of context switch. Save at `docs/workstreams/` (cross-
> workstream), and link it from `AI_BOOTSTRAP.md`.
>
> **Golden rule:** the repository files are the memory. A switch is "clean" when the
> new thread can reconstruct everything it needs from the committed files — never
> from a prior transcript. Every switch below ends by *verifying* that reconstruction
> actually happened before any real work begins.

> **Sandbox note (current setup):** all governance files for this effort live under
> `docs/workstreams/quote_observations/` (not repo root). Wherever a step says
> "root `HANDOFF.md`" or "root `PLAN.md`," read it as the co-located file in that
> folder until/unless you promote to the live repo root (Switch G).

---

## The universal pattern (every switch follows this)

1. **Freeze** — make sure the files reflect reality before you leave the old thread.
2. **Stamp** — capture the context stamp (commit, revisions, active unit, scope).
3. **Attach** — give the new thread only the files its reading order needs.
4. **Orient** — paste the opening message; let the model read before acting.
5. **Verify orientation** — confirm the model reconstructed state *from the files*.
6. **Work** — proceed only after orientation passes.
7. **Close** — update the files the work changed; re-mirror; note the next switch.

If step 5 fails (the model asks for something the files should have told it), **stop
and patch the files** — that gap is a finding, not a nuisance.

---

## The context stamp (paste at the top of any new thread)
```
Inspected code commit:
Working tree:            clean / intentionally dirty
Root HANDOFF revision:
Workstream HANDOFF revision:
FINDINGS revision:
Active PLAN unit:
Scope of this exchange:
```
For implementation activity, add:
```
Files changed since workstream handoff:
Tests run:
Decisions added:
```

---

## Switch A — Open a new thread to IMPLEMENT the active PLAN unit
*Use when: files are committed and you're ready to build Unit 1 (or any active unit).*

**Prepare (in the repo, before opening the thread)**
1. `git status --short` → clean tree. Note the SHA (`git rev-parse HEAD`).
2. Confirm the active unit: `head PLAN.md` shows exactly one unit.
3. Confirm decisions: `grep -nE '^#+ D(28|29)\b' DECISIONS.md`.

**Attach** (from the workstream folder)
- `AI_BOOTSTRAP.md`, `HANDOFF.md`, `WS1_HANDOFF.md`, `PLAN.md`, `DECISIONS.md`.
- *Not* `FINDINGS_WS1.md` unless a finding gets challenged.

**Tier-2 tripwire**
- the model must acknowledge that touching a guardrail escalates to Switch F

**Opening message**
> Context is in the attached repository artifacts, not any prior chat. Read
> `AI_BOOTSTRAP.md` (method), then `HANDOFF.md`, then `WS1_HANDOFF.md`, then
> `PLAN.md`, then `DECISIONS.md`. Confirm the inspected-code commit and working-tree
> state before proposing changes. The one active unit is **Point-in-time quote
> evidence retrieval (Unit 1)**; D29 = `fetched_at` visibility, D28 = no auto-retry.
> Implement Unit 1 against its 28 acceptance criteria. Do not reopen closed decisions
> without new evidence.
> [paste context stamp]

**Verify orientation (must pass before coding)** — the model should, unprompted:
- State the active unit and its goal in its own words.
- Name the two constraints (D29 inclusive `fetched_at <= cutoff`; visibility ≠
  eligibility; D28 no auto-retry).
- Point to the two touch points (`as_known_at` near `store.py`;
  `issue_candidates_cmd` wiring) *without you telling it*.
If it does → proceed. If it invents scope or asks for something in the files → patch.

**Close (when the unit is done)**
1. Update `PLAN.md` in place to its completed form using the closure headings:
   **Completed · Goal · Files Added/Removed/Changed · Tests · Acceptance.**
2. Update `WS1_HANDOFF.md` + `HANDOFF.md` where durable state changed.
3. Update `CHANGELOG.md`; commit as one Conventional Commit unit.
4. Re-mirror to SharePoint. → then Switch D to activate the next unit.

---

## Switch B — Start a NEW workstream inspection (build a fresh FINDINGS list)
*Use when: WS1 is stable and you're inspecting the next area (e.g. analytical claims).*

**Prepare**
1. `mkdir docs/workstreams/<new_name>/`.
2. Copy the **empty structure**, not WS1's content: create `FINDINGS.md` (header +
   snapshot block only), `HANDOFF.md` (template), reference the shared
   `AI_BOOTSTRAP.md` + this playbook, and `VISION.md`. Do **not** copy WS1 findings.
3. Add a `<new_name>` entry to `ROADMAP.md` so the program index is current.

**Open the thread — this is an INSPECTION, not implementation.** Reuse the
8-boundary method:
> New workstream inspection: `<name>`. Context is in the attached files, not prior
> chat. Read `AI_BOOTSTRAP.md`, then `ROADMAP.md` (scope), then this workstream's
> empty `FINDINGS.md`. We are running the dual-model inspection method: Claude leads,
> Copilot reviews a single canonical `FINDINGS.md`; one boundary at a time; evidence
> labels enforced; version stamp on every handoff; no absence conclusion without
> `ABSENT_CONFIRMED`. First boundary: **Inventory & ownership** — index-independent,
> from the directory tree, no classifications yet.
> [paste context stamp with a NEW snapshot id, e.g. `<NAME>-SNAPSHOT-001`]

**Critical carry-overs from WS1 (state them in the opening so they're not re-derived):**
- Evidence-label set + the SharePoint `.txt`-mirror mangling caveat.
- Author/reviewer roles fixed within a boundary; whoever has byte-fidelity leads.
- One boundary → reviewer disposition → reconcile → close → next.
- Reuse map at the end (Boundary 8) → DECISIONS + one PLAN unit.

**Verify orientation:** the model should propose *Boundary 1 inventory only* and ask
for the directory tree / drop list — not jump to conclusions or implementation.

**Close:** each boundary closes into the canonical `FINDINGS.md`; the workstream ends
with a Boundary-8 consolidation, a new `HANDOFF.md`, DECISIONS entries, and one PLAN
unit — same as WS1. Then commit + mirror.

---

## Switch C — RESUME implementation mid-unit (thread died / you took a break)
*Use when: a unit is partially built and you're re-entering.*

**Prepare**
1. `git status --short` and `git log --oneline -5` → see what's actually on disk.
2. If work-in-progress is uncommitted, that's an **intentionally dirty** tree — say so.

**Open**
> Resuming an in-progress unit. Read `AI_BOOTSTRAP.md`, `HANDOFF.md`, `WS1_HANDOFF.md`,
> `PLAN.md`. Working tree is **intentionally dirty** — here is what's already done and
> what remains. Do not restart the unit; continue from the stated point.
> [context stamp + the "implementation activity" extension: Files changed / Tests run /
> Decisions added]

**Verify orientation:** the model restates *what's done vs. remaining* from your
stamp + the code, and does not re-propose completed steps.

**Close:** as Switch A.

*Prevention:* leave a one-line "resume point" at the bottom of `PLAN.md` whenever you
stop mid-unit (e.g. `RESUME: as_known_at implemented + tests 1–8 green; wiring
issue_candidates_cmd next`). Costs nothing; saves a full re-derivation.

---

## Switch D — Close a unit and ACTIVATE the next one
*Use when: the active unit is done and merged; the next unit is in ROADMAP.*

**Steps (usually same thread or a fresh one — either works)**
1. Confirm the finished unit's `PLAN.md` is in closed form (Switch A close).
2. Move the next unit's spec **from `ROADMAP.md` into `PLAN.md`** — `ROADMAP.md`
   loses it, `PLAN.md` gains it. Exactly one active unit at all times.
3. Update `HANDOFF.md` (active unit line) and `WS1_HANDOFF.md` (active
   implementation state).
4. If the next unit is blocked by a decision (e.g. Unit 4 blocked by D28 until a
   recovery policy exists), do **not** activate it — pick an unblocked unit or make
   the decision first (Switch F).
5. Commit + mirror. → then Switch A to implement.

---

## Switch E — Route output to the OTHER model (author ↔ reviewer)
*Use when: one thread produced a boundary/plan/draft and the other must review it.*

**Steps**
1. In the authoring thread, ensure the artifact (findings boundary, plan, draft) is
   written to its canonical file and **presented/saved** — not just in chat.
2. Carry to the reviewer thread: the **canonical file** + a one-line stamp:
   `Reviewing <file> rev N; snapshot <id>; scope <boundary/plan>.`
3. Reviewer responds only with the standard headers (Accepted / Accepted-with-mod /
   Rejected / Insufficient evidence / Missing targets / Classification changes /
   Local verification / Scope-control / Disposition).
4. Carry the review back **verbatim**; author reconciles into rev N+1.
5. **Never** let the two threads diverge on which revision is canonical — if a stamp
   mismatch appears, resync before continuing (Switch H).

*This is the one switch where you are the transport layer. The version stamp is what
keeps the two models from reasoning off different copies — the failure that bit us on
the stale ROADMAP mirror.*

---

## Switch F — AMEND a locked decision / VISION / CONSTITUTION
*Use when: new evidence genuinely warrants reopening something closed.*

**Steps**
1. State the **new evidence** explicitly — amendments require it; "I changed my mind"
   does not qualify.
2. Open (or continue) a thread with the affected canonical file attached, VISION.md, CONSTITUTION.md, and this framing:
   > Proposing an amendment to `<file>` `<Dnn / invariant>`. New evidence: `<…>`.
   > Record rationale, consequences, and rejected alternatives. Do not amend
   > incidentally — this is an explicit governance event.
3. Run it through the two-model loop (Switch E) — amendments are exactly when
   adversarial review matters most.
4. On agreement: edit the canonical file, add a superseding entry (don't silently
   overwrite — mark the old one Superseded with a pointer), bump revisions, commit,
   mirror.
5. Propagate: any HANDOFF/PLAN that cited the old state must be updated the same commit.

---

## Switch G — PROMOTE sandbox governance into the LIVE repo root
*Use when: a sandbox unit produces a real change to canonical `gridiron_edge` code.*

> This is the bridge between the `docs/workstreams/quote_observations/` sandbox and
> the live repo. Do it deliberately — it's where the two governance layers reconcile.

**Steps**
1. Verify the **live** root `DECISIONS.md` is the Gridiron Edge log:
   `head -n 80 DECISIONS.md` → must show **D27/D26/D25**, NOT `ADR-001…`
   ("AI Agents for Wealth Management" = wrong file — stop).
2. Insert the sandbox decisions (D28, D29) into the **live** `DECISIONS.md` above D27,
   renumbering only if the live head has advanced past D27.
3. Reconcile the live root `ROADMAP.md` / `PLAN.md` with what actually shipped —
   the sandbox `PLAN.md`/`ROADMAP.md` were parallel; the live ones are canonical for
   the running project.
4. Keep the sandbox as the *evidence archive* (FINDINGS, HANDOFFs); the live root as
   the *execution authority*. Cross-link them.
5. Commit + mirror both.

*Trigger check:* if code under `src/gridiron_edge/` changed, the governing decision
**must** exist in the live `DECISIONS.md`, not only the sandbox — otherwise the real
audit trail is incomplete.

---

## Switch H — RECOVER a desynced or stale thread
*Use when: a model is reasoning off an old revision, or two threads disagree on state.*

**Symptoms:** a review cites content you've already changed; a model references a
decision number that moved; conclusions contradict a closed boundary.

**Steps**
1. **Stop.** Do not reconcile off the stale copy — that propagates the error.
2. Re-issue the context stamp with the *current* commit + revisions.
3. Re-attach the **current** canonical file(s); explicitly say "the copy you reviewed
   was rev N−k; here is rev N."
4. Ask the model to re-read and restate the delta before continuing.
5. If the desync came from a lagging SharePoint mirror, **re-mirror first**, then
   proceed — never review against a mirror you know is behind.

*This is the exact failure from earlier (a reviewer reading a pre-lock ROADMAP). The
version stamp is the detector; re-mirroring is the fix.*

---

## Quick reference — which files to attach per switch
| Switch | Attach |
|---|---|
| A Implement active unit | AI_BOOTSTRAP · HANDOFF · WS1_HANDOFF · PLAN · DECISIONS |
| B New workstream inspection | AI_BOOTSTRAP · this playbook · ROADMAP · new empty FINDINGS · VISION.md|
| C Resume mid-unit | AI_BOOTSTRAP · HANDOFF · WS1_HANDOFF · PLAN (+ dirty-tree note) |
| D Activate next unit | ROADMAP · PLAN · HANDOFF (usually no model needed) |
| E Author↔reviewer | the single canonical file under review + stamp |
| F Amend decision/vision | the affected canonical file + new evidence · VISION.md/CONSTITUTION.md|
| G Promote to live root | live root DECISIONS/ROADMAP/PLAN + sandbox equivalents |
| H Recover desync | current canonical file(s) + fresh stamp |

## The one habit that prevents most breakage
End **every** working session by making the files true, then re-mirroring. A thread
can die at any moment; if the files are current, no context is ever trapped in a chat.
