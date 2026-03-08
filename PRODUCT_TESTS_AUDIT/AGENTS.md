# Product Test Audit Execution Rules

This directory contains a long-running, documentation-only semantic fidelity audit.

Non-negotiable rules:
- No code changes are allowed while working on this audit.
- A method, environment, or suite may be marked `complete` only after the relevant legacy and current source bodies were manually read and compared.
- Structural evidence alone does not count as completion.

Stop rule:
- Do not stop work, report completion, or treat a checkpoint commit as a stopping point.
- Checkpoint commits are recovery-only.
- Continue until every lane and every suite tracked in `PROGRESS.md` is marked `complete`, unless a true blocker is recorded in `GAPS.md`.

Forbidden shortcuts:
- Do not promote older baseline prose to `complete` after a revalidation or spot-check pass.
- Do not treat suite wiring, environment wiring, inventory reconciliation, or tracker cleanup as a substitute for the
  lane's fresh one-method-at-a-time source-body pass.
- If a lane file says `revalidated`, that is not completion evidence by itself.

Allowed files:
- `README.md`
- `PROGRESS.md`
- `GAPS.md`
- `lanes/*.md`
- `suites/*.md`

Required workflow for each lane:
1. Compare every legacy method one at a time against its mapped current method(s).
   Continue immediately to step 2. Step 1 is not a stopping point.
2. Compare every environment used by the lane.
   Continue immediately to step 3. Step 2 is not a stopping point.
3. Compare every suite owned or extended by the lane.
   Continue immediately to step 4. Step 3 is not a stopping point.
4. Update the lane file.
   Continue immediately to step 5. Step 4 is not a stopping point.
5. Update suite files.
   Continue immediately to step 6. Step 5 is not a stopping point.
6. Update `PROGRESS.md`.
   Continue immediately to step 7. Step 6 is not a stopping point.
7. Update `GAPS.md` if needed.
   Continue immediately to step 8. Step 7 is not a stopping point.
8. Create an audit-only checkpoint commit and continue.
   After step 8, continue with the next incomplete lane. Step 8 is not a stopping point.

Lane completion checklist:
1. Every mapped legacy/current method comparison for the lane was freshly re-read from source in this final pass.
2. Every split/merged/current-only class in the lane was reviewed and documented.
3. Every environment and suite tied to the lane was re-read from source.
4. The lane note in `PROGRESS.md` describes the fresh source-body work that actually happened.
5. No status is marked `complete` just because older baseline prose already existed.

Completion gate:
- Every lane row in `PROGRESS.md` must show:
  - `method status = complete`
  - `environment status = complete`
  - `suite status = complete`
- Every suite row in `PROGRESS.md` must show:
  - `suite audit status = complete`
- `GAPS.md` must contain every unresolved fidelity issue and no stale resolved issue.

If any part of the completion gate is false, the audit is still in progress.
