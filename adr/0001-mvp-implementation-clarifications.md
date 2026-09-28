# ADR-0001: Nalarvo MVP Implementation Clarifications

* **Status:** Approved
* **Date:** 2026-09-25
* **Author:** Lead Systems Architect / Core Lead
* **Scope:** Reconciles cross-document ambiguities across Nalarvo MVP documentation without altering canonical product thesis, domain invariants, authority models, or technical topology.

---

## 1. Physical Tier-1 Schema Baseline & Migration Reconciliation

* **Canonical Source Sequence:** `Nalarvo — MVP Scope & Implementation Plan.md` §31 retains its original 14 migration names and order: `0001_identity_workspace`, `0002_platform_resources`, `0003_company_workforce`, `0004_project_team`, `0005_work`, `0006_governance`, `0007_execution`, `0008_runtime_durability`, `0009_events_observability`, `0010_artifacts`, `0011_information`, `0012_information_fts`, `0013_capability_extension`, `0014_projections`. This source history is not rewritten.
* **MVP RECONCILED PHYSICAL MIGRATION PLAN:** The engineering mapping in `analysis/TIER1_SCHEMA_CHECKLIST.md` may introduce durable Core primitives earlier and split source groups into incremental physical migrations. It is a derived implementation plan, not the canonical source sequence. The checklist maps canonical source concept → reconciled physical migration → first required milestone; moving a table never removes its semantics.
* **Semantic Authority:** The Logical Data Model remains the semantic definition of concepts. Every Tier-1 concept is explicitly classified by physical strategy and first-required milestone; M1 creates only the minimal identity, Company, idempotency, event, outbox, and inbox slice.
* **Resource Reservations & Budgeting:** Minimum durable resource reservations and atomic hard-budget enforcement are required before budgeted execution. Advanced reservation optimization and speculative capacity booking are deferred.
* **M7 Gate:** `evaluation_records` and `acceptance_decisions` are durable before M7 completion and survive restart/replay. `Run SUCCEEDED ≠ WorkItem COMPLETED`.

---

## 2. Evaluation Records & Material Acceptance

* Durable entities `evaluation_records` and `acceptance_decisions` are first-class persistent tables required prior to completing Milestone 7 (Evaluation & Acceptance).
* **Invariant:** `Run SUCCEEDED ≠ WorkItem COMPLETED`. A completed Run produces execution evidence. An evaluation record scores or assesses output. An explicit acceptance decision (automated rule or authorized human) transitions the WorkItem / Objective outcome.
* Evaluation and acceptance decisions must never live solely in memory, logs, or UI state; they must persist across daemon restarts and preserve full audit lineage.

---

## 3. Run Admission Invariants & State Transitions

* When a `Run` has been durably created as `QUEUED` but fails a hard admission invariant (e.g. invalid context, revoked agent, broken policy constraint), it transitions:
  $$\text{QUEUED} \longrightarrow \text{FAILED}$$
  with `failure_class = ADMISSION_REJECTED`.
* No separate `REJECTED` lifecycle state is added to the Run state machine.
* Transient worker unavailability or queue backpressure does **not** reject admission; the Run remains `QUEUED` until picked up.

---

## 4. Run Cancellation Semantics

* The `Run` lifecycle explicitly supports:
  * $\text{QUEUED} \longrightarrow \text{CANCELLED}$
  * $\text{RUNNING} \longrightarrow \text{CANCELLED}$
  * $\text{WAITING\_APPROVAL} \longrightarrow \text{CANCELLED}$
  * $\text{WAITING\_DEPENDENCY} \longrightarrow \text{CANCELLED}$
  * $\text{PAUSED} \longrightarrow \text{CANCELLED}$
* **Non-Destructive Invariant:** Cancellation halts pending work but strictly preserves all previously recorded `Action`, `Artifact`, `Usage`, `AuditLog`, `Trace`, and side-effect receipts.

---

## 5. ApprovalRequest to Action State Mapping

* When resolving approval lifecycle into action authorization:
  * `ApprovalRequest.APPROVED` $\longrightarrow$ `Action.authorization = APPROVED`
  * `ApprovalRequest.REJECTED` $\longrightarrow$ `Action.authorization = REJECTED`
  * `ApprovalRequest.EXPIRED` $\longrightarrow$ `Action.authorization = CANCELLED`
  * `ApprovalRequest.CANCELLED` $\longrightarrow$ `Action.authorization = CANCELLED`
* Expiration represents lack of timely authorization, never human rejection. New authorization cycles require a fresh `ApprovalRequest` record.

---

## 6. Execution Failure Semantics: OUTCOME_UNKNOWN

* If an external action or tool execution disconnects or crashes mid-flight such that side-effect completion is indeterminate:
  * `Action.status = OUTCOME_UNKNOWN`
  * Blind automated retry is strictly prohibited.
  * The execution gate halts for manual operator resolution or an explicit deterministic reconciliation probe.

---

## 7. M0 Bootstrap Milestone Scope

* M0 is strictly focused on:
  1. Rust multi-crate workspace (`contracts`, `local-api`, `client`, `daemon`, `cli`).
  2. Local daemon loopback HTTP listener on `127.0.0.1` secured with generated Bearer token authentication.
  3. Thin Tauri + React desktop shell verifying authenticated Core connectivity (`/api/v1/health`).
  4. CI, linting, formatting, and test infrastructure.
* No domain business logic, M1 persistence migrations, or agent runtimes are introduced during M0.
