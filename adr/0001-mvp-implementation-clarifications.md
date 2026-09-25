# ADR-0001: Nalarvo MVP Implementation Clarifications

* **Status:** Approved
* **Date:** 2026-09-25
* **Author:** Lead Systems Architect / Core Lead
* **Scope:** Reconciles cross-document ambiguities across Nalarvo MVP documentation without altering canonical product thesis, domain invariants, authority models, or technical topology.

---

## 1. Physical Tier-1 Schema Baseline & Migration Sequence

* **Canonical Migration Baseline:** The authoritative physical MVP sequencing follows the canonical 14-migration sequence defined in `Nalarvo — MVP Scope & Implementation Plan.md`:
  * `0001_identity_workspace`
  * `0002_governance_policy`
  * `0003_work_tracking`
  * `0004_agent_configuration`
  * `0005_runtime_execution`
  * `0006_action_receipts`
  * `0007_budget_reservations`
  * `0008_security_audit`
  * `0009_evaluations_acceptance` (durable evaluation records & acceptance decisions prior to M7 completion)
  * `0010_knowledge_memory`
  * `0011_capabilities_tools`
  * `0012_system_projections`
  * `0013_checkpointing_recovery`
  * `0014_governance_v2_extensions`
* **Semantic Authority:** The Logical Data Model remains the semantic definition of what concepts exist. Before M1 expansion, every Tier-1 entity must be explicitly classified as `TABLE NOW`, `TABLE LATER IN MVP`, `EMBEDDED / VALUE OBJECT`, `DERIVED / PROJECTION`, or `EXPLICITLY DEFERRED`.
* **Resource Reservations & Budgeting:** Minimum durable budget tracking and atomic hard-budget reservation semantics are mapped as `TABLE NOW` / `EMBEDDED` in early milestones (M3/M4/M5). Complex multi-agent optimization and speculative capacity booking are `EXPLICITLY DEFERRED`.

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
