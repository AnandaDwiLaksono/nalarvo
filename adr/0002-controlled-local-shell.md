# CONTROLLED_LOCAL shell execution

Operator configuration uses the daemon's existing Clap flag/environment mechanism:
`--shell-allowlist` or `NALARVO_SHELL_ALLOWLIST`. Value is a JSON array; default `[]`
means deny every command. Configuration is startup-only, not public CRUD.

Example value (Windows, trusted installed executable):

    [{"executable":"C:\\Windows\\System32\\whoami.exe","args":[],"timeout_ms":5000}]

Each rule permits precisely one absolute local-drive `.exe` and exact ordered argv.
No PATH lookup, glob, raw command line, argument prefix, batch-file handling, or
implicit cmd/PowerShell interpreter. Unknown fields, relative/UNC/device/ADS paths,
reparse points, NUL, excessive argv, missing image and timeout outside 1..30000ms
are rejected. Up to 128 rules, 64 arguments, 4096 bytes per argument; Windows total
command-line limit also applies. SHA-256 is pinned at configuration and checked at
invocation; executable/ancestor handles deny write/delete substitution during the
invocation. A changed executable requires operator reconfiguration. Its digest is
included in the Action fingerprint, so an approval cannot authorize a changed image.

The allowlist does not grant authority. Production worker IPC invokes Core dispatch;
Action, live permission/policy/approval, canonical Project binding, lease and budget
checks remain mandatory. Immediately before spawn, application uses BEGIN IMMEDIATE,
revalidates Action/authority/current lease and Project material, then creates the
contained process while cancellation/takeover writers are serialized. The running
Job guard survives transaction commit, failing closed on commit/future-drop errors.
DB cancellation, lease fencing/expiry, Project deactivation/root change and worker
cancellation signal terminate the job. DB polling errors fail closed.
Trusted supervisor heartbeat versions are synchronized with the live dispatch by a
read/write guard; heartbeat is not mistaken for fencing. Foreign DB version changes
remain rejected. Completion uses the current trusted lease version.

Windows executor creates an unnamed non-inheritable Job with KILL_ON_JOB_CLOSE and
no breakaway flags. CreateProcessW uses an explicit application path, CREATE_SUSPENDED,
CREATE_NO_WINDOW, a minimal UTF-16 environment (SystemRoot/WINDIR from Windows API),
NUL stdin and a HANDLE_LIST containing only stdout/stderr/stdin. AssignProcessToJobObject
must succeed before ResumeThread. Failed assignment terminates/waits the suspended
process; no process entrypoint runs. Both pipes are drained, retaining at most 64 KiB
per stream. Timeout, cancellation, error, future drop and successful parent exit all
terminate remaining Job members. Cleanup checks Job accounting and waits retained
process handles before acknowledging termination. No taskkill fallback.

Security boundary: CONTROLLED_LOCAL is process-lifetime containment, NOT an OS sandbox.
The executable, its dependencies/plugins, project working tree and exact arguments
must be trusted by the operator. Same-user processes retain OS file/network rights;
DLL/plugin loading, WMI/COM/service/task-scheduler brokers, remote execution and
malicious tools are not confined by Job membership. Do not allow tools/arguments
that delegate side effects to external services or load untrusted code. Supporting
hostile arbitrary programs requires a stronger security boundary, outside M5.
Already-completed side effects are not rolled back. Crash cleanup relies on last
Job-handle close; explicit timeout/cancel returns only after containment cleanup.
Non-Windows shell execution remains fail closed (no containment implementation).

Deterministic Windows tests use a compiled fixture, parent+descendant TCP readiness,
retained OS process handles and post-termination release of delayed writes. They
exercise production dispatch, real nalarvo-worker IPC, deny/approval, exact argv,
budget, Project scope, cancellation/fencing, bounded output and native Job assignment
rejection. Sleeps are not used as readiness or delayed-side-effect evidence.

References (Microsoft):
- https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects
- https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject
- https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-terminatejobobject
- https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-createprocessw
- https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-updateprocthreadattribute
