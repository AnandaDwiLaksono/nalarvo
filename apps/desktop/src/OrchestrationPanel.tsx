import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";

type Command =
  | { type: "StaffTeam" | "ScheduleWork" }
  | { type: "GenerateProjectPlan"; planner_role: string; reason: string }
  | {
      type: "DecidePlan";
      proposal_id: string;
      apply: boolean;
      confirmed: boolean;
      reason: string;
    }
  | { type: "CompleteProject"; expected_version: number };
type Staffing = {
  shortages: { role_id: string; missing: number; reason: string }[];
  team_active: boolean;
};
type Result =
  | { type: "Staffing"; result: Staffing }
  | { type: "Proposal"; result: string }
  | { type: "Runs" | "PlanApplied" | "Completed"; result: unknown };
export default function OrchestrationPanel({
  companyId,
  projectId,
  projectStatus,
  workItems,
  version,
  refresh,
}: {
  companyId: string;
  projectId: string;
  projectStatus: string;
  workItems: { id: string; title: string; status: string }[];
  version: number;
  refresh: () => Promise<void>;
}) {
  const [teams, setTeams] = useState<
    { id: string; name: string; status: string }[]
  >([]);
  const [teamId, setTeamId] = useState("");
  const [role, setRole] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [proposalId, setProposalId] = useState("");
  const [planConfirmed, setPlanConfirmed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [shortages, setShortages] = useState<
    { role_id: string; missing: number; reason: string }[]
  >([]);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  useEffect(() => {
    let current = true;
    setTeams([]);
    setTeamId("");
    setConfirmed(false);
    setMessage("");
    setLoading(true);
    setProposalId("");
    setPlanConfirmed(false);
    setShortages([]);
    invoke<{ teams: { id: string; name: string; status: string }[] }>(
      "core_list_teams",
      { companyId, projectId },
    )
      .then(({ teams: v }) => {
        if (current) {
          setTeams(v);
          setTeamId(v[0]?.id ?? "");
        }
      })
      .catch((e: unknown) => {
        if (current) setMessage(String(e));
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [companyId, projectId]);
  const team = teams.find((t) => t.id === teamId);
  const active =
    projectStatus === "ACTIVE" &&
    !!team &&
    ["FORMING", "ACTIVE", "PAUSED"].includes(team.status);
  async function send(command: Command) {
    if (
      busy ||
      loading ||
      !active ||
      (command.type === "ScheduleWork" && team?.status !== "ACTIVE") ||
      (command.type === "CompleteProject" && !confirmed) ||
      (command.type === "DecidePlan" && (!planConfirmed || !proposalId.trim()))
    )
      return;
    setBusy(true);
    setMessage("");
    try {
      const result = await invoke<Result>("core_orchestrate_project", {
        companyId,
        projectId,
        request: {
          team_id: teamId,
          idempotency_key: crypto.randomUUID(),
          command,
        },
      });
      if (result.type === "Staffing") {
        setShortages(result.result.shortages);
        setTeams((current) =>
          current.map((t) =>
            t.id === teamId
              ? {
                  ...t,
                  status: result.result.team_active ? "ACTIVE" : t.status,
                }
              : t,
          ),
        );
      }
      setMessage(JSON.stringify(result));
      setConfirmed(false);
      setPlanConfirmed(false);
      await refresh();
    } catch (e) {
      setMessage(String(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <fieldset disabled={busy || loading}>
      <legend>Project orchestration</legend>
      <label>
        Team{" "}
        <select
          value={teamId}
          onChange={(e) => {
            setTeamId(e.target.value);
            setConfirmed(false);
            setPlanConfirmed(false);
          }}
        >
          {teams.map((t) => (
            <option key={t.id} value={t.id}>
              {t.name} · {t.status}
            </option>
          ))}
        </select>
      </label>
      <button
        disabled={!active}
        onClick={() => void send({ type: "StaffTeam" })}
      >
        Match staffing
      </button>
      <button
        disabled={!active || team?.status !== "ACTIVE"}
        onClick={() => void send({ type: "ScheduleWork" })}
      >
        Schedule ready work
      </button>
      <ul aria-label="WorkItem state">
        {workItems.map((w) => (
          <li key={w.id}>
            {w.title} · {w.status}
          </li>
        ))}
      </ul>
      <ul aria-label="Staffing shortages">
        {shortages.map((s, i) => (
          <li key={i}>
            {s.role_id} · missing {s.missing} · {s.reason}
          </li>
        ))}
      </ul>
      <label>
        Planner role ID{" "}
        <input value={role} onChange={(e) => setRole(e.target.value)} />
      </label>
      <button
        disabled={!active || !role.trim()}
        onClick={() =>
          void send({
            type: "GenerateProjectPlan",
            planner_role: role,
            reason: "Owner requested project plan",
          })
        }
      >
        Generate plan proposal
      </button>
      <label>
        Proposal ID{" "}
        <input
          value={proposalId}
          onChange={(e) => {
            setProposalId(e.target.value);
            setPlanConfirmed(false);
          }}
        />
      </label>
      <label>
        <input
          type="checkbox"
          checked={planConfirmed}
          onChange={(e) => setPlanConfirmed(e.target.checked)}
        />
        Confirm plan mutation
      </label>
      <button
        disabled={!active || !proposalId.trim() || !planConfirmed}
        onClick={() =>
          void send({
            type: "DecidePlan",
            proposal_id: proposalId,
            apply: true,
            confirmed: planConfirmed,
            reason: "Owner confirmed project plan",
          })
        }
      >
        Apply plan proposal
      </button>
      <label>
        <input
          type="checkbox"
          checked={confirmed}
          onChange={(e) => setConfirmed(e.target.checked)}
        />
        Confirm completion; disband teams and release allocations. History
        retained.
      </label>
      <button
        disabled={!active || !confirmed}
        onClick={() =>
          void send({ type: "CompleteProject", expected_version: version })
        }
      >
        Complete Project
      </button>
      <p role="status">
        {loading
          ? "Loading orchestration"
          : busy
            ? "Processing orchestration"
            : message}
      </p>
    </fieldset>
  );
}
