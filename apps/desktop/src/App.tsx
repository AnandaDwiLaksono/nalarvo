import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import "./app.css";

type Health = { status: string; service: string };
type Workspace = { id: string; name: string; lifecycle_state: string };
type Provider = {
  id: string;
  name: string;
  endpoint: string;
  lifecycle_state: string;
  health_state: string;
  configured_model_ids: string[];
  row_version: number;
};
type Company = {
  id: string;
  name: string;
  mission?: string | null;
  status: string;
  row_version: number;
};
type Department = {
  id: string;
  name: string;
  lifecycle_state: string;
  row_version: number;
};
type Role = {
  id: string;
  name: string;
  description?: string | null;
  row_version: number;
};
type Agent = {
  id: string;
  name: string;
  description?: string | null;
  instructions?: string;
  lifecycle_state: string;
  department_id: string;
  role_id: string;
  model_profile_id?: string | null;
  max_active_allocations: number;
  availability?: string;
  row_version: number;
};
type Tab = "Departments" | "Roles" | "Agents";
const DEFAULT_WORKSPACE_ID = "0191e4b8-0002-7000-8000-000000000001";

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [workspace, setWorkspace] = useState<Workspace | null>(null);
  const [providers, setProviders] = useState<Provider[]>([]);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [departments, setDepartments] = useState<Department[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [selectedCompany, setSelectedCompany] = useState("");
  const [selectedAgent, setSelectedAgent] = useState("");
  const [tab, setTab] = useState<Tab>("Departments");
  const [companyName, setCompanyName] = useState("");
  const [providerName, setProviderName] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [modelIds, setModelIds] = useState("");
  const [departmentName, setDepartmentName] = useState("");
  const [roleName, setRoleName] = useState("");
  const [agentName, setAgentName] = useState("");
  const [agentDepartment, setAgentDepartment] = useState("");
  const [agentRole, setAgentRole] = useState("");
  const [agentInstructions, setAgentInstructions] = useState("");
  const [capacity, setCapacity] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const loadCompanies = async (workspaceId: string) => {
    const result = await invoke<{ companies: Company[] }>(
      "core_list_companies",
      {
        workspaceId,
      },
    );
    setCompanies(result.companies);
    setSelectedCompany((previous) =>
      result.companies.some((company) => company.id === previous)
        ? previous
        : (result.companies[0]?.id ?? ""),
    );
  };

  const loadProviders = async () => {
    const result = await invoke<{ providers: Provider[] }>(
      "core_list_providers",
    );
    setProviders(result.providers);
  };

  const loadWorkforce = async (companyId: string) => {
    if (!companyId) {
      setDepartments([]);
      setRoles([]);
      setAgents([]);
      return;
    }
    const [departmentList, roleList, agentList] = await Promise.all([
      invoke<{ departments: Department[] }>("core_list_departments", {
        companyId,
      }),
      invoke<{ roles: Role[] }>("core_list_roles", { companyId }),
      invoke<{ agents: Agent[] }>("core_list_agents", { companyId }),
    ]);
    setDepartments(departmentList.departments);
    setRoles(roleList.roles);
    setAgents(agentList.agents);
  };

  useEffect(() => {
    void (async () => {
      try {
        const core = await invoke<Health>("core_health");
        setHealth(core);
        const personal = await invoke<Workspace>("core_get_workspace");
        setWorkspace(personal);
        await Promise.all([
          loadCompanies(personal.id || DEFAULT_WORKSPACE_ID),
          loadProviders(),
        ]);
      } catch {
        setError("Core unavailable or configuration could not be loaded.");
      }
    })();
  }, []);

  useEffect(() => {
    if (!selectedCompany) return;
    void loadWorkforce(selectedCompany).catch(() => {
      setError("Workforce could not be loaded.");
    });
  }, [selectedCompany]);

  const mutate = async (work: () => Promise<void>) => {
    setBusy(true);
    setError("");
    try {
      await work();
    } catch {
      // Core owns detailed errors; never render a provider response that might echo a key.
      setError("Operation failed. Check the Core configuration and try again.");
    } finally {
      setBusy(false);
    }
  };

  const activeCompany = companies.find(
    (company) => company.id === selectedCompany,
  );
  const activeAgent = agents.find((agent) => agent.id === selectedAgent);

  return (
    <main>
      <header>
        <p className="eyebrow">Nalarvo</p>
        <h1>Workspace / Company / Workforce</h1>
        <p className={`status ${health ? "healthy" : "checking"}`}>
          {health ? "Core healthy" : "Checking Core…"}
        </p>
      </header>

      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {workspace && (
        <>
          <section aria-labelledby="workspace-title">
            <h2 id="workspace-title">Personal Workspace</h2>
            <p>{workspace.name}</p>
            <p>Lifecycle: {workspace.lifecycle_state}</p>
            <small>
              Platform configuration is separate from Company workforce.
            </small>
          </section>

          <section aria-labelledby="providers-title">
            <h2 id="providers-title">Provider settings</h2>
            <ul>
              {providers.map((provider) => (
                <li key={provider.id}>
                  <strong>{provider.name}</strong> · {provider.lifecycle_state}{" "}
                  · {provider.health_state}
                  <p>
                    {provider.endpoint} · Models:{" "}
                    {provider.configured_model_ids.join(", ") || "None"}
                  </p>
                  <button
                    disabled={busy}
                    onClick={() =>
                      void mutate(async () => {
                        await invoke("core_toggle_provider", {
                          providerId: provider.id,
                          action:
                            provider.lifecycle_state === "ENABLED"
                              ? "disable"
                              : "enable",
                          expectedVersion: provider.row_version,
                        });
                        await loadProviders();
                      })
                    }
                  >
                    {provider.lifecycle_state === "ENABLED"
                      ? "Disable"
                      : "Enable"}
                  </button>
                  <button
                    disabled={busy}
                    onClick={() =>
                      void mutate(async () => {
                        await invoke("core_test_provider", {
                          providerId: provider.id,
                        });
                        await loadProviders();
                      })
                    }
                  >
                    Test health
                  </button>
                </li>
              ))}
            </ul>
            <form
              onSubmit={(event) => {
                event.preventDefault();
                void mutate(async () => {
                  await invoke("core_create_provider", {
                    req: {
                      name: providerName.trim(),
                      endpoint: endpoint.trim(),
                      secret: apiKey,
                      model_ids: modelIds
                        .split(",")
                        .map((item) => item.trim())
                        .filter(Boolean),
                    },
                  });
                  setApiKey("");
                  setProviderName("");
                  setEndpoint("");
                  setModelIds("");
                  await loadProviders();
                });
              }}
            >
              <label>
                Provider name{" "}
                <input
                  required
                  value={providerName}
                  onChange={(event) => setProviderName(event.target.value)}
                />
              </label>
              <label>
                OpenAI-compatible endpoint{" "}
                <input
                  required
                  type="url"
                  value={endpoint}
                  onChange={(event) => setEndpoint(event.target.value)}
                />
              </label>
              <label>
                API key (write-only){" "}
                <input
                  required
                  type="password"
                  autoComplete="new-password"
                  value={apiKey}
                  onChange={(event) => setApiKey(event.target.value)}
                />
              </label>
              <label>
                Model IDs (comma-separated){" "}
                <input
                  value={modelIds}
                  onChange={(event) => setModelIds(event.target.value)}
                />
              </label>
              <button
                disabled={
                  busy || !providerName.trim() || !endpoint.trim() || !apiKey
                }
                type="submit"
              >
                Add provider
              </button>
            </form>
          </section>

          <section aria-labelledby="companies-title">
            <h2 id="companies-title">Companies</h2>
            <form
              onSubmit={(event) => {
                event.preventDefault();
                void mutate(async () => {
                  await invoke("core_create_company", {
                    workspaceId: workspace.id,
                    req: { name: companyName.trim(), description: null },
                  });
                  setCompanyName("");
                  await loadCompanies(workspace.id);
                });
              }}
            >
              <label>
                Company name{" "}
                <input
                  required
                  value={companyName}
                  onChange={(event) => setCompanyName(event.target.value)}
                />
              </label>
              <button disabled={busy || !companyName.trim()} type="submit">
                Create Company
              </button>
            </form>
            <label>
              Current Company{" "}
              <select
                value={selectedCompany}
                onChange={(event) => setSelectedCompany(event.target.value)}
              >
                {companies.map((company) => (
                  <option key={company.id} value={company.id}>
                    {company.name}
                  </option>
                ))}
              </select>
            </label>
            {activeCompany && (
              <>
                <p>
                  {activeCompany.name} · {activeCompany.status} · version{" "}
                  {activeCompany.row_version}
                </p>
                {activeCompany.mission && (
                  <p>Mission: {activeCompany.mission}</p>
                )}
                {(
                  {
                    DRAFT: "activate",
                    ACTIVE: "pause",
                    PAUSED: "resume",
                  } as Record<string, string>
                )[activeCompany.status] && (
                  <button
                    disabled={busy}
                    onClick={() =>
                      void mutate(async () => {
                        const action = (
                          {
                            DRAFT: "activate",
                            ACTIVE: "pause",
                            PAUSED: "resume",
                          } as Record<string, string>
                        )[activeCompany.status];
                        await invoke("core_company_lifecycle", {
                          companyId: activeCompany.id,
                          action,
                          expectedVersion: activeCompany.row_version,
                        });
                        await loadCompanies(workspace.id);
                      })
                    }
                  >
                    {
                      (
                        {
                          DRAFT: "Activate",
                          ACTIVE: "Pause",
                          PAUSED: "Resume",
                        } as Record<string, string>
                      )[activeCompany.status]
                    }
                  </button>
                )}
                {activeCompany.status !== "ARCHIVED" && (
                  <button
                    disabled={busy}
                    onClick={() =>
                      void mutate(async () => {
                        await invoke("core_company_lifecycle", {
                          companyId: activeCompany.id,
                          action: "archive",
                          expectedVersion: activeCompany.row_version,
                        });
                        await loadCompanies(workspace.id);
                      })
                    }
                  >
                    Archive
                  </button>
                )}
              </>
            )}
          </section>

          {activeCompany && (
            <section aria-labelledby="workforce-title">
              <h2 id="workforce-title">Workforce · {activeCompany.name}</h2>
              <nav aria-label="Workforce views">
                {(["Departments", "Roles", "Agents"] as const).map((view) => (
                  <button
                    key={view}
                    type="button"
                    aria-current={tab === view ? "page" : undefined}
                    onClick={() => setTab(view)}
                  >
                    {view}
                  </button>
                ))}
              </nav>
              {tab === "Departments" && (
                <>
                  <ul>
                    {departments.map((department) => (
                      <li key={department.id}>
                        {department.name} · {department.lifecycle_state}
                      </li>
                    ))}
                  </ul>
                  <form
                    onSubmit={(event) => {
                      event.preventDefault();
                      void mutate(async () => {
                        await invoke("core_create_department", {
                          companyId: activeCompany.id,
                          req: { name: departmentName.trim() },
                        });
                        setDepartmentName("");
                        await loadWorkforce(activeCompany.id);
                      });
                    }}
                  >
                    <label>
                      Department name{" "}
                      <input
                        required
                        value={departmentName}
                        onChange={(event) =>
                          setDepartmentName(event.target.value)
                        }
                      />
                    </label>
                    <button
                      type="submit"
                      disabled={busy || !departmentName.trim()}
                    >
                      Create Department
                    </button>
                  </form>
                </>
              )}
              {tab === "Roles" && (
                <>
                  <ul>
                    {roles.map((role) => (
                      <li key={role.id}>{role.name}</li>
                    ))}
                  </ul>
                  <form
                    onSubmit={(event) => {
                      event.preventDefault();
                      void mutate(async () => {
                        await invoke("core_create_role", {
                          companyId: activeCompany.id,
                          req: { name: roleName.trim() },
                        });
                        setRoleName("");
                        await loadWorkforce(activeCompany.id);
                      });
                    }}
                  >
                    <label>
                      Role name{" "}
                      <input
                        required
                        value={roleName}
                        onChange={(event) => setRoleName(event.target.value)}
                      />
                    </label>
                    <button type="submit" disabled={busy || !roleName.trim()}>
                      Create Role
                    </button>
                  </form>
                </>
              )}
              {tab === "Agents" && (
                <>
                  <ul>
                    {agents.map((agent) => (
                      <li key={agent.id}>
                        <button
                          type="button"
                          onClick={() => setSelectedAgent(agent.id)}
                        >
                          {agent.name}
                        </button>
                        {agent.lifecycle_state} ·{" "}
                        {agent.availability ?? "UNAVAILABLE"}
                      </li>
                    ))}
                  </ul>
                  {activeAgent && (
                    <article aria-label="Agent detail">
                      <h3>{activeAgent.name}</h3>
                      <p>Department: {activeAgent.department_id}</p>
                      <p>Role: {activeAgent.role_id}</p>
                      <p>Instructions: {activeAgent.instructions || "None"}</p>
                      <p>
                        ModelProfile: {activeAgent.model_profile_id || "None"}
                      </p>
                      <p>Lifecycle: {activeAgent.lifecycle_state}</p>
                      <p>Capacity: {activeAgent.max_active_allocations}</p>
                      <p>
                        Availability:{" "}
                        {activeAgent.availability ?? "UNAVAILABLE"}
                      </p>
                    </article>
                  )}
                  <form
                    onSubmit={(event) => {
                      event.preventDefault();
                      void mutate(async () => {
                        await invoke("core_create_agent", {
                          companyId: activeCompany.id,
                          req: {
                            name: agentName.trim(),
                            department_id: agentDepartment,
                            role_id: agentRole,
                            instructions: agentInstructions,
                            max_active_allocations: capacity,
                          },
                        });
                        setAgentName("");
                        setAgentInstructions("");
                        await loadWorkforce(activeCompany.id);
                      });
                    }}
                  >
                    <label>
                      Agent name{" "}
                      <input
                        required
                        value={agentName}
                        onChange={(event) => setAgentName(event.target.value)}
                      />
                    </label>
                    <label>
                      Primary Department{" "}
                      <select
                        required
                        value={agentDepartment}
                        onChange={(event) =>
                          setAgentDepartment(event.target.value)
                        }
                      >
                        <option value="">Choose</option>
                        {departments.map((department) => (
                          <option key={department.id} value={department.id}>
                            {department.name}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Role{" "}
                      <select
                        required
                        value={agentRole}
                        onChange={(event) => setAgentRole(event.target.value)}
                      >
                        <option value="">Choose</option>
                        {roles.map((role) => (
                          <option key={role.id} value={role.id}>
                            {role.name}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Instructions{" "}
                      <textarea
                        value={agentInstructions}
                        onChange={(event) =>
                          setAgentInstructions(event.target.value)
                        }
                      />
                    </label>
                    <label>
                      Capacity slots{" "}
                      <input
                        type="number"
                        min={1}
                        value={capacity}
                        onChange={(event) =>
                          setCapacity(Number(event.target.value))
                        }
                      />
                    </label>
                    <button
                      type="submit"
                      disabled={
                        busy ||
                        !agentName.trim() ||
                        !agentDepartment ||
                        !agentRole ||
                        !Number.isInteger(capacity) ||
                        capacity < 1
                      }
                    >
                      Create Agent
                    </button>
                  </form>
                </>
              )}
            </section>
          )}
        </>
      )}
    </main>
  );
}
