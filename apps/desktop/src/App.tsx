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
type Project = {
  id: string;
  company_id: string;
  name: string;
  description?: string | null;
  working_root_path?: string | null;
  working_root_bound_at?: string | null;
  status: string;
  row_version: number;
  created_at?: string;
  updated_at?: string;
};
type WorkItem = {
  id: string;
  company_id: string;
  project_id: string;
  objective_id?: string | null;
  parent_work_item_id?: string | null;
  title: string;
  description?: string | null;
  work_type: string;
  status: string;
  row_version: number;
  created_at?: string;
  updated_at?: string;
};
type WorkDependency = {
  id: string;
  depends_on_work_item_id: string;
  dependency_type: string;
};
type WorkAssignment = {
  id: string;
  agent_id: string;
  allocation_id: string;
  status: string;
  is_primary: boolean;
  row_version: number;
};
type Run = {
  id: string;
  work_item_id: string;
  executing_agent_id: string;
  lifecycle_state: string;
  attempt_number: number;
  created_at: string;
};
type Tab = "Departments" | "Roles" | "Agents" | "Projects" | "Work" | "Runs";

const DEFAULT_WORKSPACE_ID = "0191e4b8-0002-7000-8000-000000000001";

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [workspace, setWorkspace] = useState<Workspace | null>(null);
  const [providers, setProviders] = useState<Provider[]>([]);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [departments, setDepartments] = useState<Department[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [workItems, setWorkItems] = useState<WorkItem[]>([]);
  const [dependencies, setDependencies] = useState<WorkDependency[]>([]);
  const [assignments, setAssignments] = useState<WorkAssignment[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [selectedCompany, setSelectedCompany] = useState("");
  const [selectedAgent, setSelectedAgent] = useState("");
  const [selectedProject, setSelectedProject] = useState("");
  const [selectedWorkItem, setSelectedWorkItem] = useState("");
  const [tab, setTab] = useState<Tab>("Departments");
  const [projectName, setProjectName] = useState("");
  const [projectDescription, setProjectDescription] = useState("");
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
      setProjects([]);
      setWorkItems([]);
      return;
    }
    const [departmentList, roleList, agentList, projectList] =
      await Promise.all([
        invoke<{ departments: Department[] }>("core_list_departments", {
          companyId,
        }),
        invoke<{ roles: Role[] }>("core_list_roles", { companyId }),
        invoke<{ agents: Agent[] }>("core_list_agents", { companyId }),
        invoke<{ projects: Project[] }>("core_list_projects", { companyId }),
      ]);
    setDepartments(departmentList.departments);
    setRoles(roleList.roles);
    setAgents(agentList.agents);
    setProjects(projectList.projects);
    setSelectedProject((previous) =>
      projectList.projects.some((project) => project.id === previous)
        ? previous
        : (projectList.projects[0]?.id ?? ""),
    );
  };

  const loadWorkItems = async (companyId: string, projectId: string) => {
    if (!companyId || !projectId) {
      setWorkItems([]);
      setDependencies([]);
      setAssignments([]);
      return;
    }
    const result = await invoke<{ work_items: WorkItem[] }>(
      "core_list_work_items",
      {
        companyId,
        projectId,
      },
    );
    setWorkItems(result.work_items);
    setSelectedWorkItem((previous) =>
      result.work_items.some((item) => item.id === previous)
        ? previous
        : (result.work_items[0]?.id ?? ""),
    );
  };

  const loadWorkDetails = async (
    companyId: string,
    projectId: string,
    workId: string,
  ) => {
    if (!companyId || !projectId || !workId) {
      setDependencies([]);
      setAssignments([]);
      return;
    }
    const [depRes, assignRes] = await Promise.all([
      invoke<{ dependencies: WorkDependency[] }>("core_list_dependencies", {
        companyId,
        projectId,
        workId,
      }),
      invoke<{ assignments: WorkAssignment[] }>("core_list_assignments", {
        companyId,
        projectId,
        workId,
      }),
    ]);
    setDependencies(depRes.dependencies);
    setAssignments(assignRes.assignments);
  };

  const loadRuns = async (companyId: string, projectId: string) => {
    if (!companyId || !projectId) {
      setRuns([]);
      return;
    }
    const result = await invoke<{ runs: Run[] }>("core_list_runs", {
      companyId,
      projectId,
    });
    setRuns(result.runs);
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
    void loadWorkforce(selectedCompany).catch((cause) => {
      console.error("Workforce could not be loaded.", cause);
      setError("Workforce could not be loaded.");
    });
  }, [selectedCompany]);

  useEffect(() => {
    if (!selectedCompany || !selectedProject) return;
    void loadWorkItems(selectedCompany, selectedProject).catch(() => {
      setError("Work could not be loaded.");
    });
    void loadRuns(selectedCompany, selectedProject).catch(() => {
      // safe ignore
    });
  }, [selectedCompany, selectedProject]);

  useEffect(() => {
    if (!selectedCompany || !selectedProject || !selectedWorkItem) return;
    void loadWorkDetails(
      selectedCompany,
      selectedProject,
      selectedWorkItem,
    ).catch(() => {
      setError("Work details could not be loaded.");
    });
  }, [selectedCompany, selectedProject, selectedWorkItem]);

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
  const activeProject = projects.find(
    (project) => project.id === selectedProject,
  );
  const activeWorkItem = workItems.find((item) => item.id === selectedWorkItem);

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
                {(
                  [
                    "Departments",
                    "Roles",
                    "Agents",
                    "Projects",
                    "Work",
                    "Runs",
                  ] as const
                ).map((view) => (
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
              {tab === "Projects" && activeCompany && (
                <section aria-labelledby="projects-title">
                  <h2 id="projects-title">Projects · {activeCompany.name}</h2>
                  <ul>
                    {projects.map((project) => (
                      <li key={project.id}>
                        <button
                          type="button"
                          onClick={() => setSelectedProject(project.id)}
                        >
                          {project.name}
                        </button>
                        {" · "}
                        {project.status}
                      </li>
                    ))}
                  </ul>
                  {activeProject && (
                    <article aria-label="Project detail">
                      <h3>{activeProject.name}</h3>
                      <p>
                        {activeProject.status} · version{" "}
                        {activeProject.row_version}
                      </p>
                      {activeProject.description && (
                        <p>{activeProject.description}</p>
                      )}
                      {activeProject.status === "DRAFT" && (
                        <button
                          disabled={busy}
                          onClick={() =>
                            void mutate(async () => {
                              await invoke("core_activate_project", {
                                companyId: activeCompany.id,
                                projectId: activeProject.id,
                                expectedVersion: activeProject.row_version,
                              });
                              await loadWorkforce(activeCompany.id);
                            })
                          }
                        >
                          Activate Project
                        </button>
                      )}
                      <p>
                        Working Root:{" "}
                        {activeProject.working_root_path || "None"}
                      </p>
                      {activeProject.working_root_path ? (
                        <button
                          type="button"
                          disabled={busy}
                          onClick={() =>
                            void mutate(async () => {
                              await invoke("core_unbind_project_working_root", {
                                companyId: activeCompany.id,
                                projectId: activeProject.id,
                                req: {
                                  expected_version: activeProject.row_version,
                                },
                              });
                              await loadWorkforce(activeCompany.id);
                            })
                          }
                        >
                          Unbind Working Root
                        </button>
                      ) : (
                        <button
                          type="button"
                          disabled={busy}
                          onClick={() =>
                            void mutate(async () => {
                              const path = window.prompt(
                                "Enter absolute directory path:",
                              );
                              if (!path) return;
                              await invoke("core_bind_project_working_root", {
                                companyId: activeCompany.id,
                                projectId: activeProject.id,
                                req: {
                                  path,
                                  expected_version: activeProject.row_version,
                                },
                              });
                              await loadWorkforce(activeCompany.id);
                            })
                          }
                        >
                          Bind Working Root
                        </button>
                      )}
                    </article>
                  )}
                  <form
                    onSubmit={(event) => {
                      event.preventDefault();
                      void mutate(async () => {
                        await invoke("core_create_project", {
                          companyId: activeCompany.id,
                          req: {
                            name: projectName.trim(),
                            description: projectDescription.trim() || null,
                          },
                        });
                        setProjectName("");
                        setProjectDescription("");
                        await loadWorkforce(activeCompany.id);
                      });
                    }}
                  >
                    <label>
                      Project name{" "}
                      <input
                        required
                        value={projectName}
                        onChange={(event) => setProjectName(event.target.value)}
                      />
                    </label>
                    <label>
                      Description{" "}
                      <textarea
                        value={projectDescription}
                        onChange={(event) =>
                          setProjectDescription(event.target.value)
                        }
                      />
                    </label>
                    <button
                      type="submit"
                      disabled={busy || !projectName.trim()}
                    >
                      Create Project
                    </button>
                  </form>
                </section>
              )}
              {tab === "Work" && activeCompany && (
                <section aria-labelledby="work-title">
                  <h2 id="work-title">Work · {activeCompany.name}</h2>
                  <label>
                    Project{" "}
                    <select
                      value={selectedProject}
                      onChange={(event) =>
                        setSelectedProject(event.target.value)
                      }
                    >
                      {projects.map((project) => (
                        <option key={project.id} value={project.id}>
                          {project.name}
                        </option>
                      ))}
                    </select>
                  </label>
                  <ul>
                    {workItems.map((item) => (
                      <li key={item.id}>
                        <button
                          type="button"
                          onClick={() => setSelectedWorkItem(item.id)}
                        >
                          {item.title}
                        </button>
                        {" · "}
                        {item.status} · {item.work_type}
                      </li>
                    ))}
                  </ul>
                  {activeProject && activeWorkItem && (
                    <article aria-label="Work detail">
                      <h3>{activeWorkItem.title}</h3>
                      <p>Status: {activeWorkItem.status}</p>
                      <p>Type: {activeWorkItem.work_type}</p>
                      {activeWorkItem.description && (
                        <p>Description: {activeWorkItem.description}</p>
                      )}
                      <div>
                        <h4>Work Lifecycle</h4>
                        {activeWorkItem.status === "BACKLOG" && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() =>
                              void mutate(async () => {
                                await invoke("core_work_lifecycle", {
                                  companyId: activeCompany.id,
                                  projectId: activeProject.id,
                                  workId: activeWorkItem.id,
                                  action: "ready",
                                  expectedVersion: activeWorkItem.row_version,
                                });
                                await loadWorkItems(
                                  activeCompany.id,
                                  activeProject.id,
                                );
                              })
                            }
                          >
                            Mark Ready
                          </button>
                        )}
                        {activeWorkItem.status === "READY" && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() =>
                              void mutate(async () => {
                                await invoke("core_work_lifecycle", {
                                  companyId: activeCompany.id,
                                  projectId: activeProject.id,
                                  workId: activeWorkItem.id,
                                  action: "start",
                                  expectedVersion: activeWorkItem.row_version,
                                });
                                await loadWorkItems(
                                  activeCompany.id,
                                  activeProject.id,
                                );
                              })
                            }
                          >
                            Start Work
                          </button>
                        )}
                        {activeWorkItem.status === "IN_PROGRESS" && (
                          <>
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() =>
                                void mutate(async () => {
                                  await invoke("core_work_lifecycle", {
                                    companyId: activeCompany.id,
                                    projectId: activeProject.id,
                                    workId: activeWorkItem.id,
                                    action: "complete",
                                    expectedVersion: activeWorkItem.row_version,
                                  });
                                  await loadWorkItems(
                                    activeCompany.id,
                                    activeProject.id,
                                  );
                                })
                              }
                            >
                              Complete Work
                            </button>
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() =>
                                void mutate(async () => {
                                  await invoke("core_work_lifecycle", {
                                    companyId: activeCompany.id,
                                    projectId: activeProject.id,
                                    workId: activeWorkItem.id,
                                    action: "fail",
                                    expectedVersion: activeWorkItem.row_version,
                                  });
                                  await loadWorkItems(
                                    activeCompany.id,
                                    activeProject.id,
                                  );
                                })
                              }
                            >
                              Fail Work
                            </button>
                          </>
                        )}
                      </div>

                      <div>
                        <h4>Dependencies</h4>
                        <ul>
                          {dependencies.map((dep) => (
                            <li key={dep.id}>
                              {dep.dependency_type} on{" "}
                              {dep.depends_on_work_item_id}
                              <button
                                type="button"
                                disabled={busy}
                                onClick={() =>
                                  void mutate(async () => {
                                    await invoke("core_delete_dependency", {
                                      companyId: activeCompany.id,
                                      projectId: activeProject.id,
                                      workId: activeWorkItem.id,
                                      dependencyId: dep.id,
                                    });
                                    await loadWorkDetails(
                                      activeCompany.id,
                                      activeProject.id,
                                      activeWorkItem.id,
                                    );
                                  })
                                }
                              >
                                Remove Dependency
                              </button>
                            </li>
                          ))}
                        </ul>
                      </div>

                      <div>
                        <h4>Assignments & History</h4>
                        <ul>
                          {assignments.map((assign) => (
                            <li key={assign.id}>
                              Agent {assign.agent_id} ({assign.status})
                              {assign.status === "ACTIVE" && (
                                <button
                                  type="button"
                                  disabled={busy}
                                  onClick={() =>
                                    void mutate(async () => {
                                      const newAgentId = window.prompt(
                                        "Enter new Agent ID:",
                                      );
                                      if (!newAgentId) return;
                                      await invoke(
                                        "core_assignment_lifecycle",
                                        {
                                          companyId: activeCompany.id,
                                          projectId: activeProject.id,
                                          workId: activeWorkItem.id,
                                          assignmentId: assign.id,
                                          req: {
                                            expected_version:
                                              assign.row_version,
                                          },
                                        },
                                      );
                                      await invoke("core_create_assignment", {
                                        companyId: activeCompany.id,
                                        projectId: activeProject.id,
                                        workId: activeWorkItem.id,
                                        req: {
                                          agent_id: newAgentId,
                                          allocation_id: assign.allocation_id,
                                          is_primary: true,
                                        },
                                      });
                                      await loadWorkDetails(
                                        activeCompany.id,
                                        activeProject.id,
                                        activeWorkItem.id,
                                      );
                                    })
                                  }
                                >
                                  Reassign
                                </button>
                              )}
                            </li>
                          ))}
                        </ul>
                      </div>
                    </article>
                  )}
                </section>
              )}
              {tab === "Runs" && activeProject && (
                <section aria-labelledby="runs-title">
                  <h3 id="runs-title">Runs · {activeProject.name}</h3>
                  <ul>
                    {runs.map((run) => (
                      <li key={run.id}>
                        Run {run.id} · WorkItem {run.work_item_id} · Agent{" "}
                        {run.executing_agent_id} · State: {run.lifecycle_state}{" "}
                        (Attempt #{run.attempt_number})
                        {run.lifecycle_state === "QUEUED" && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() =>
                              void mutate(async () => {
                                await invoke("core_queue_run", {
                                  companyId: activeCompany.id,
                                  projectId: activeProject.id,
                                  runId: run.id,
                                  payload: { expected_version: 1 },
                                });
                                await loadRuns(
                                  activeCompany.id,
                                  activeProject.id,
                                );
                              })
                            }
                          >
                            Queue Run
                          </button>
                        )}
                        {(run.lifecycle_state === "QUEUED" ||
                          run.lifecycle_state === "RUNNING") && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() =>
                              void mutate(async () => {
                                await invoke("core_cancel_run", {
                                  companyId: activeCompany.id,
                                  projectId: activeProject.id,
                                  runId: run.id,
                                  payload: {
                                    expected_version: 1,
                                    reason: "Cancelled via Desktop",
                                  },
                                });
                                await loadRuns(
                                  activeCompany.id,
                                  activeProject.id,
                                );
                              })
                            }
                          >
                            Cancel Run
                          </button>
                        )}
                      </li>
                    ))}
                  </ul>
                </section>
              )}
            </section>
          )}
        </>
      )}
    </main>
  );
}
