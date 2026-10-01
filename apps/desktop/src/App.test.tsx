import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

const { invoke } = vi.hoisted(() => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/core", () => ({ invoke }));

import App from "./App";

beforeEach(() => invoke.mockReset());
afterEach(() => cleanup());

test("shows workspace provider settings and isolated workforce controls", async () => {
  invoke.mockImplementation(async (command?: string) => {
    if (command === "core_health")
      return { status: "ok", service: "nalarvo-core" };
    if (command === "core_get_workspace") {
      return {
        id: "workspace-1",
        name: "Personal Workspace",
        lifecycle_state: "ACTIVE",
      };
    }
    if (command === "core_list_providers") {
      return {
        providers: [
          {
            id: "provider-1",
            name: "Local OpenAI",
            endpoint: "http://127.0.0.1:48080",
            lifecycle_state: "ENABLED",
            health_state: "HEALTHY",
            configured_model_ids: ["test-model"],
          },
        ],
      };
    }
    if (command === "core_list_companies") {
      return {
        companies: [
          {
            id: "company-a",
            name: "Company A",
            status: "ACTIVE",
            row_version: 2,
            mission: "Build safely",
          },
        ],
      };
    }
    if (command === "core_list_departments")
      return {
        departments: [
          { id: "dept-a", name: "Engineering", lifecycle_state: "ACTIVE" },
        ],
      };
    if (command === "core_list_roles")
      return { roles: [{ id: "role-a", name: "Engineer" }] };
    if (command === "core_list_agents") {
      return {
        agents: [
          {
            id: "agent-a",
            name: "Nara",
            lifecycle_state: "ACTIVE",
            department_id: "dept-a",
            role_id: "role-a",
            max_active_allocations: 1,
            availability: "AVAILABLE",
          },
        ],
      };
    }
    if (command === "core_list_projects") return { projects: [] };
    if (command === "core_list_work_items") return { work_items: [] };
    return undefined;
  });

  render(<App />);

  expect(
    await screen.findByRole("heading", { name: "Personal Workspace" }),
  ).toBeInTheDocument();
  expect(await screen.findByText("Local OpenAI")).toBeInTheDocument();
  expect(await screen.findByText(/ENABLED · HEALTHY/)).toBeInTheDocument();
  expect(
    await screen.findByRole("heading", { name: "Workforce · Company A" }),
  ).toBeInTheDocument();
  expect(await screen.findByText(/Engineering · ACTIVE/)).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Agents" }));
  const nara = await screen.findByRole("button", { name: "Nara" });
  expect(nara).toBeInTheDocument();
  fireEvent.click(nara);
  expect(await screen.findByText("Role: role-a")).toBeInTheDocument();
  expect(invoke).toHaveBeenCalledWith("core_get_workspace");
  expect(invoke).toHaveBeenCalledWith("core_list_providers");
});

test("lists, creates, shows, and activates company projects with their work", async () => {
  invoke.mockImplementation(async (command?: string) => {
    if (command === "core_health")
      return { status: "ok", service: "nalarvo-core" };
    if (command === "core_get_workspace")
      return {
        id: "workspace-1",
        name: "Personal Workspace",
        lifecycle_state: "ACTIVE",
      };
    if (command === "core_list_providers") return { providers: [] };
    if (command === "core_list_companies")
      return {
        companies: [
          {
            id: "company-a",
            name: "Company A",
            status: "ACTIVE",
            row_version: 2,
          },
        ],
      };
    if (command === "core_list_departments") return { departments: [] };
    if (command === "core_list_roles") return { roles: [] };
    if (command === "core_list_agents") return { agents: [] };
    if (command === "core_list_projects")
      return {
        projects: [
          {
            id: "project-a",
            company_id: "company-a",
            name: "M3",
            description: "Ship projects",
            status: "DRAFT",
            row_version: 4,
          },
        ],
      };
    if (command === "core_list_work_items")
      return {
        work_items: [
          {
            id: "work-a",
            company_id: "company-a",
            project_id: "project-a",
            title: "Implement UI",
            description: "TDD",
            work_type: "TASK",
            status: "OPEN",
            row_version: 1,
          },
        ],
      };
    return undefined;
  });

  render(<App />);

  fireEvent.click(await screen.findByRole("button", { name: "Projects" }));
  expect(
    await screen.findByRole("heading", { name: "Projects · Company A" }),
  ).toBeInTheDocument();
  const project = await screen.findByRole("button", { name: "M3" });
  fireEvent.click(project);
  expect(await screen.findByText("Ship projects")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Work" }));
  expect(
    await screen.findByRole("heading", { name: "Work · Company A" }),
  ).toBeInTheDocument();
  const workItemBtn = await screen.findByRole("button", {
    name: "Implement UI",
  });
  expect(workItemBtn).toBeInTheDocument();
  fireEvent.click(workItemBtn);
  expect(await screen.findByText("Description: TDD")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Projects" }));

  const input = screen.getByLabelText("Project name");
  fireEvent.change(input, { target: { value: "New project" } });
  fireEvent.submit(
    screen.getByRole("button", { name: "Create Project" }).closest("form")!,
  );
  await waitFor(() => expect(input).toHaveValue(""));
  expect(invoke).toHaveBeenCalledWith("core_create_project", {
    companyId: "company-a",
    req: { name: "New project", description: null },
  });

  fireEvent.click(screen.getByRole("button", { name: "Activate Project" }));
  expect(invoke).toHaveBeenCalledWith("core_activate_project", {
    companyId: "company-a",
    projectId: "project-a",
    expectedVersion: 4,
  });
  expect(invoke).toHaveBeenCalledWith("core_list_work_items", {
    companyId: "company-a",
    projectId: "project-a",
  });
});
