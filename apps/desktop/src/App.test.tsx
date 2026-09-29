import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, expect, test, vi } from "vitest";

const { invoke } = vi.hoisted(() => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/core", () => ({ invoke }));

import App from "./App";

beforeEach(() => invoke.mockReset());

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
