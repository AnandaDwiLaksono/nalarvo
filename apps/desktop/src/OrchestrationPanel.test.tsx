import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";
const { invoke } = vi.hoisted(() => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/core", () => ({ invoke }));
import OrchestrationPanel from "./OrchestrationPanel";
afterEach(() => {
  cleanup();
  invoke.mockReset();
});
const props = {
  companyId: "c",
  projectId: "p",
  projectStatus: "ACTIVE",
  version: 7,
  refresh: vi.fn(async () => {}),
  workItems: [{ id: "w", title: "Delivery", status: "BLOCKED" }],
};
test("loading and errors never offer execution bypass", async () => {
  let reject!: (e: Error) => void;
  invoke.mockReturnValue(
    new Promise((_, r) => {
      reject = r;
    }),
  );
  render(<OrchestrationPanel {...props} />);
  expect(screen.getByRole("status")).toHaveTextContent("Loading orchestration");
  expect(
    screen.getByRole("button", { name: "Schedule ready work" }),
  ).toBeDisabled();
  reject(new Error("Core unavailable"));
  await screen.findByText("Error: Core unavailable");
  expect(
    screen.getByRole("button", { name: "Schedule ready work" }),
  ).toBeDisabled();
  expect(
    screen.queryByRole("button", { name: /force|bypass/i }),
  ).not.toBeInTheDocument();
});
test("shows work and staffing state; sends confirmed typed plan and completion commands", async () => {
  invoke.mockImplementation(async (cmd: string) =>
    cmd === "core_list_teams"
      ? { teams: [{ id: "t", name: "Delivery team", status: "ACTIVE" }] }
      : {
          type: "Staffing",
          result: {
            allocations: [],
            shortages: [
              { role_id: "qa", missing: 1, reason: "No eligible Agent" },
            ],
            team_active: false,
          },
        },
  );
  render(<OrchestrationPanel {...props} />);
  await screen.findByText("Delivery team · ACTIVE");
  expect(screen.getByText("Delivery · BLOCKED")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Match staffing" }));
  await screen.findByText(/qa · missing 1 · No eligible Agent/);
  fireEvent.change(screen.getByLabelText("Proposal ID"), {
    target: { value: "proposal" },
  });
  expect(
    screen.getByRole("button", { name: "Apply plan proposal" }),
  ).toBeDisabled();
  fireEvent.click(screen.getByLabelText("Confirm plan mutation"));
  fireEvent.click(screen.getByRole("button", { name: "Apply plan proposal" }));
  await waitFor(() =>
    expect(invoke).toHaveBeenCalledWith("core_orchestrate_project", {
      companyId: "c",
      projectId: "p",
      request: {
        team_id: "t",
        idempotency_key: expect.any(String),
        command: {
          type: "DecidePlan",
          proposal_id: "proposal",
          apply: true,
          confirmed: true,
          reason: "Owner confirmed project plan",
        },
      },
    }),
  );
  await waitFor(() =>
    expect(screen.getByLabelText("Confirm plan mutation")).not.toBeChecked(),
  );
  expect(
    screen.getByRole("button", { name: "Complete Project" }),
  ).toBeDisabled();
  fireEvent.click(screen.getByLabelText(/Confirm completion/));
  fireEvent.click(screen.getByRole("button", { name: "Complete Project" }));
  await waitFor(() =>
    expect(invoke).toHaveBeenCalledWith(
      "core_orchestrate_project",
      expect.objectContaining({
        request: expect.objectContaining({
          command: { type: "CompleteProject", expected_version: 7 },
        }),
      }),
    ),
  );
});
test("inactive project and disbanded team cannot dispatch; pending mutation disables repeats", async () => {
  invoke.mockResolvedValue({
    teams: [{ id: "t", name: "Old team", status: "DISBANDED" }],
  });
  render(<OrchestrationPanel {...props} projectStatus="COMPLETED" />);
  await screen.findByText("Old team · DISBANDED");
  expect(
    screen.getByRole("button", { name: "Schedule ready work" }),
  ).toBeDisabled();
  cleanup();
  invoke
    .mockResolvedValueOnce({
      teams: [{ id: "t", name: "Active team", status: "ACTIVE" }],
    })
    .mockReturnValue(new Promise(() => {}));

  render(<OrchestrationPanel {...props} />);
  await screen.findByText("Active team · ACTIVE");
  fireEvent.click(screen.getByRole("button", { name: "Schedule ready work" }));
  expect(
    screen.getByRole("button", { name: "Schedule ready work" }),
  ).toBeDisabled();
  expect(screen.getByRole("status")).toHaveTextContent(
    "Processing orchestration",
  );
});
