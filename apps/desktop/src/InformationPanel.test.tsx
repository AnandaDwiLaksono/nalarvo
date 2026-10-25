import {
  fireEvent,
  render,
  screen,
  waitFor,
  cleanup,
} from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";
const { invoke } = vi.hoisted(() => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/core", () => ({ invoke }));
import InformationPanel from "./InformationPanel";
afterEach(() => {
  cleanup();
  invoke.mockReset();
});
test("loads every version page with a stable cursor", async () => {
  invoke.mockImplementation(
    async (command: string, args: { afterVersion?: number }) => {
      if (command === "core_list_artifacts")
        return [{ id: "a", title: "Report", artifact_type: "DOCUMENT" }];
      if (command === "core_list_knowledge") return [];
      if (command === "core_artifact_versions") {
        const start = args.afterVersion ?? 0;
        return Array.from({ length: start === 0 ? 100 : 5 }, (_, i) => ({
          id: `v${start + i + 1}`,
          version_no: start + i + 1,
          content_ref: { hash: "abc" },
          provenance: {},
        }));
      }
    },
  );
  render(<InformationPanel companyId="c" runs={[]} />);
  fireEvent.click(screen.getByRole("button", { name: "Refresh information" }));
  fireEvent.click(
    await screen.findByRole("button", { name: "Versions · Report" }),
  );
  await screen.findByText(/Version 100 ·/);
  fireEvent.click(screen.getByRole("button", { name: "More versions" }));
  await screen.findByText(/Version 105 ·/);
  expect(screen.getByText(/Version 1 ·/)).toBeInTheDocument();
  expect(invoke).toHaveBeenLastCalledWith("core_artifact_versions", {
    companyId: "c",
    artifactId: "a",
    afterVersion: 100,
  });
  expect(
    screen.queryByRole("button", { name: "More versions" }),
  ).not.toBeInTheDocument();
});
test("shows immutable versions, scoped search, snapshot references", async () => {
  invoke.mockImplementation(async (command: string) => {
    if (command === "core_list_artifacts")
      return [{ id: "a", title: "Report", artifact_type: "DOCUMENT" }];
    if (command === "core_artifact_versions")
      return [
        {
          id: "v1",
          version_no: 1,
          content_ref: { hash: "abc" },
          provenance: { run_id: "r" },
        },
        {
          id: "v2",
          version_no: 2,
          content_ref: { hash: "def" },
          provenance: { run_id: "r" },
        },
      ];
    if (command === "core_list_knowledge") return [];
    if (command === "core_search_information")
      return [
        {
          id: "k",
          title: "Guide",
          kind: "KNOWLEDGE",
          version_no: 2,
          lifecycle_state: "ACTIVE",
        },
      ];
    if (command === "core_context_snapshots")
      return [
        {
          id: "s",
          builder_version: "M6.1",
          prompt_hash: "hash",
          references: [{ kind: "KNOWLEDGE", version_id: "kv2" }],
        },
      ];
  });
  render(<InformationPanel companyId="c" projectId="p" runs={[{ id: "r" }]} />);
  fireEvent.click(screen.getByRole("button", { name: "Refresh information" }));
  fireEvent.click(
    await screen.findByRole("button", { name: "Versions · Report" }),
  );
  expect(await screen.findByText(/Version 1/)).toBeInTheDocument();
  expect(screen.getByText(/Version 2/)).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("Knowledge search"), {
    target: { value: "Guide" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Search" }));
  expect(await screen.findByText(/Guide · KNOWLEDGE/)).toBeInTheDocument();
  expect(invoke).toHaveBeenCalledWith("core_search_information", {
    companyId: "c",
    payload: { query: "Guide", project_id: "p", agent_id: null },
  });
  fireEvent.click(screen.getByRole("button", { name: "Context · r" }));
  await waitFor(() => expect(screen.getByText(/kv2/)).toBeInTheDocument());
});
