import { render, screen } from "@testing-library/react";
import { beforeEach, expect, test, vi } from "vitest";

const { invoke } = vi.hoisted(() => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/core", () => ({ invoke }));

import App from "./App";

beforeEach(() => invoke.mockReset());

test("shows authenticated Core health and lists companies", async () => {
  invoke.mockImplementation(async (cmd?: string) => {
    if (cmd === "core_health") {
      return { status: "ok", service: "nalarvo-core" };
    }
    if (cmd === "core_list_companies") {
      return {
        companies: [
          {
            id: "comp-1",
            name: "Nalarvo Corp",
            status: "DRAFT",
            row_version: 1,
          },
        ],
      };
    }
    return undefined;
  });

  render(<App />);

  expect(await screen.findByText("Core healthy")).toBeInTheDocument();
  expect(await screen.findByText("Nalarvo Corp")).toBeInTheDocument();
  expect(invoke).toHaveBeenCalledWith("core_health");
  expect(invoke).toHaveBeenCalledWith("core_list_companies", {
    workspaceId: "0191e4b8-0002-7000-8000-000000000001",
  });
});
