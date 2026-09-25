import { render, screen } from "@testing-library/react";
import { beforeEach, expect, test, vi } from "vitest";

const { invoke } = vi.hoisted(() => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/core", () => ({ invoke }));

import App from "./App";

beforeEach(() => invoke.mockReset());

test("shows authenticated Core health returned through the Tauri bridge", async () => {
  invoke.mockResolvedValue({ status: "ok", service: "nalarvo-core" });
  render(<App />);

  expect(await screen.findByText("Core healthy")).toBeInTheDocument();
  expect(invoke).toHaveBeenCalledWith("core_health");
});
