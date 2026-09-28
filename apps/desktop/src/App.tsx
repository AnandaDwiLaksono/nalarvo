import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import "./app.css";

type Health = { status: "ok"; service: string };
type Company = {
  id: string;
  name: string;
  status: string;
  row_version: number;
};
type CompanyList = { companies: Company[] };

const WORKSPACE_ID = "0191e4b8-0002-7000-8000-000000000001";

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [newName, setNewName] = useState("");
  const [creating, setCreating] = useState(false);

  const loadCompanies = async () => {
    try {
      const result = await invoke<CompanyList>("core_list_companies", {
        workspaceId: WORKSPACE_ID,
      });
      if (result && Array.isArray(result.companies)) {
        setCompanies(result.companies);
      }
    } catch {
      /* daemon may not be ready yet */
    }
  };

  useEffect(() => {
    void (async () => {
      try {
        setHealth(await invoke<Health>("core_health"));
        await loadCompanies();
      } catch {
        setUnavailable(true);
      }
    })();
  }, []);

  const handleCreate = async () => {
    if (!newName.trim()) return;
    setCreating(true);
    try {
      await invoke("core_create_company", {
        workspaceId: WORKSPACE_ID,
        req: { name: newName.trim(), description: null },
      });
      setNewName("");
      await loadCompanies();
    } catch (e) {
      alert(String(e));
    } finally {
      setCreating(false);
    }
  };

  return (
    <main>
      <section aria-live="polite">
        <p className="eyebrow">Nalarvo</p>
        <h1>AI Company Operating System</h1>
        <p
          className={`status ${health ? "healthy" : unavailable ? "error" : "checking"}`}
        >
          {health
            ? "Core healthy"
            : unavailable
              ? "Core unavailable"
              : "Checking Core…"}
        </p>
        {health && <small>{health.service}</small>}
      </section>

      {health && (
        <section>
          <h2>Companies</h2>
          <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1rem" }}>
            <input
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="Company name"
              onKeyDown={(e) => e.key === "Enter" && handleCreate()}
            />
            <button
              onClick={handleCreate}
              disabled={creating || !newName.trim()}
            >
              {creating ? "Creating…" : "Create"}
            </button>
          </div>
          {companies.length === 0 ? (
            <p>No companies yet.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Version</th>
                </tr>
              </thead>
              <tbody>
                {companies.map((c) => (
                  <tr key={c.id}>
                    <td>{c.name}</td>
                    <td>{c.status}</td>
                    <td>{c.row_version}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      )}
    </main>
  );
}
