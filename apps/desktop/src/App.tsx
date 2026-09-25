import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import "./app.css";

type Health = { status: "ok"; service: string };

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [unavailable, setUnavailable] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        setHealth(await invoke<Health>("core_health"));
      } catch {
        setUnavailable(true);
      }
    })();
  }, []);

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
    </main>
  );
}
