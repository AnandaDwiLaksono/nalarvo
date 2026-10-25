import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";
type Artifact = { id: string; title: string; artifact_type: string };
type Version = {
  id: string;
  version_no: number;
  content_ref: { hash: string };
  provenance: unknown;
};
type Information = {
  id: string;
  title: string;
  kind: string;
  version_no: number;
  lifecycle_state: string;
};
type Snapshot = {
  id: string;
  builder_version: string;
  prompt_hash: string;
  references: unknown;
};
export default function InformationPanel({
  companyId,
  projectId,
  runs,
}: {
  companyId: string;
  projectId?: string;
  runs: { id: string }[];
}) {
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [versions, setVersions] = useState<Version[]>([]);
  const [versionArtifact, setVersionArtifact] = useState("");
  const [moreVersions, setMoreVersions] = useState(false);
  const [knowledge, setKnowledge] = useState<Information[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [query, setQuery] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  async function load(action: () => Promise<void>) {
    setError("");
    setBusy(true);
    try {
      await action();
    } catch {
      setError("Information could not be loaded.");
    } finally {
      setBusy(false);
    }
  }
  return (
    <section aria-label="Information browser">
      <h3>Artifacts, Knowledge & Context</h3>
      <button
        disabled={busy}
        onClick={() =>
          void load(async () => {
            setVersions([]);
            setMoreVersions(false);
            setArtifacts(
              await invoke<Artifact[]>("core_list_artifacts", { companyId }),
            );
            setKnowledge(
              await invoke<Information[]>("core_list_knowledge", { companyId }),
            );
          })
        }
      >
        Refresh information
      </button>
      {artifacts.length === 0 && <p>No artifacts loaded.</p>}
      <ul>
        {artifacts.map((a) => (
          <li key={a.id}>
            {a.artifact_type}{" "}
            <button
              disabled={busy}
              onClick={() =>
                void load(async () => {
                  const page = await invoke<Version[]>(
                    "core_artifact_versions",
                    {
                      companyId,
                      artifactId: a.id,
                    },
                  );
                  setVersionArtifact(a.id);
                  setVersions(page);
                  setMoreVersions(page.length === 100);
                })
              }
            >
              Versions · {a.title}
            </button>
          </li>
        ))}
      </ul>
      <ul>
        {versions.map((v) => (
          <li key={v.id}>
            Version {v.version_no} · {v.id} · SHA-256 {v.content_ref.hash}
            <pre>{JSON.stringify(v.provenance, null, 2)}</pre>
          </li>
        ))}
      </ul>
      {moreVersions && (
        <button
          disabled={busy}
          onClick={() =>
            void load(async () => {
              const page = await invoke<Version[]>("core_artifact_versions", {
                companyId,
                artifactId: versionArtifact,
                afterVersion: versions.at(-1)?.version_no,
              });
              setVersions((current) => [...current, ...page]);
              setMoreVersions(page.length === 100);
            })
          }
        >
          More versions
        </button>
      )}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          void load(async () =>
            setKnowledge(
              await invoke<Information[]>("core_search_information", {
                companyId,
                payload: {
                  query,
                  project_id: projectId ?? null,
                  agent_id: null,
                },
              }),
            ),
          );
        }}
      >
        <label>
          Knowledge search
          <input
            value={query}
            maxLength={256}
            required
            onChange={(e) => setQuery(e.target.value)}
          />
        </label>
        <button disabled={busy}>Search</button>
      </form>
      <ul>
        {knowledge.map((k) => (
          <li key={k.id}>
            {k.title} · {k.kind} · Version {k.version_no} · {k.lifecycle_state}
          </li>
        ))}
      </ul>
      <h4>Context provenance</h4>
      {runs.map((r) => (
        <button
          key={r.id}
          disabled={busy}
          onClick={() =>
            void load(async () =>
              setSnapshots(
                await invoke<Snapshot[]>("core_context_snapshots", {
                  companyId,
                  runId: r.id,
                }),
              ),
            )
          }
        >
          Context · {r.id}
        </button>
      ))}
      {snapshots.map((s) => (
        <article key={s.id}>
          {s.builder_version} · {s.prompt_hash}
          <pre>{JSON.stringify(s.references, null, 2)}</pre>
        </article>
      ))}
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
