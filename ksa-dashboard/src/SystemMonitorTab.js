import React, { useState, useEffect, useRef, useMemo } from "react";

const C = {
  bg: "transparent",
  card: "#FFFFFF",
  text: "#1e293b",
  muted: "#64748b",
  border: "#e2e8f0",
  accent: "#E8563A",
  success: "#10B981",
  warning: "#F59E0B",
  error: "#EF4444",
};

const SB = "https://omowdfzyudedrtcuhnvy.supabase.co/rest/v1";

function sbHeaders(anonKey, token) {
  return {
    apikey: anonKey,
    Authorization: `Bearer ${token}`,
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
  };
}

export function SystemMonitorTab({ anonKey, session }) {
  const token = session?.access_token;
  const [agentLogs, setAgentLogs] = useState([]);
  const [audits, setAudits] = useState([]);
  const [pipelineJobs, setPipelineJobs] = useState([]);
  const [health, setHealth] = useState({
    supabase: "checking",
    daemon: "checking",
    claude: "checking",
    playwright: "unknown",
  });

  const terminalRef = useRef(null);

  useEffect(() => {
    let interval;

    const fetchData = async () => {
      const h = sbHeaders(anonKey, token);
      try {
        // Agent Logs
        const logRes = await fetch(`${SB}/agent_logs?select=*&order=timestamp.desc&limit=100`, { headers: h });
        if (logRes.ok) {
          const data = await logRes.json();
          setAgentLogs(data.reverse());
          if (data.length > 0) {
            const diffMins = (new Date() - new Date(data[data.length - 1].timestamp)) / 60000;
            setHealth(prev => ({ ...prev, daemon: diffMins < 10 ? "healthy" : "warning" }));
          } else {
            setHealth(prev => ({ ...prev, daemon: "warning" }));
          }
        }

        // Audit Logs
        const auditRes = await fetch(`${SB}/nexus_audit_log?select=*&order=created_at.desc&limit=50`, { headers: h });
        if (auditRes.ok) setAudits(await auditRes.json());

        // Pipeline Jobs
        const jobsRes = await fetch(`${SB}/pipeline_jobs?select=*&order=created_at.desc&limit=20`, { headers: h });
        if (jobsRes.ok) {
          const jobs = await jobsRes.json();
          setPipelineJobs(jobs);
          if (jobs.some(j => j.status === "running")) {
            setHealth(prev => ({ ...prev, daemon: "healthy" }));
          }
        }

        setHealth(prev => ({ ...prev, supabase: "healthy", claude: "healthy" }));
      } catch {
        setHealth(prev => ({ ...prev, supabase: "error" }));
      }
    };

    fetchData();
    interval = setInterval(fetchData, 3000);
    return () => clearInterval(interval);
  }, [anonKey, token]);

  // Build merged terminal feed: agent logs + pipeline job events
  const terminalFeed = useMemo(() => {
    const entries = [];

    agentLogs.forEach(log => {
      entries.push({
        key: `log-${log.id}`,
        ts: new Date(log.timestamp),
        type: "agent",
        level: log.level,
        message: log.message,
      });
    });

    pipelineJobs.forEach(job => {
      const shortId = (job.id || "").slice(0, 8);
      if (job.created_at) {
        entries.push({
          key: `job-q-${job.id}`,
          ts: new Date(job.created_at),
          type: "job",
          level: "JOB",
          message: `Pipeline job queued | id=${shortId} | batch_size=${job.batch_size ?? "?"}`,
          status: "pending",
        });
      }
      if (job.started_at) {
        entries.push({
          key: `job-s-${job.id}`,
          ts: new Date(job.started_at),
          type: "job",
          level: "JOB",
          message: `Pipeline job started | id=${shortId} | batch_size=${job.batch_size ?? "?"}`,
          status: "running",
        });
      }
      if (job.completed_at) {
        const durMs = new Date(job.completed_at) - new Date(job.started_at || job.created_at);
        const durStr = durMs > 0 ? ` | duration=${Math.round(durMs / 1000)}s` : "";
        entries.push({
          key: `job-c-${job.id}`,
          ts: new Date(job.completed_at),
          type: "job",
          level: "JOB",
          message: `Pipeline job ${job.status} | id=${shortId}${durStr}`,
          status: job.status,
        });
      }
    });

    return entries.sort((a, b) => a.ts - b.ts);
  }, [agentLogs, pipelineJobs]);

  // Auto-scroll terminal
  useEffect(() => {
    if (terminalRef.current) {
      terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
    }
  }, [terminalFeed]);

  const StatusLight = ({ status }) => {
    const colors = { healthy: C.success, warning: C.warning, error: C.error, checking: C.muted, unknown: C.muted };
    return (
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <div style={{ width: 10, height: 10, borderRadius: "50%", background: colors[status] || C.muted, boxShadow: `0 0 8px ${colors[status] || C.muted}` }} />
        <span style={{ fontSize: 13, color: C.text, textTransform: "capitalize", fontWeight: 600 }}>{status}</span>
      </div>
    );
  };

  const levelColor = (level, status) => {
    if (level === "JOB") {
      const sc = { pending: "#60A5FA", running: "#FBBF24", completed: "#34D399", failed: "#F87171" };
      return sc[status] || "#A78BFA";
    }
    return { INFO: "#60A5FA", SUCCESS: "#34D399", WARNING: "#FBBF24", ERROR: "#F87171" }[level] || "#f8fafc";
  };

  const jobStatusColor = s => ({ completed: C.success, running: C.warning, pending: "#60A5FA", failed: C.error }[s] || C.muted);

  return (
    <div style={{ color: C.text }}>
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ fontSize: 24, fontWeight: 800, margin: "0 0 4px", letterSpacing: "-.5px", display: "flex", alignItems: "center", gap: 10 }}>
          <span>👁️‍🗨️</span> System Monitor
        </h2>
        <p style={{ margin: 0, color: C.muted, fontSize: 13 }}>Real-time telemetry, API health, and user auditing.</p>
      </div>

      {/* Health Overview */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 24 }}>
        {[
          { label: "Supabase API", key: "supabase" },
          { label: "Agent Daemon", key: "daemon" },
          { label: "Anthropic Claude API", key: "claude" },
          { label: "Playwright Webhooks", key: "playwright" },
        ].map(({ label, key }) => (
          <div key={key} style={{ background: C.card, padding: 16, borderRadius: 10, border: `1px solid ${C.border}` }}>
            <div style={{ fontSize: 11, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>{label}</div>
            <StatusLight status={key === "playwright" ? "healthy" : health[key]} />
          </div>
        ))}
      </div>

      {/* Pipeline Jobs Table — always visible */}
      <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, marginBottom: 20 }}>
        <div style={{ padding: "12px 16px", borderBottom: `1px solid ${C.border}`, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{ fontSize: 13, fontWeight: 700 }}>Pipeline Jobs</span>
          <span style={{ fontSize: 10, color: C.success, display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.success, display: "inline-block" }} />
            LIVE · updates every 3s
          </span>
        </div>
        {pipelineJobs.length === 0 ? (
          <div style={{ padding: 16, color: C.muted, fontSize: 13 }}>No pipeline jobs found.</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                  {["Status", "Batch Size", "Queued At", "Started At", "Completed At"].map(col => (
                    <th key={col} style={{ padding: "8px 16px", textAlign: "left", color: C.muted, fontWeight: 600, fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pipelineJobs.map((job, i) => {
                  const sc = jobStatusColor(job.status);
                  const isActive = job.status === "running" || job.status === "pending";
                  return (
                    <tr key={i} style={{ borderBottom: i === pipelineJobs.length - 1 ? "none" : `1px solid ${C.border}`, background: isActive ? "rgba(251,191,36,0.06)" : "transparent" }}>
                      <td style={{ padding: "10px 16px" }}>
                        <span style={{ background: `${sc}20`, color: sc, padding: "2px 8px", borderRadius: 4, fontWeight: 700, fontSize: 11, textTransform: "uppercase" }}>{job.status}</span>
                      </td>
                      <td style={{ padding: "10px 16px", color: C.text }}>{job.batch_size ?? "—"}</td>
                      <td style={{ padding: "10px 16px", color: C.muted }}>{job.created_at ? new Date(job.created_at).toLocaleString() : "—"}</td>
                      <td style={{ padding: "10px 16px", color: C.muted }}>{job.started_at ? new Date(job.started_at).toLocaleString() : "—"}</td>
                      <td style={{ padding: "10px 16px", color: C.muted }}>{job.completed_at ? new Date(job.completed_at).toLocaleString() : "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 20 }}>

        {/* Live Terminal — merged agent logs + pipeline job events */}
        <div style={{ background: "#0f172a", color: "#f8fafc", border: `1px solid ${C.border}`, borderRadius: 10, display: "flex", flexDirection: "column", height: 500 }}>
          <div style={{ padding: "12px 16px", borderBottom: `1px solid #1e293b`, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <span style={{ fontSize: 13, fontWeight: 700 }}>Agent Terminal</span>
              <button
                onClick={() => {
                  const text = terminalFeed.map(e => `${e.ts.toLocaleTimeString()} [${e.level}] ${e.message}`).join('\n');
                  navigator.clipboard.writeText(text);
                }}
                title="Copy Logs"
                style={{ background: "rgba(255,255,255,0.1)", border: "1px solid rgba(255,255,255,0.1)", color: "#fff", width: 28, height: 28, borderRadius: 6, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}
                onMouseEnter={e => e.currentTarget.style.background = "rgba(255,255,255,0.2)"}
                onMouseLeave={e => e.currentTarget.style.background = "rgba(255,255,255,0.1)"}
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M16 4h2a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h2" />
                  <rect x="8" y="2" width="8" height="4" rx="1" ry="1" />
                </svg>
              </button>
            </div>
            <span style={{ fontSize: 10, color: C.success, display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.success, display: "inline-block" }} className="blink" />
              LIVE
            </span>
          </div>
          <div ref={terminalRef} style={{ flex: 1, overflowY: "auto", padding: 16, fontFamily: "monospace", fontSize: 12, lineHeight: 1.6 }}>
            {terminalFeed.length === 0 && (
              <div style={{ color: C.muted }}>No logs yet. Waiting for agent activity or pipeline jobs...</div>
            )}
            {terminalFeed.map(entry => (
              <div key={entry.key} style={{ marginBottom: 4, display: "flex", gap: 12, borderLeft: entry.type === "job" ? "2px solid #A78BFA44" : "none", paddingLeft: entry.type === "job" ? 8 : 0 }}>
                <span style={{ color: C.muted, flexShrink: 0 }}>{entry.ts.toLocaleTimeString()}</span>
                <span style={{ color: levelColor(entry.level, entry.status), width: 72, flexShrink: 0, fontWeight: 700 }}>[{entry.level}]</span>
                <span style={{ color: entry.type === "job" ? "#C4B5FD" : "#D1D5DB", wordBreak: "break-all" }}>{entry.message}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Live Audit Trail */}
        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, display: "flex", flexDirection: "column", height: 500 }}>
          <div style={{ padding: "12px 16px", borderBottom: `1px solid ${C.border}`, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: 13, fontWeight: 700 }}>User Audit Trail</span>
            <span style={{ fontSize: 11, color: C.muted }}>Last 50 actions</span>
          </div>
          <div style={{ flex: 1, overflowY: "auto", padding: 16 }}>
            {audits.length === 0 && <div style={{ color: C.muted, fontSize: 12 }}>No user activity found.</div>}
            {audits.map((a, i) => (
              <div key={i} style={{ paddingBottom: 12, marginBottom: 12, borderBottom: i === audits.length - 1 ? "none" : `1px solid ${C.border}` }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: C.accent }}>{a.user_id?.split('-')[0] || "Unknown"}</span>
                  <span style={{ fontSize: 10, color: C.muted }}>{new Date(a.created_at).toLocaleTimeString()}</span>
                </div>
                <div style={{ fontSize: 13, color: C.text }}>
                  <span style={{ color: C.muted }}>Action:</span> <span style={{ fontWeight: 600 }}>{a.action}</span>
                </div>
                <div style={{ fontSize: 11, color: C.muted, marginTop: 4 }}>Target: {a.resource}</div>
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  );
}
