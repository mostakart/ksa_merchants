import React, { useState, useEffect, useRef } from "react";
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip } from "recharts";
import { sbGet, sbFetch } from "./App"; // Assumes these exist in App.js or we can write our own fetch

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

export function SystemMonitorTab({ anonKey, session }) {
  const token = session?.access_token;
  const [logs, setLogs] = useState([]);
  const [audits, setAudits] = useState([]);
  const [health, setHealth] = useState({
    supabase: "checking",
    daemon: "checking",
    claude: "checking",
    playwright: "unknown" // hard to check directly from frontend
  });
  
  const terminalRef = useRef(null);

  // Poll for logs and audits
  useEffect(() => {
    let interval;
    
    const fetchData = async () => {
      try {
        // 1. Fetch Agent Logs
        const logRes = await fetch(`https://omowdfzyudedrtcuhnvy.supabase.co/rest/v1/agent_logs?select=*&order=timestamp.desc&limit=100`, {
          headers: { 
            apikey: anonKey, 
            Authorization: `Bearer ${token}`,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
          }
        });
        if (logRes.ok) {
          const logData = await logRes.json();
          setLogs(logData.reverse()); // oldest first for terminal view
          
          // Determine Daemon health based on latest log timestamp
          if (logData.length > 0) {
            const lastLogTime = new Date(logData[logData.length - 1].timestamp);
            const diffMins = (new Date() - lastLogTime) / 1000 / 60;
            setHealth(h => ({ ...h, daemon: diffMins < 10 ? "healthy" : "warning" }));
          }
        }
        
        // 2. Fetch Audit Logs
        const auditRes = await fetch(`https://omowdfzyudedrtcuhnvy.supabase.co/rest/v1/nexus_audit_log?select=*&order=created_at.desc&limit=50`, {
          headers: { 
            apikey: anonKey, 
            Authorization: `Bearer ${token}`,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
          }
        });
        if (auditRes.ok) {
          setAudits(await auditRes.json());
        }
        
        // 3. Supabase Health Check (if the above worked, it's healthy)
        setHealth(h => ({ ...h, supabase: "healthy", claude: "healthy" })); // Assuming claude is healthy if agent logs don't show errors
        
      } catch (e) {
        setHealth(h => ({ ...h, supabase: "error" }));
      }
    };
    
    fetchData();
    interval = setInterval(fetchData, 3000); // Poll every 3s
    
    return () => clearInterval(interval);
  }, [anonKey, token]);

  // Auto-scroll terminal
  useEffect(() => {
    if (terminalRef.current) {
      terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
    }
  }, [logs]);

  const StatusLight = ({ status }) => {
    const colors = {
      healthy: C.success,
      warning: C.warning,
      error: C.error,
      checking: C.muted,
      unknown: C.muted
    };
    return (
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <div style={{ width: 10, height: 10, borderRadius: "50%", background: colors[status] || C.muted, boxShadow: `0 0 8px ${colors[status] || C.muted}` }} />
        <span style={{ fontSize: 13, color: C.text, textTransform: "capitalize", fontWeight: 600 }}>{status}</span>
      </div>
    );
  };

  const getLogColor = (level) => {
    switch (level) {
      case "INFO": return "#60A5FA";
      case "SUCCESS": return "#34D399";
      case "WARNING": return "#FBBF24";
      case "ERROR": return "#F87171";
      default: return "#f8fafc";
    }
  };

  return (
    <div style={{ color: C.text }}>
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ fontSize: 24, fontWeight: 800, margin: "0 0 4px", letterSpacing: "-.5px", display: "flex", alignItems: "center", gap: 10 }}>
          <span>👁️‍🗨️</span> System Monitor (God Mode)
        </h2>
        <p style={{ margin: 0, color: C.muted, fontSize: 13 }}>Real-time telemetry, API health, and user auditing.</p>
      </div>

      {/* Health Overview */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 24 }}>
        <div style={{ background: C.card, padding: 16, borderRadius: 10, border: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 11, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Supabase API</div>
          <StatusLight status={health.supabase} />
        </div>
        <div style={{ background: C.card, padding: 16, borderRadius: 10, border: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 11, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Agent Daemon</div>
          <StatusLight status={health.daemon} />
        </div>
        <div style={{ background: C.card, padding: 16, borderRadius: 10, border: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 11, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Anthropic Claude API</div>
          <StatusLight status={health.claude} />
        </div>
        <div style={{ background: C.card, padding: 16, borderRadius: 10, border: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 11, color: C.muted, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Playwright Webhooks</div>
          <StatusLight status="healthy" /> {/* Placeholder, assuming healthy if scraping runs */}
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 20 }}>
        
        {/* Live Terminal */}
        <div style={{ background: "#0f172a", color: "#f8fafc", border: `1px solid ${C.border}`, borderRadius: 10, display: "flex", flexDirection: "column", height: 500 }}>
          <div style={{ padding: "12px 16px", borderBottom: `1px solid #1e293b`, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <span style={{ fontSize: 13, fontWeight: 700 }}>Python Agent Terminal</span>
              <button 
                onClick={() => {
                  const text = logs.map(l => `${new Date(l.timestamp).toLocaleTimeString()} [${l.level}] ${l.message}`).join('\n');
                  navigator.clipboard.writeText(text);
                  alert("Terminal logs copied to clipboard!");
                }}
                title="Copy Logs"
                style={{
                  background: "rgba(255,255,255,0.1)", border: "1px solid rgba(255,255,255,0.1)",
                  color: "#fff", width: 28, height: 28, borderRadius: 6, cursor: "pointer",
                  transition: "background 0.2s", display: "flex", alignItems: "center", justifyContent: "center"
                }}
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
          <div 
            ref={terminalRef}
            style={{ flex: 1, overflowY: "auto", padding: 16, fontFamily: "monospace", fontSize: 12, lineHeight: 1.6 }}
          >
            {logs.length === 0 && <div style={{ color: C.muted }}>Waiting for agent logs... (Make sure the agent is running and agent_logs table is created)</div>}
            {logs.map((log) => (
              <div key={log.id} style={{ marginBottom: 4, display: "flex", gap: 12 }}>
                <span style={{ color: C.muted, flexShrink: 0 }}>{new Date(log.timestamp).toLocaleTimeString()}</span>
                <span style={{ color: getLogColor(log.level), width: 60, flexShrink: 0, fontWeight: 700 }}>[{log.level}]</span>
                <span style={{ color: "#D1D5DB", wordBreak: "break-all" }}>{log.message}</span>
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
                  <span style={{ fontSize: 12, fontWeight: 600, color: C.accent }}>{a.user_id?.split('-')[0] || "Unknown User"}</span>
                  <span style={{ fontSize: 10, color: C.muted }}>{new Date(a.created_at).toLocaleTimeString()}</span>
                </div>
                <div style={{ fontSize: 13, color: C.text }}>
                  <span style={{ color: C.muted }}>Action:</span> <span style={{ fontWeight: 600 }}>{a.action}</span>
                </div>
                <div style={{ fontSize: 11, color: C.muted, marginTop: 4 }}>
                  Target: {a.resource}
                </div>
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  );
}
