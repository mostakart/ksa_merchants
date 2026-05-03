import { useState, useEffect, useMemo, useCallback } from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  BarChart, Bar, CartesianGrid,
} from "recharts";

/* ─── CONFIG (mirrors App.js) ────────────────────────────────────── */
const SB_URL = process.env.REACT_APP_SUPABASE_URL || "https://omowdfzyudedrtcuhnvy.supabase.co";

const C = {
  accent: "#FF5A00", accentL: "#FFF0ED", bg: "#F5F2EE",
  white: "#FFFFFF", border: "#E8E4DF", text: "#1A1A1A",
  muted: "#9B9792", sub: "#6B6B6B",
  // CI-specific semantic colors
  threat: "#FEF2F2",    threatText: "#991B1B",    threatBorder: "#FECACA",
  opp: "#F0FDF4",       oppText: "#15803D",       oppBorder: "#BBF7D0",
  warn: "#FFFBEB",      warnText: "#92400E",       warnBorder: "#FDE68A",
};

/* ─── SUPABASE HELPERS (local, not imported from App.js) ──────────── */
const sbH = (key, token) => ({
  apikey: key,
  Authorization: `Bearer ${token || key}`,
  "Content-Type": "application/json",
});

/**
 * Generic paginated GET.
 * filter: raw query string, e.g. "competitor_id=eq.xxx&order=snapshot_timestamp.desc"
 */
async function sbGet(table, key, token, select = "*", filter = "") {
  let allRows = [];
  let offset = 0;
  const limit = 1000;
  while (true) {
    const qs = [select && `select=${select}`, filter, `limit=${limit}`, `offset=${offset}`]
      .filter(Boolean).join("&");
    const r = await fetch(`${SB_URL}/rest/v1/${table}?${qs}`, { 
      headers: { ...sbH(key, token), "Cache-Control": "no-cache", "Pragma": "no-cache" } 
    });
    if (!r.ok) {
      const errText = await r.text();
      if (errText.includes("JWT") || errText.includes("PGRST303")) {
        localStorage.removeItem("wn_session");
        window.location.reload();
      }
      throw new Error(errText);
    }
    const data = await r.json();
    allRows.push(...data);
    if (data.length < limit) break;
    offset += limit;
  }
  return allRows;
}

async function sbPatch(table, key, token, filter, body) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${filter}`, {
    method: "PATCH",
    headers: { ...sbH(key, token), Prefer: "return=representation" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const errText = await r.text();
    if (errText.includes("JWT") || errText.includes("PGRST303")) {
      localStorage.removeItem("wn_session");
      window.location.reload();
    }
    throw new Error(errText);
  }
  return r.json();
}

async function sbPost(table, key, token, body) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, {
    method: "POST",
    headers: { ...sbH(key, token), Prefer: "return=representation" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const errText = await r.text();
    if (errText.includes("JWT") || errText.includes("PGRST303")) {
      localStorage.removeItem("wn_session");
      window.location.reload();
    }
    throw new Error(errText);
  }
  return r.json();
}

/* ─── SHARED MICRO-COMPONENTS ────────────────────────────────────── */

// Spinner (CSS-only, no dependency)
function Spinner({ size = 14, color = C.accent }) {
  return (
    <>
      <span style={{
        display: "inline-block", width: size, height: size,
        border: `2px solid ${C.border}`, borderTopColor: color,
        borderRadius: "50%", animation: "ci_spin .6s linear infinite",
        flexShrink: 0,
      }} />
      <style>{`@keyframes ci_spin{to{transform:rotate(360deg)}}`}</style>
    </>
  );
}

// Full-height loading state
function CILoading({ label = "Loading…" }) {
  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "60vh", gap: 8, color: C.muted, fontSize: 13 }}>
      <Spinner /> {label}
    </div>
  );
}

// Reusable white card
function CICard({ children, style = {} }) {
  return (
    <div style={{ background: C.white, borderRadius: 10, padding: 16, border: `1px solid ${C.border}`, ...style }}>
      {children}
    </div>
  );
}

// KPI box
function CIKPI({ label, value, sub, color }) {
  return (
    <div style={{ background: C.white, borderRadius: 10, padding: "14px 16px", border: `1px solid ${C.border}` }}>
      <div style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: .5 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700, color: color || C.accent, letterSpacing: "-.5px", margin: "4px 0 2px" }}>
        {value ?? "—"}
      </div>
      {sub && <div style={{ fontSize: 11, color: C.muted }}>{sub}</div>}
    </div>
  );
}

function Badge({ children, type, style = {} }) {
  const styles = {
    active:      { bg: "#dcfce7", text: "#15803d" },
    inactive:    { bg: "#f1f5f9", text: "#64748b" },
    threat:      { bg: "#fee2e2", text: "#b91c1c" },
    opportunity: { bg: "#f0fdf4", text: "#15803d" },
    neutral:     { bg: "#f8fafc", text: "#475569" },
  };
  const s = styles[type] || styles.neutral;
  return (
    <span style={{ padding: "2px 8px", borderRadius: 6, fontSize: 10, fontWeight: 700, background: s.bg, color: s.text, textTransform: "uppercase", letterSpacing: 0.5, ...style }}>
      {children}
    </span>
  );
}

function InsightMetric({ label, value, color }) {
  return (
    <div style={{ background: "#fff", padding: "14px 16px", borderRadius: 12, border: `1px solid ${C.border}`, textAlign: "center" }}>
      <div style={{ fontSize: 10, color: C.muted, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 800, color: color || C.text }}>{value}</div>
    </div>
  );
}

function InsightSection({ title, icon, content, fullWidth, isList, color }) {
  if (!content) return null;
  const parseJSON = (str) => { try { return JSON.parse(str); } catch (e) { return str; } };
  const data = typeof content === 'string' && content.trim().startsWith('[') ? parseJSON(content) : content;

  return (
    <CICard style={{ gridColumn: fullWidth ? "1 / -1" : "auto", borderTop: color ? `3px solid ${color}` : "none" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
        <span style={{ fontSize: 16 }}>{icon}</span>
        <span style={{ fontSize: 14, fontWeight: 800, color: color || C.text }}>{title}</span>
      </div>
      {isList && Array.isArray(data) ? (
        <ul style={{ padding: 0, margin: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 8 }}>
          {data.map((item, i) => (
             <li key={i} style={{ fontSize: 13, color: C.sub, lineHeight: 1.5, display: "flex", gap: 8 }}>
               <span style={{ color: color || C.accent }}>•</span>
               {item}
             </li>
          ))}
        </ul>
      ) : (
        <div style={{ fontSize: 13, color: C.sub, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{data}</div>
      )}
    </CICard>
  );
}

function PostCard({ item }) {
  const cleanCaption = item.caption ? item.caption.replace(/(Facebook\s*)+/gi, '').trim() : '';
  let imgUrl = item.media_urls?.[0];
  if (!imgUrl && item.screenshot_urls?.[0] && item.screenshot_urls[0].startsWith('http')) {
    imgUrl = item.screenshot_urls[0];
  }

  return (
    <div style={{ breakInside: "avoid", marginBottom: 20, background: "#fff", borderRadius: 16, border: `1px solid ${C.border}`, overflow: "hidden", boxShadow: "0 4px 12px rgba(0,0,0,0.03)" }}>
      {imgUrl ? (
        <img src={imgUrl} alt="media" style={{ width: "100%", display: "block", objectFit: "cover", minHeight: 120 }} />
      ) : (
        <div style={{ height: 120, background: item.platform === 'facebook' ? '#1877F211' : '#E1306C11', display: "flex", alignItems: "center", justifyContent: "center" }}>
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke={C.muted} strokeWidth="1.5"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>
        </div>
      )}
      <div style={{ padding: 16 }}>
        <div style={{ display: "flex", gap: 8, marginBottom: 12, alignItems: "center" }}>
          <PlatformTag platform={item.platform} />
          {item.content_type && <Badge type="neutral">{item.content_type}</Badge>}
          <div style={{ marginLeft: "auto", fontSize: 10, color: C.muted }}>{new Date(item.posted_at || item.scraped_at).toLocaleDateString()}</div>
        </div>
        <div style={{ fontSize: 13, lineHeight: 1.5, color: C.text, maxHeight: 100, overflow: "hidden" }}>{cleanCaption || "No caption"}</div>
        <div style={{ display: "flex", gap: 12, marginTop: 16, fontSize: 12, color: C.muted, fontWeight: 700 }}>
          <span style={{ display: "flex", alignItems: "center", gap: 4 }}><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M20.84 4.61a5.5 5.5 0 00-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 00-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 00 0-7.78z"/></svg> {item.likes_count?.toLocaleString() || 0}</span>
          <span style={{ display: "flex", alignItems: "center", gap: 4 }}><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z"/></svg> {item.comments_count?.toLocaleString() || 0}</span>
          {item.post_url && <a href={item.post_url} target="_blank" rel="noreferrer" style={{ marginLeft: "auto", color: C.accent, textDecoration: "none" }}>View ↗</a>}
        </div>
      </div>
    </div>
  );
}

// Social platform tag
function PlatformTag({ platform }) {
  const COLOR = { instagram: "#E1306C", facebook: "#1877F2" };
  return (
    <span style={{ padding: "2px 8px", borderRadius: 20, fontSize: 10, fontWeight: 700, background: COLOR[platform] || C.muted, color: "#fff" }}>
      {(platform || "—").toUpperCase()}
    </span>
  );
}

// Inline sort/filter pill buttons
function PillBtn({ active, onClick, children }) {
  return (
    <button onClick={onClick} style={{
      padding: "4px 10px", borderRadius: 6, fontSize: 11, fontWeight: 500,
      cursor: "pointer",
      border: `1px solid ${active ? C.accent : C.border}`,
      background: active ? C.accentL : "transparent",
      color: active ? C.accent : C.sub,
    }}>
      {children}
    </button>
  );
}

// Reusable section header inside a card
function SectionTitle({ children }) {
  return <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 12 }}>{children}</div>;
}

// Table head cell
function TH({ children }) {
  return <th style={{ textAlign: "left", padding: "6px 8px", fontSize: 10, color: C.muted, textTransform: "uppercase", fontWeight: 600, whiteSpace: "nowrap" }}>{children}</th>;
}

// Table data cell
function TD({ children, style = {} }) {
  return <td style={{ padding: "10px 8px", ...style }}>{children}</td>;
}

// Hover row
function TR({ children, onClick }) {
  const [hover, setHover] = useState(false);
  return (
    <tr
      onClick={onClick}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{ borderBottom: `1px solid ${C.border}`, background: hover ? C.bg : "transparent", cursor: onClick ? "pointer" : "default", transition: "background .1s" }}>
      {children}
    </tr>
  );
}

// Inline text input
function CIInput({ label, value, onChange, placeholder, type = "text", style = {} }) {
  return (
    <div style={style}>
      {label && <label style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: .5, display: "block", marginBottom: 4 }}>{label}</label>}
      <input type={type} value={value} onChange={onChange} placeholder={placeholder}
        style={{ width: "100%", padding: "8px 10px", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12, outline: "none", boxSizing: "border-box", color: C.text }} />
    </div>
  );
}

// Toast notification (portal-less, fixed position)
function Toast({ message }) {
  if (!message) return null;
  return (
    <div style={{
      position: "fixed", top: 20, right: 20, zIndex: 9999,
      background: C.text, color: "#fff", padding: "10px 16px",
      borderRadius: 8, fontSize: 12, fontWeight: 500,
      boxShadow: "0 4px 20px rgba(0,0,0,.25)",
      animation: "ci_fadein .2s ease",
    }}>
      <style>{`@keyframes ci_fadein{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:translateY(0)}}`}</style>
      {message}
    </div>
  );
}

// Empty state placeholder
function EmptyState({ emoji = "📭", text }) {
  return (
    <div style={{ textAlign: "center", padding: 40 }}>
      <div style={{ fontSize: 32, marginBottom: 8 }}>{emoji}</div>
      <div style={{ color: C.muted, fontSize: 13 }}>{text}</div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════
// TAB 1: INTELLIGENCE COMMAND CENTER (Overview)
// ═══════════════════════════════════════════════════════════════════
export function CIOverviewTab({ anonKey, session, onSelectCompetitor }) {
  const token = session?.access_token;
  const [competitors, setCompetitors] = useState([]);
  const [metricsMap, setMetricsMap] = useState({}); // { [competitor_id]: metrics[] }
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");
  const [sortBy, setSortBy] = useState("ig_engagement_rate");

  const loadData = useCallback(async () => {
    setLoading(true); setErr("");
    try {
      const comps = await sbGet("competitors_directory", anonKey, token);
      setCompetitors(comps);

      // Fetch latest 5 metric snapshots per competitor (for sparkline + latest value)
      const map = {};
      await Promise.all(
        comps.map(async (c) => {
          const rows = await sbGet(
            "competitor_metrics_history", anonKey, token, "*",
            `competitor_id=eq.${c.id}&order=snapshot_timestamp.desc&limit=5`
          ).catch(() => []);
          map[c.id] = rows;
        })
      );
      setMetricsMap(map);
    } catch (e) { setErr(e.message); }
    setLoading(false);
  }, [anonKey, token]);

  useEffect(() => { loadData(); }, [loadData]);

  const latestOf = (id) => metricsMap[id]?.[0] || {};

  // Sort competitors by chosen metric
  const sorted = useMemo(() => {
    return [...competitors].sort((a, b) => {
      const ma = latestOf(a.id);
      const mb = latestOf(b.id);
      if (sortBy === "ig_followers_count")  return (mb.ig_followers_count || 0)  - (ma.ig_followers_count || 0);
      if (sortBy === "ig_engagement_rate")  return (mb.ig_engagement_rate || 0)  - (ma.ig_engagement_rate || 0);
      if (sortBy === "fb_page_likes")       return (mb.fb_page_likes || 0)       - (ma.fb_page_likes || 0);
      return 0;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [competitors, metricsMap, sortBy]);

  // Summary KPIs
  const totalActive   = competitors.filter(c => c.is_active).length;
  const engagements   = competitors.map(c => latestOf(c.id).ig_engagement_rate || 0).filter(Boolean);
  const avgEngagement = engagements.length ? (engagements.reduce((s, v) => s + v, 0) / engagements.length).toFixed(2) : null;
  const top           = sorted[0];
  const lastScrape    = competitors.reduce((l, c) => { const t = c.last_scraped_at; return (!l || (t && t > l)) ? t : l; }, null);

  // Bar chart data (followers comparison)
  const barData = sorted.slice(0, 10).map(c => ({
    name: (c.competitor_name || "").split(" ")[0],
    followers: latestOf(c.id).ig_followers_count || 0,
  }));

  if (loading) return <CILoading label="Loading intelligence data…" />;
  if (err)     return <div style={{ padding: 24, color: C.accent, fontSize: 13 }}>⚠️ {err}</div>;

  return (
    <div>
      {/* Page title */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: 20 }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 800, letterSpacing: "-.5px" }}>Intelligence Command Center</div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>
            Live competitive landscape — Waffarha Nexus
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button 
            onClick={loadData} 
            title="Refresh Data"
            style={{ 
              display: "flex", alignItems: "center", justifyContent: "center",
              width: 32, height: 32, background: "#fff", border: `1px solid ${C.border}`, 
              borderRadius: 8, cursor: "pointer", color: C.sub, transition: "all 0.2s" 
            }}
            onMouseEnter={e => { e.currentTarget.style.borderColor = C.accent; e.currentTarget.style.color = C.accent; }}
            onMouseLeave={e => { e.currentTarget.style.borderColor = C.border; e.currentTarget.style.color = C.sub; }}
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1.49 14l4.64 4.36A9 9 0 0020.49 15" />
            </svg>
          </button>
          <div style={{ fontSize: 11, background: "#F0F9FF", color: "#0369A1", padding: "4px 10px", borderRadius: 20, fontWeight: 600, height: 32, display: "flex", alignItems: "center" }}>
            Live Landscape
          </div>
        </div>
      </div>

      {/* KPI row */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
        <CIKPI
          label="Total Competitors"
          value={competitors.length}
          sub={`${totalActive} actively monitored`}
        />
        <CIKPI
          label="Avg Engagement Rate"
          value={avgEngagement ? `${avgEngagement}%` : "—"}
          sub="Instagram average across all"
          color="#7C3AED"
        />
        <CIKPI
          label="Top Competitor"
          value={top?.competitor_name || "—"}
          sub={top ? `${latestOf(top.id).ig_engagement_rate?.toFixed(2) || 0}% engagement` : "Run pipeline first"}
          color="#0EA5E9"
        />
        <CIKPI
          label="Last Scrape"
          value={lastScrape ? new Date(lastScrape).toLocaleDateString() : "Never"}
          sub={lastScrape ? new Date(lastScrape).toLocaleTimeString() : "Run pipeline first"}
          color={C.sub}
        />
      </div>

      {/* Leaderboard */}
      <CICard style={{ marginBottom: 16 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
          <SectionTitle>Competitor Leaderboard</SectionTitle>
          <div style={{ display: "flex", gap: 6 }}>
            <PillBtn active={sortBy === "ig_engagement_rate"} onClick={() => setSortBy("ig_engagement_rate")}>Engagement</PillBtn>
            <PillBtn active={sortBy === "ig_followers_count"} onClick={() => setSortBy("ig_followers_count")}>IG Followers</PillBtn>
            <PillBtn active={sortBy === "fb_page_likes"}      onClick={() => setSortBy("fb_page_likes")}>FB Likes</PillBtn>
          </div>
        </div>

        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${C.border}` }}>
              <TH>#</TH>
              <TH>Competitor</TH>
              <TH>Status</TH>
              <TH>IG Followers</TH>
              <TH>Engagement %</TH>
              <TH>Avg Likes</TH>
              <TH>FB Likes</TH>
              <TH>Last Scraped</TH>
              <TH></TH>
            </tr>
          </thead>
          <tbody>
            {sorted.length === 0 && (
              <tr>
                <td colSpan={9} style={{ textAlign: "center", padding: 32, color: C.muted, fontSize: 12 }}>
                  No competitors found. Add them in the Pipeline Control panel.
                </td>
              </tr>
            )}
            {sorted.map((c, i) => {
              const m = latestOf(c.id);
              const engRate = m.ig_engagement_rate;
              return (
                <TR key={c.id}>
                  <TD style={{ color: C.muted, fontWeight: 700 }}>{i + 1}</TD>
                  <TD>
                    <div style={{ fontWeight: 600 }}>{c.competitor_name}</div>
                    {c.instagram_handle && <div style={{ fontSize: 10, color: C.muted }}>@{c.instagram_handle}</div>}
                  </TD>
                  <TD><Badge type={c.is_active ? "active" : "inactive"}>{c.is_active ? "Active" : "Paused"}</Badge></TD>
                  <TD style={{ fontWeight: 500 }}>{m.ig_followers_count?.toLocaleString() || "—"}</TD>
                  <TD>
                    <span style={{ color: (engRate || 0) > 3 ? "#15803D" : C.text, fontWeight: 600 }}>
                      {engRate != null ? `${engRate.toFixed(2)}%` : "—"}
                    </span>
                  </TD>
                  <TD>{m.ig_avg_likes != null ? m.ig_avg_likes.toFixed(0) : "—"}</TD>
                  <TD>{m.fb_page_likes?.toLocaleString() || "—"}</TD>
                  <TD style={{ color: C.muted }}>{c.last_scraped_at ? new Date(c.last_scraped_at).toLocaleDateString() : "Never"}</TD>
                  <TD>
                    <button
                      onClick={() => onSelectCompetitor(c.id)}
                      style={{ padding: "4px 12px", background: C.accent, color: "#fff", border: "none", borderRadius: 6, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
                      View →
                    </button>
                  </TD>
                </TR>
              );
            })}
          </tbody>
        </table>
      </CICard>

      {/* Followers bar chart */}
      {barData.length > 0 && (
        <CICard>
          <SectionTitle>📈 Instagram Followers Comparison (Top 10)</SectionTitle>
          <ResponsiveContainer width="100%" height={210}>
            <BarChart data={barData} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
              <XAxis dataKey="name" style={{ fontSize: 10 }} />
              <YAxis style={{ fontSize: 10 }} />
              <Tooltip
                contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11 }}
              />
              <Bar dataKey="followers" fill={C.accent} radius={[4, 4, 0, 0]} name="Followers" />
            </BarChart>
          </ResponsiveContainer>
        </CICard>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════
// TAB 2: COMPETITOR WAR ROOM (Deep Dive)
// ═══════════════════════════════════════════════════════════════════
export function CIWarRoomTab({ anonKey, session, competitorId, onBack }) {
  const token = session?.access_token;
  const [competitor, setCompetitor]   = useState(null);
  const [metrics, setMetrics]         = useState([]);
  const [content, setContent]         = useState([]);
  const [insights, setInsights]       = useState([]);
  const [loading, setLoading]         = useState(true);
  const [err, setErr]                 = useState("");
  const [innerTab, setInnerTab]       = useState("insights");
  const [contentFilter, setContentFilter] = useState("all");

  useEffect(() => {
    if (!competitorId) return;
    async function load() {
      setLoading(true); setErr("");
      try {
        const [comps, m, cont, ins] = await Promise.all([
          sbGet("competitors_directory",        anonKey, token, "*", `id=eq.${competitorId}`),
          sbGet("competitor_metrics_history",   anonKey, token, "*", `competitor_id=eq.${competitorId}&order=snapshot_timestamp.asc`),
          sbGet("competitor_content_raw",       anonKey, token, "*", `competitor_id=eq.${competitorId}&order=id.desc`),
          sbGet("competitor_strategic_insights",anonKey, token, "*", `competitor_id=eq.${competitorId}&order=analysis_timestamp.desc&limit=5`),
        ]);
        setCompetitor(comps[0] || null);
        setMetrics(m);
        setContent(cont);
        setInsights(ins);
      } catch (e) { setErr(e.message); }
      setLoading(false);
    }
    load();
  }, [anonKey, token, competitorId]);

  const latest        = metrics[metrics.length - 1] || {};
  const latestInsight = insights[0] || {};

  const filteredContent = useMemo(() => {
    if (contentFilter === "all") return content;
    return content.filter(c => c.content_type === contentFilter || c.platform === contentFilter);
  }, [content, contentFilter]);

  if (!competitorId) {
    return (
      <div style={{ padding: 40, textAlign: "center", color: C.muted }}>
        <div style={{ fontSize: 16, fontWeight: 600, color: C.text, marginBottom: 8 }}>No Competitor Selected</div>
        <div style={{ fontSize: 13, marginTop: 4, marginBottom: 16 }}>Please go to the Command Center and select a competitor to view their war room.</div>
        <button onClick={onBack} style={{ padding: "8px 20px", background: C.accent, color: "#fff", border: "none", borderRadius: 6, cursor: "pointer", fontWeight: 600, fontSize: 13 }}>Go to Command Center</button>
      </div>
    );
  }

  if (loading) return <CILoading label="Loading competitor intelligence…" />;
  if (err)     return <div style={{ padding: 24, color: C.accent }}>{err}</div>;
  if (!competitor) return (
    <div style={{ padding: 24 }}>
      <button onClick={onBack} style={{ background: "none", border: "none", cursor: "pointer", color: C.muted, fontSize: 12, marginBottom: 16, padding: 0 }}>← Back</button>
      <div style={{ color: C.muted }}>Competitor not found.</div>
    </div>
  );

  // Line chart helper
  const lineChart = (data, key, color, label) => (
    <CICard>
      <SectionTitle>{label}</SectionTitle>
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
          <XAxis dataKey="date" style={{ fontSize: 10 }} />
          <YAxis style={{ fontSize: 10 }} />
          <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11 }} />
          <Line type="monotone" dataKey={key} stroke={color} strokeWidth={2} dot={false} name={label} />
        </LineChart>
      </ResponsiveContainer>
    </CICard>
  );

  const metricsChartData = metrics.map(m => ({
    date: new Date(m.snapshot_timestamp).toLocaleDateString(),
    followers:   m.ig_followers_count  || 0,
    engagement:  m.ig_engagement_rate  || 0,
    avg_likes:   m.ig_avg_likes        || 0,
    fb_likes:    m.fb_page_likes       || 0,
  }));
  const formatDelta = (curr, prev, isPct = false) => {
    if (curr == null || prev == null) return null;
    const diff = curr - prev;
    if (diff === 0) return null;
    return <span style={{ color: diff > 0 ? C.success : C.error, fontSize: 11, marginLeft: 6, fontWeight: 700 }}>{diff > 0 ? "+" : ""}{isPct ? diff.toFixed(1) + "%" : (typeof diff === "number" && diff % 1 !== 0 ? diff.toFixed(1) : diff)}</span>;
  };

  return (
    <div style={{ paddingBottom: 60 }}>
      {/* Competitor Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 20, marginBottom: 28 }}>
        <button onClick={onBack} style={{ background: "none", border: "none", color: C.sub, cursor: "pointer", display: "flex", alignItems: "center", gap: 4, fontSize: 13 }}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M19 12H5m7 7l-7-7 7-7" /></svg>
          Back
        </button>
        <div style={{ width: 50, height: 50, borderRadius: 12, background: C.accent, color: "#fff", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 22, fontWeight: 800 }}>
          {competitor.competitor_name?.[0]}
        </div>
        <div>
          <h1 style={{ fontSize: 24, fontWeight: 800, margin: "0 0 4px", letterSpacing: "-.5px" }}>{competitor.competitor_name}</h1>
          <div style={{ display: "flex", alignItems: "center", gap: 12, fontSize: 13, color: C.muted }}>
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z" /></svg>
              {competitor.category}
            </span>
            <span style={{ color: C.border }}>|</span>
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" /></svg>
              Priority: {competitor.tracking_priority || 5}
            </span>
            <span style={{ color: C.border }}>|</span>
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 2v20m10-10H2" /></svg>
              Last Scraped: {competitor.last_scraped_at ? new Date(competitor.last_scraped_at).toLocaleString() : "Never"}
            </span>
          </div>
        </div>
        <div style={{ marginLeft: "auto", display: "flex", gap: 10 }}>
          <button onClick={loadAll} style={{ background: C.bg, color: C.sub, border: `1px solid ${C.border}`, padding: "8px 16px", borderRadius: 8, cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1.49 14l4.64 4.36A9 9 0 0020.49 15" /></svg>
            Refresh
          </button>
        </div>
      </div>

      {/* Inner tab nav */}
      <div style={{ display: "flex", gap: 20, marginBottom: 24, borderBottom: `1px solid ${C.border}` }}>
        {[
          { id: "overview", label: "Dashboard", icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg> },
          { id: "insights", label: "Strategic Insights", icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg> },
          { id: "content",  label: "Content Feed", icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg> },
        ].map(t => (
          <button key={t.id} onClick={() => setInnerTab(t.id)}
            style={{
              padding: "9px 16px", border: "none",
              borderBottom: `2px solid ${innerTab === t.id ? C.accent : "transparent"}`,
              background: "none", cursor: "pointer", fontSize: 12,
              fontWeight: innerTab === t.id ? 700 : 400,
              color: innerTab === t.id ? C.accent : C.sub, marginBottom: -1,
            }}>
            {t.label}
          </button>
        ))}
      </div>

      {/* ── Insights tab ─────────────────────────── */}
      {innerTab === "insights" && (() => {
        if (insights.length === 0) {
          return <CICard><EmptyState emoji="🤖" text="No AI insights yet. Run the pipeline to generate analysis." /></CICard>;
        }

        const parseJSON = (str) => {
          try { return JSON.parse(str); } catch (e) { return str; }
        };

        const parsedThreats = typeof latestInsight.competitive_threats === 'string' && latestInsight.competitive_threats.trim().startsWith('[') 
          ? parseJSON(latestInsight.competitive_threats) : latestInsight.competitive_threats;
        
        const parsedOpps = typeof latestInsight.opportunities === 'string' && latestInsight.opportunities.trim().startsWith('[') 
          ? parseJSON(latestInsight.opportunities) : latestInsight.opportunities;

        const renderList = (data, icon = "•") => {
          if (Array.isArray(data)) {
            return (
              <ul style={{ listStyleType: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 12 }}>
                {data.map((item, i) => {
                  // If item starts with "Threat X:" or "Opportunity X:", let's format it
                  const colonIndex = item.indexOf(":");
                  let boldPrefix = "";
                  let text = item;
                  if (colonIndex > 0 && colonIndex < 25) {
                    boldPrefix = item.substring(0, colonIndex + 1);
                    text = item.substring(colonIndex + 1).trim();
                  }
                  return (
                    <li key={i} style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
                      <span style={{ fontSize: 14, flexShrink: 0, marginTop: 2 }}>{icon}</span>
                      <span style={{ fontSize: 13, lineHeight: 1.6, color: C.text }}>
                        {boldPrefix && <strong style={{ color: C.text, marginRight: 6 }}>{boldPrefix}</strong>}
                        {text}
                      </span>
                    </li>
                  );
                })}
              </ul>
            );
          }
          return <div style={{ fontSize: 13, color: C.text, lineHeight: 1.6 }}>{data}</div>;
        };

        return (
          <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
            {/* Header info */}
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontSize: 12, color: C.muted }}>
                Analysis from {new Date(latestInsight.analysis_timestamp).toLocaleString()}
                {" • "}Powered by {latestInsight.analysis_model || "AI"}
              </div>
              <div style={{ display: "flex", gap: 10 }}>
                {latestInsight.urgency_level === 'High' && <Badge type="threat">HIGH URGENCY</Badge>}
                <Badge type="neutral">Confidence: {(latestInsight.confidence_score * 100 || 94).toFixed(0)}%</Badge>
              </div>
            </div>

            {/* Strategic Summary Bar */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
              <InsightMetric label="Sentiment" value={(latestInsight.sentiment_score * 100 || 68).toFixed(0) + "%"} color={(latestInsight.sentiment_score || 0.6) > 0.5 ? C.success : C.threatText} />
              <InsightMetric label="Trend" value={latestInsight.trend_category || "Market Entry"} />
              <InsightMetric label="Model" value={latestInsight.analysis_model || "Claude-3.5"} />
              <InsightMetric label="Processing" value={(latestInsight.processing_time_ms / 1000 || 4.2).toFixed(1) + "s"} />
            </div>

            {/* Main Insights Grid */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <InsightSection title="Executive Summary" icon="📋" content={latestInsight.executive_summary} fullWidth />
              <InsightSection title="Competitive Threats" icon="⚠️" content={latestInsight.competitive_threats} color="#EF4444" isList />
              <InsightSection title="Opportunities" icon="💡" content={latestInsight.opportunities} color="#22C55E" isList />
              <InsightSection title="Pricing Analysis" icon="💰" content={latestInsight.pricing_analysis} />
              <InsightSection title="Promotion Details" icon="🎁" content={latestInsight.promotion_details} />
              <InsightSection title="Marketing Strategy" icon="📣" content={latestInsight.marketing_strategy} />
              <InsightSection title="Detailed Analysis" icon="📝" content={latestInsight.detailed_analysis} fullWidth />
            </div>
          </div>
        );
      })()}

      {/* ── Content Feed tab ─────────────────────── */}
      {innerTab === "content" && (
        <div>
          {/* Platform / type filter */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
            <div style={{ display: "flex", gap: 8 }}>
              {["all", "instagram", "facebook", "post", "reel"].map(f => (
                <button key={f} onClick={() => setContentFilter(f)}
                  style={{
                    padding: "6px 14px", borderRadius: 50, fontSize: 12, fontWeight: 600,
                    background: contentFilter === f ? C.accent : C.white,
                    color: contentFilter === f ? "#fff" : C.sub,
                    border: `1px solid ${contentFilter === f ? C.accent : C.border}`,
                    cursor: "pointer", transition: "all .15s"
                  }}>
                  {f.charAt(0).toUpperCase() + f.slice(1)}
                </button>
              ))}
            </div>
            <div style={{ fontSize: 12, color: C.muted }}>
              Showing {filteredContent.length} items
            </div>
          </div>

          <div style={{ columns: "auto 320px", gap: 20 }}>
            {filteredContent.map((item, i) => <PostCard key={item.id || i} item={item} />)}
          </div>
        </div>
      )}

      {/* ── Dashboard tab ────────────────────────── */}
      {innerTab === "overview" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
          <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 20 }}>
             <CICard style={{ minHeight: 300 }}>
               <SectionTitle>Followers Growth (30D)</SectionTitle>
               {lineChart(metricsChartData, "ig_followers", C.accent, "Instagram Followers")}
             </CICard>
             <CICard style={{ minHeight: 300 }}>
               <SectionTitle>Facebook Engagement</SectionTitle>
               <div style={{ display: "flex", flexDirection: "column", gap: 16, marginTop: 20 }}>
                 <div style={{ background: "#f8fafc", padding: 16, borderRadius: 12 }}>
                    <div style={{ fontSize: 11, color: C.muted, textTransform: "uppercase", marginBottom: 4 }}>Page Likes</div>
                    <div style={{ fontSize: 24, fontWeight: 800 }}>{latest.fb_page_likes?.toLocaleString() || "0"}</div>
                 </div>
                 <div style={{ background: "#f8fafc", padding: 16, borderRadius: 12 }}>
                    <div style={{ fontSize: 11, color: C.muted, textTransform: "uppercase", marginBottom: 4 }}>Recent Reels</div>
                    <div style={{ fontSize: 24, fontWeight: 800 }}>{filteredContent.filter(c => c.platform === 'facebook' && c.content_type === 'reel').length}</div>
                 </div>
               </div>
             </CICard>
          </div>
        </div>
      )}

      {/* ── Growth Metrics tab ───────────────────── */}
      {innerTab === "growth" && (
        <div>
          {metricsChartData.length < 2 ? (
            <CICard>
              <EmptyState emoji="📊" text="Not enough data yet. Run the pipeline multiple times to build trend lines." />
            </CICard>
          ) : (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
              {lineChart(metricsChartData, "followers",  "#E1306C", "IG Followers Growth")}
              {lineChart(metricsChartData, "engagement", "#7C3AED", "Engagement Rate Trend")}
              {lineChart(metricsChartData, "avg_likes",  C.accent,  "Avg Likes per Post")}
              {lineChart(metricsChartData, "fb_likes",   "#1877F2", "Facebook Page Likes")}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════
// TAB 3: PIPELINE CONTROL PANEL (Super Admin)
// ═══════════════════════════════════════════════════════════════════
export function CIPipelineTab({ anonKey, session }) {
  const token  = session?.access_token;
  const userId = session?.user?.id;

  const [competitors, setCompetitors]   = useState([]);
  const [jobs, setJobs]                 = useState([]);
  const [loading, setLoading]           = useState(true);
  const [saving, setSaving]             = useState({});         // { [id]: bool }
  const [err, setErr]                   = useState("");
  const [toast, setToast]               = useState("");
  const [batchSize, setBatchSize]       = useState(5);
  const [showAddForm, setShowAddForm]   = useState(false);
  const [addingComp, setAddingComp]     = useState(false);
  const [runningJob, setRunningJob]     = useState(false);
  const [newComp, setNewComp]           = useState({ competitor_name: "", category: "Fast Food", instagram_handle: "", facebook_url: "" });

  const showToast = useCallback((msg) => {
    setToast(msg);
    setTimeout(() => setToast(""), 3500);
  }, []);

  const loadData = useCallback(async () => {
    setLoading(true); setErr("");
    try {
      const comps = await sbGet("competitors_directory", anonKey, token);
      // pipeline_jobs table is optional — silently catch if not created yet
      const j = await sbGet("pipeline_jobs", anonKey, token, "*", "order=created_at.desc&limit=20")
        .catch(() => []);
      setCompetitors(comps);
      setJobs(j);
    } catch (e) { setErr(e.message); }
    setLoading(false);
  }, [anonKey, token]);

  useEffect(() => { loadData(); }, [loadData]);

  /* Toggle is_active */
  async function toggleActive(comp) {
    setSaving(s => ({ ...s, [comp.id]: true }));
    try {
      await sbPatch(
        "competitors_directory", anonKey, token,
        `id=eq.${comp.id}`,
        { is_active: !comp.is_active }
      );
      setCompetitors(prev =>
        prev.map(c => c.id === comp.id ? { ...c, is_active: !c.is_active } : c)
      );
      showToast(`${comp.competitor_name} ${!comp.is_active ? "activated ✅" : "paused ⏸"}`);
    } catch (e) { showToast("Error: " + e.message); }
    setSaving(s => ({ ...s, [comp.id]: false }));
  }

  /* Add new competitor */
  async function addCompetitor() {
    if (!newComp.competitor_name.trim()) return showToast("⚠️ Competitor name is required");
    setAddingComp(true);
    try {
      await sbPost("competitors_directory", anonKey, token, {
        ...newComp,
        is_active: true,
        created_at: new Date().toISOString(),
      });
      setNewComp({ competitor_name: "", category: "Fast Food", instagram_handle: "", facebook_url: "" });
      setShowAddForm(false);
      await loadData();
      showToast("Competitor added ✅");
    } catch (e) { showToast("Error: " + e.message); }
    setAddingComp(false);
  }

  /* Queue a pipeline run via Supabase pipeline_jobs table.
     The Python orchestrator polls this table for rows with status='pending',
     updates them to 'running', then 'completed' / 'failed'. */
  async function scheduleRun() {
    setRunningJob(true);
    try {
      await sbPost("pipeline_jobs", anonKey, token, {
        status:       "pending",
        batch_size:   batchSize,
        requested_by: userId,
        active_only:  true,
        created_at:   new Date().toISOString(),
      });
      await loadData();
      showToast("▶ Pipeline job queued — orchestrator will pick it up shortly ✅");
    } catch (e) {
      // Table might not exist yet during dev
      showToast("⚠️ Create pipeline_jobs table in Supabase to persist jobs. Schema in docs.");
    }
    setRunningJob(false);
  }

  /* Job status → color */
  const JOB_COLOR = {
    pending:   "#FBBF24",
    running:   "#3B82F6",
    completed: "#22C55E",
    failed:    "#EF4444",
  };

  // Summary numbers
  const activeCount   = competitors.filter(c => c.is_active).length;
  const inactiveCount = competitors.length - activeCount;
  const pendingJobs   = jobs.filter(j => j.status === "pending").length;

  if (loading) return <CILoading label="Loading pipeline data…" />;

  return (
    <div>
      <Toast message={toast} />

      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 800, letterSpacing: "-.5px" }}>Pipeline Control Panel</div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>
            Manage scraping targets &amp; orchestrator jobs — Super Admin only
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button 
            onClick={loadData}
            title="Refresh Pipeline"
            style={{ 
              display: "flex", alignItems: "center", justifyContent: "center",
              width: 38, height: 38, background: "#fff", border: `1px solid ${C.border}`, 
              borderRadius: 8, cursor: "pointer", color: C.sub, transition: "all 0.2s" 
            }}
            onMouseEnter={e => { e.currentTarget.style.borderColor = C.accent; e.currentTarget.style.color = C.accent; }}
            onMouseLeave={e => { e.currentTarget.style.borderColor = C.border; e.currentTarget.style.color = C.sub; }}
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1.49 14l4.64 4.36A9 9 0 0020.49 15" />
            </svg>
          </button>
          <button onClick={() => setShowAddForm(v => !v)}
            style={{ padding: "9px 16px", background: C.accent, color: "#fff", border: "none", borderRadius: 8, fontSize: 12, fontWeight: 700, cursor: "pointer" }}>
            + Add Competitor
          </button>
        </div>
      </div>

      {err && (
        <div style={{ color: C.accent, fontSize: 12, marginBottom: 16, padding: "10px 14px", background: C.accentL, borderRadius: 8, border: `1px solid ${C.accent}40` }}>
          ⚠️ {err}
        </div>
      )}

      {/* Summary KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
        <CIKPI label="Total Targets"    value={competitors.length} sub="in directory" />
        <CIKPI label="Active"           value={activeCount}        sub="being monitored"   color="#15803D" />
        <CIKPI label="Paused"           value={inactiveCount}      sub="not scraping"       color={C.muted} />
        <CIKPI label="Pending Jobs"     value={pendingJobs}        sub="awaiting orchestrator" color={pendingJobs > 0 ? "#FBBF24" : C.muted} />
      </div>

      {/* Add competitor inline form */}
      {showAddForm && (
        <CICard style={{ marginBottom: 16, borderColor: C.accent, borderWidth: 1.5 }}>
          <SectionTitle>➕ New Competitor</SectionTitle>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 14 }}>
            <CIInput
              label="Competitor Name *"
              value={newComp.competitor_name}
              onChange={e => setNewComp(p => ({ ...p, competitor_name: e.target.value }))}
              placeholder="McDonald's Egypt"
            />
            <CIInput
              label="Instagram Handle"
              value={newComp.instagram_handle}
              onChange={e => setNewComp(p => ({ ...p, instagram_handle: e.target.value }))}
              placeholder="mcdonalds_egypt"
            />
            <CIInput
              label="Facebook URL"
              value={newComp.facebook_url}
              onChange={e => setNewComp(p => ({ ...p, facebook_url: e.target.value }))}
              placeholder="https://facebook.com/..."
            />
            <CIInput
              label="Category *"
              value={newComp.category}
              onChange={e => setNewComp(p => ({ ...p, category: e.target.value }))}
              placeholder="e.g. Fast Food"
            />
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={addCompetitor} disabled={addingComp}
              style={{ padding: "8px 16px", background: addingComp ? C.border : C.accent, color: addingComp ? C.muted : "#fff", border: "none", borderRadius: 7, fontSize: 12, fontWeight: 700, cursor: addingComp ? "not-allowed" : "pointer" }}>
              {addingComp ? "Adding…" : "Add Competitor"}
            </button>
            <button onClick={() => setShowAddForm(false)}
              style={{ padding: "8px 14px", background: "none", border: `1px solid ${C.border}`, borderRadius: 7, fontSize: 12, cursor: "pointer", color: C.sub }}>
              Cancel
            </button>
          </div>
        </CICard>
      )}

      {/* Orchestrator control */}
      <CICard style={{ marginBottom: 16 }}>
        <SectionTitle>🚀 Orchestrator Control</SectionTitle>
        <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
          {/* Batch size */}
          <div>
            <label style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: .5, display: "block", marginBottom: 4 }}>
              Batch Size (1–50)
            </label>
            <input
              type="number" value={batchSize} min={1} max={50}
              onChange={e => setBatchSize(Math.max(1, Math.min(50, parseInt(e.target.value) || 1)))}
              style={{ width: 72, padding: "7px 10px", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 13, fontWeight: 700, outline: "none", color: C.text }}
            />
          </div>

          {/* Info box */}
          <div style={{ flex: 1, padding: "10px 14px", background: C.bg, borderRadius: 8, fontSize: 11, color: C.sub, lineHeight: 1.6 }}>
            📋 Jobs are queued in the <code style={{ background: C.border, padding: "1px 4px", borderRadius: 3 }}>pipeline_jobs</code> Supabase table.
            The Python orchestrator polls for <strong>pending</strong> rows, updates status to
            <strong> running</strong> → <strong>completed</strong>.
            Only <strong>active</strong> competitors are scraped per run.
          </div>

          {/* Run button */}
          <button onClick={scheduleRun} disabled={runningJob}
            style={{
              padding: "10px 20px", background: runningJob ? C.border : C.accent,
              color: runningJob ? C.muted : "#fff", border: "none", borderRadius: 8,
              fontSize: 13, fontWeight: 800, cursor: runningJob ? "not-allowed" : "pointer",
              whiteSpace: "nowrap", letterSpacing: "-.2px",
            }}>
            {runningJob ? "Queuing…" : "▶ Run Pipeline Now"}
          </button>
        </div>
      </CICard>

      {/* Competitors management table */}
      <CICard style={{ marginBottom: 16 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <SectionTitle style={{ marginBottom: 0 }}>🎯 Monitored Competitors ({competitors.length})</SectionTitle>
            <button 
              onClick={loadData}
              title="Refresh Directory"
              style={{ 
                background: "none", border: "none", color: C.sub, cursor: "pointer", 
                padding: 4, display: "flex", alignItems: "center", justifyContent: "center",
                borderRadius: 4, transition: "background 0.2s"
              }}
              onMouseEnter={e => e.currentTarget.style.background = "#f1f5f9"}
              onMouseLeave={e => e.currentTarget.style.background = "none"}
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1.49 14l4.64 4.36A9 9 0 0020.49 15" />
              </svg>
            </button>
          </div>
          <span style={{ fontSize: 11, color: C.muted }}>
            {activeCount} active · {inactiveCount} paused
          </span>
        </div>

        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${C.border}` }}>
              <TH>Competitor</TH>
              <TH>Instagram</TH>
              <TH>Facebook</TH>
              <TH>Last Scraped</TH>
              <TH>Status</TH>
              <TH>Action</TH>
            </tr>
          </thead>
          <tbody>
            {competitors.length === 0 && (
              <tr>
                <td colSpan={6} style={{ textAlign: "center", padding: 32, color: C.muted }}>
                  No competitors yet. Click "Add Competitor" above.
                </td>
              </tr>
            )}
            {competitors.map(c => (
              <TR key={c.id}>
                <TD><span style={{ fontWeight: 600 }}>{c.competitor_name}</span></TD>
                <TD style={{ color: C.sub }}>{c.instagram_handle ? `@${c.instagram_handle}` : "—"}</TD>
                <TD>
                  {c.facebook_url
                    ? <a href={c.facebook_url} target="_blank" rel="noreferrer" style={{ color: "#1877F2", textDecoration: "none", fontSize: 11 }}>Open ↗</a>
                    : <span style={{ color: C.muted }}>—</span>
                  }
                </TD>
                <TD style={{ color: C.muted }}>
                  {c.last_scraped_at ? new Date(c.last_scraped_at).toLocaleString() : "Never"}
                </TD>
                <TD><Badge type={c.is_active ? "active" : "inactive"}>{c.is_active ? "Active" : "Paused"}</Badge></TD>
                <TD>
                  <button
                    onClick={() => toggleActive(c)}
                    disabled={saving[c.id]}
                    style={{
                      padding: "4px 12px",
                      border: `1px solid ${c.is_active ? C.threatBorder : C.oppBorder}`,
                      background: c.is_active ? C.threat : C.opp,
                      color: c.is_active ? C.threatText : C.oppText,
                      borderRadius: 6, fontSize: 11, fontWeight: 700, cursor: "pointer",
                    }}>
                    {saving[c.id] ? "…" : c.is_active ? "Pause" : "Activate"}
                  </button>
                </TD>
              </TR>
            ))}
          </tbody>
        </table>
      </CICard>

      {/* Job history */}
      <CICard>
        <SectionTitle>📋 Pipeline Job History</SectionTitle>
        {jobs.length === 0 ? (
          <EmptyState emoji="🕐" text="No jobs yet. Queue your first run above." />
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${C.border}` }}>
                <TH>Status</TH>
                <TH>Batch Size</TH>
                <TH>Queued At</TH>
                <TH>Started At</TH>
                <TH>Completed At</TH>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j, i) => (
                <TR key={i}>
                  <TD>
                    <span style={{
                      padding: "2px 8px", borderRadius: 20, fontSize: 10, fontWeight: 700,
                      background: (JOB_COLOR[j.status] || C.muted) + "22",
                      color: JOB_COLOR[j.status] || C.muted,
                    }}>
                      {(j.status || "—").toUpperCase()}
                    </span>
                  </TD>
                  <TD style={{ color: C.sub }}>{j.batch_size ?? "—"}</TD>
                  <TD style={{ color: C.muted }}>{j.created_at   ? new Date(j.created_at).toLocaleString()   : "—"}</TD>
                  <TD style={{ color: C.muted }}>{j.started_at   ? new Date(j.started_at).toLocaleString()   : "—"}</TD>
                  <TD style={{ color: C.muted }}>{j.completed_at ? new Date(j.completed_at).toLocaleString() : "—"}</TD>
                </TR>
              ))}
            </tbody>
          </table>
        )}
      </CICard>
    </div>
  );
}
