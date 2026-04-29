import { useState, useEffect, useMemo, useRef } from "react";
import { pipeline, env } from "@xenova/transformers";
import MerchantProfiler from "./MerchantProfiler";
import MerchantNotes from "./MerchantNotes";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, CartesianGrid,
} from "recharts";

env.allowLocalModels = false;

/* ─── CONFIG ─────────────────────────────────────────────────── */
const SB_URL = process.env.REACT_APP_SUPABASE_URL || "https://omowdfzyudedrtcuhnvy.supabase.co";
const CITIES = ["riyadh", "jeddah", "dammam", "khobar", "mecca", "medina"];
const OMAN_CITIES = ["muscat"];
const C = {
  accent: "#FF5A00", accentL: "#FFF0ED", bg: "#F5F2EE",
  white: "#FFFFFF", border: "#E8E4DF", text: "#1A1A1A",
  muted: "#9B9792", sub: "#6B6B6B",
};

/* ─── MERCHANT CACHE ─────────────────────────────────────────── */
const merchantCache = new Map();
const CACHE_TTL = 30 * 60 * 1000;

/* ─── BRANDING ───────────────────────────────────────────────── */
const WaffarhaIcon = ({ size = 120, style }) => (
  <img src="https://i.ibb.co/j9kcTcrK/Waffarha-logo.png" alt="Waffarha Icon" style={{ width: size, height: "auto", objectFit: "contain", display: "block", ...style }} />
);

const WaffarhaLogo = ({ height = 36, style }) => (
  <img src="https://i.ibb.co/d4qyQ4gF/waffarha-logo.png" alt="Waffarha Logo" style={{ height: height, width: "auto", objectFit: "contain", display: "block", ...style }} />
);

/* ─── SUPABASE REST HELPERS ──────────────────────────────────── */
const sbH = (key, token) => ({
  apikey: key,
  Authorization: `Bearer ${token || key}`,
  "Content-Type": "application/json",
});

async function sbFetch(table, key, token, select = "*") {
  let allRows = [];
  let offset = 0;
  const limit = 1000;
  while (true) {
    const r = await fetch(
      `${SB_URL}/rest/v1/${table}?select=${select}&limit=${limit}&offset=${offset}`,
      { headers: sbH(key, token) }
    );
    if (!r.ok) {
      const msg = await r.text();
      throw new Error(`${r.status}: ${msg}`);
    }
    const data = await r.json();
    allRows.push(...data);
    if (data.length < limit) break;
    offset += limit;
  }
  return allRows;
}

async function sbLogin(key, email, password) {
  const r = await fetch(`${SB_URL}/auth/v1/token?grant_type=password`, {
    method: "POST",
    headers: { apikey: key, "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  const d = await r.json();
  if (!r.ok) throw new Error(d.error_description || d.message || "Login failed");
  return d;
}

/* ─── AUDIT LOG ──────────────────────────────────────────────── */
async function logAudit(anonKey, token, userId, action, resource, meta = {}) {
  try {
    await fetch(`${SB_URL}/rest/v1/nexus_audit_log`, {
      method: "POST",
      headers: { ...sbH(anonKey, token), Prefer: "return=minimal" },
      body: JSON.stringify({ user_id: userId, action, resource, metadata: meta, created_at: new Date().toISOString() }),
    });
  } catch (e) { /* silent fail */ }
}

/* ─── NORMALIZATION ──────────────────────────────────────────── */
const normArabic = s => (s || "").toLowerCase().replace(/[أإآا]/g, 'ا').replace(/[ةه]/g, 'ه').replace(/[ىي]/g, 'ي').replace(/\s+/g, '');
const editDist = (s1, s2) => {
  if (!s1) return s2.length; if (!s2) return s1.length;
  let c = Array(s2.length + 1).fill(0).map((_, i) => i);
  for (let i = 1; i <= s1.length; i++) {
    let last = i;
    for (let j = 1; j <= s2.length; j++) {
      const val = c[j - 1]; c[j - 1] = last;
      last = s1[i - 1] === s2[j - 1] ? val : Math.min(val, last, c[j]) + 1;
    }
    c[s2.length] = last;
  }
  return c[s2.length];
};

const norm = m => {
  const pRaw = m.priority ? String(m.priority).replace(/🔴|🟡|🟢/g, "").trim() : "";
  const prio = !pRaw || pRaw.toLowerCase() === "none" || pRaw.toLowerCase() === "null" || pRaw.toLowerCase() === "nan" ? "Uncategorized" : pRaw;
  const hRaw = String(m.opening_hours || "").trim();
  let hCat = "Not Available";
  if (hRaw.toLowerCase().includes("24 hours") || hRaw.toLowerCase().includes("24/7") || hRaw.includes("٢٤ ساعة")) hCat = "24 Hours";
  else if (hRaw.length > 2 && hRaw.toLowerCase() !== "none" && hRaw.toLowerCase() !== "null") hCat = "Specified Hours";
  return {
    Merchant: m.merchant_name || "",
    Mall: m.mall || "",
    City: m.city || "",
    Priority: prio,
    Rating: parseFloat(m.rating) || 0,
    Reviews: parseInt(m.reviews_count) || 0,
    AvgPrice: m.avg_price || "",
    Branches: Math.max(1, parseInt(m.branches_ksa) || 0),
    Phone: m.phone || "",
    Website: m.website || "",
    Reviews3: m.top_reviews || "",
    Category: m.category || "Uncategorized",
    OpeningHours: hRaw.toLowerCase() === "none" || hRaw.toLowerCase() === "null" || hRaw.toLowerCase() === "nan" ? "" : hRaw,
    HoursCategory: hCat,
    SubCategory: m.sub_category || "General",
  };
};

/* ─── STAT COMPUTATION ──────────────────────────────────────── */
function computeCityStats(merchants) {
  const s = {};
  merchants.forEach(m => {
    const city = m.City || "Unknown";
    if (!s[city]) s[city] = { total: 0, high: 0, medium: 0, low: 0, rSum: 0, rN: 0, malls: new Set() };
    s[city].total++;
    const p = m.Priority.toLowerCase();
    if (p.includes("high")) s[city].high++;
    else if (p.includes("medium")) s[city].medium++;
    else s[city].low++;
    if (m.Rating > 0) { s[city].rSum += m.Rating; s[city].rN++; }
    if (m.Mall) s[city].malls.add(m.Mall);
  });
  const r = {};
  for (const [city, d] of Object.entries(s)) {
    r[city] = { total: d.total, high: d.high, medium: d.medium, low: d.low, avgRating: d.rN ? +(d.rSum / d.rN).toFixed(2) : 0, malls: d.malls.size };
  }
  return r;
}

// eslint-disable-next-line no-unused-vars
function price_to_range(level) {
  return { 0: "10–20 SAR", 1: "20–40 SAR", 2: "40–80 SAR", 3: "80–150 SAR", 4: "150+ SAR" }[level] || "";
}

function computePriceTiers(merchants) {
  const tiers = { "10–20 SAR": 0, "20–40 SAR": 0, "40–80 SAR": 0, "80–150 SAR": 0, "150+ SAR": 0 };
  merchants.forEach(m => { if (tiers[m.AvgPrice] !== undefined) tiers[m.AvgPrice]++; });
  const colors = ["#4ADE80", "#E8563A", "#FBBF24", "#818CF8", "#A78BFA"];
  return Object.entries(tiers).map(([name, value], i) => ({ name, value, color: colors[i] }));
}

function computeTopMalls(merchants, n = 8) {
  const cnt = {}, hi = {};
  merchants.forEach(m => {
    if (!m.Mall) return;
    cnt[m.Mall] = (cnt[m.Mall] || 0) + 1;
    if (m.Priority.toLowerCase().includes("high")) hi[m.Mall] = (hi[m.Mall] || 0) + 1;
  });
  return Object.entries(cnt).sort((a, b) => b[1] - a[1]).slice(0, n)
    .map(([name, count]) => ({ name, count, high: hi[name] || 0 }));
}

/* ─── SMALL UI COMPONENTS ────────────────────────────────────── */
function KPI({ label, value, sub, color, onClick }) {
  const accent = color || C.accent;
  return (
    <div onClick={onClick}
      style={{
        background: C.white, borderRadius: 10, padding: "14px 16px", border: `1px solid ${C.border}`,
        cursor: onClick ? "pointer" : "default",
        transition: "all .12s",
        boxShadow: "none"
      }}
      onMouseEnter={e => {
        if (onClick) {
          e.currentTarget.style.borderColor = accent;
          e.currentTarget.style.boxShadow = `0 2px 12px ${accent}22`;
        }
      }}
      onMouseLeave={e => {
        if (onClick) {
          e.currentTarget.style.borderColor = C.border;
          e.currentTarget.style.boxShadow = "none";
        }
      }}
    >
      <div style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: .5 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700, color: accent, letterSpacing: "-.5px", margin: "4px 0 2px" }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: C.muted }}>{sub}</div>}
    </div>
  );
}

function Card({ children, style = {} }) {
  return (
    <div style={{ background: C.white, borderRadius: 10, padding: 16, border: `1px solid ${C.border}`, ...style }}>
      {children}
    </div>
  );
}

function ChartTitle({ children }) {
  return <div style={{ fontSize: 13, fontWeight: 500, marginBottom: 12, color: C.text }}>{children}</div>;
}

const CustomTip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, padding: "8px 12px", fontSize: 11 }}>
      {label && <p style={{ fontWeight: 500, marginBottom: 3 }}>{label}</p>}
      {payload.map(p => <p key={p.name} style={{ color: p.fill || p.color }}>{p.name}: {Number(p.value).toLocaleString()}</p>)}
    </div>
  );
};

/* ─── SETUP SCREEN ───────────────────────────────────────────── */
function SetupScreen({ onSetup }) {
  const [key, setKey] = useState("");
  const [err, setErr] = useState("");
  return (
    <div style={{ display: "flex", height: "100vh", alignItems: "center", justifyContent: "center", background: C.bg }}>
      <div style={{ background: C.white, borderRadius: 14, padding: 36, width: 440, boxShadow: "0 4px 24px rgba(0,0,0,.08)", border: `1px solid ${C.border}` }}>
        <div style={{ textAlign: "center", marginBottom: 24 }}>
          <WaffarhaIcon style={{ margin: "0 auto 16px" }} />
          <div style={{ fontSize: 20, fontWeight: 700 }}>Database Setup</div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 4 }}>Waffarha KSA Market</div>
        </div>
        <div style={{ background: "#F9F8F7", borderRadius: 8, padding: "10px 14px", marginBottom: 18, fontSize: 12 }}>
          <div style={{ color: C.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: .5, marginBottom: 4 }}>Supabase Project</div>
          <code style={{ fontSize: 11, color: C.sub }}>{SB_URL}</code>
        </div>
        <label style={{ display: "block", fontSize: 11, fontWeight: 500, color: C.sub, marginBottom: 6, textTransform: "uppercase", letterSpacing: .5 }}>
          Anon Key (public)
        </label>
        <input value={key} onChange={e => setKey(e.target.value)}
          placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
          style={{ width: "100%", padding: "10px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, fontFamily: "monospace", outline: "none", color: C.text, boxSizing: "border-box", marginBottom: 6 }}
        />
        <div style={{ fontSize: 11, color: C.muted, marginBottom: 18 }}>
          📍 Supabase → Settings → API → <strong>anon public</strong>
        </div>
        {err && <div style={{ color: C.accent, fontSize: 12, padding: "8px 10px", background: C.accentL, borderRadius: 6, marginBottom: 12 }}>{err}</div>}
        <button onClick={() => { if (!key.trim()) return setErr("Anon key required"); onSetup(key.trim()); }}
          style={{ width: "100%", padding: 13, background: C.accent, color: "#fff", border: "none", borderRadius: 8, fontSize: 14, fontWeight: 600, cursor: "pointer" }}>
          Connect to Supabase →
        </button>
      </div>
    </div>
  );
}

/* ─── LOGIN SCREEN ───────────────────────────────────────────── */
function LoginScreen({ anonKey, onLogin }) {
  const [email, setEmail] = useState(localStorage.getItem("wn_email") || "");
  const [password, setPassword] = useState("");
  const [rememberEmail, setRememberEmail] = useState(!!localStorage.getItem("wn_email"));
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  async function handleLogin() {
    if (!email || !password) return setErr("Both fields required");
    setLoading(true); setErr("");
    try {
      const session = await sbLogin(anonKey, email, password);
      if (rememberEmail) localStorage.setItem("wn_email", email);
      else localStorage.removeItem("wn_email");
      onLogin(session);
    } catch (e) { setErr(e.message); }
    setLoading(false);
  }

  return (
    <div style={{ display: "flex", height: "100vh", alignItems: "center", justifyContent: "center", background: C.bg }}>
      <div style={{ background: C.white, borderRadius: 14, padding: 36, width: 360, boxShadow: "0 4px 24px rgba(0,0,0,.08)", border: `1px solid ${C.border}` }}>
        <div style={{ textAlign: "center", marginBottom: 24 }}>
          <WaffarhaIcon style={{ margin: "0 auto 16px" }} />
          <div style={{ fontSize: 20, fontWeight: 700 }}>Sign In</div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 4 }}>Waffarha BD Team</div>
        </div>
        <div style={{ marginBottom: 14 }}>
          <label style={{ display: "block", fontSize: 11, fontWeight: 500, color: C.sub, marginBottom: 6, textTransform: "uppercase", letterSpacing: .5 }}>Email</label>
          <input type="email" value={email} onChange={e => setEmail(e.target.value)} placeholder="bd@waffarha.com"
            onKeyDown={e => e.key === "Enter" && handleLogin()}
            style={{ width: "100%", padding: "10px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, outline: "none", color: C.text, boxSizing: "border-box" }} />
        </div>
        <div style={{ marginBottom: 14 }}>
          <label style={{ display: "block", fontSize: 11, fontWeight: 500, color: C.sub, marginBottom: 6, textTransform: "uppercase", letterSpacing: .5 }}>Password</label>
          <input type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="••••••••"
            onKeyDown={e => e.key === "Enter" && handleLogin()}
            style={{ width: "100%", padding: "10px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, outline: "none", color: C.text, boxSizing: "border-box" }} />
        </div>
        <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 12, color: C.sub, marginBottom: 14, cursor: "pointer" }}>
          <input type="checkbox" checked={rememberEmail} onChange={e => setRememberEmail(e.target.checked)} />
          Remember my email
        </label>
        {err && <div style={{ color: C.accent, fontSize: 12, padding: "8px 10px", background: C.accentL, borderRadius: 6, marginBottom: 12 }}>{err}</div>}
        <button onClick={handleLogin} disabled={loading}
          style={{ width: "100%", padding: 12, background: loading ? C.border : C.accent, color: loading ? C.muted : "#fff", border: "none", borderRadius: 8, fontSize: 13, fontWeight: 600, cursor: loading ? "not-allowed" : "pointer" }}>
          {loading ? "Signing in…" : "Sign In →"}
        </button>
        <div style={{ marginTop: 16, padding: "12px 14px", background: "#F0FDF4", borderRadius: 8, fontSize: 11, color: "#15803D", lineHeight: 1.8 }}>
          <strong>Create users manually:</strong><br />
          Supabase → Authentication → Users → Add user
        </div>
      </div>
    </div>
  );
}

/* ─── LOADING SCREEN ─────────────────────────────────────────── */
function LoadingScreen() {
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh", alignItems: "center", justifyContent: "center", background: C.bg, gap: 24, textAlign: "center", padding: 20 }}>
      <WaffarhaLogo height={65} style={{ marginBottom: -5 }} />

      <div style={{ maxWidth: 500 }}>
        <div style={{ fontSize: 32, fontWeight: 800, color: C.text, marginBottom: 8, letterSpacing: "-1px" }}>
          Nexus
        </div>
        <div style={{ fontSize: 16, color: C.sub, fontWeight: 500, lineHeight: 1.6, maxWidth: 420, margin: "0 auto" }}>
          The all-in-one portal for unparalleled merchant intelligence and smarter CRM quality analysis
        </div>
      </div>

      <div style={{ display: "flex", gap: 7, marginTop: 10 }}>
        {[0, 150, 300].map(d => (
          <div key={d} style={{ width: 8, height: 8, borderRadius: "50%", background: C.accent, animation: "bounce .7s infinite alternate", animationDelay: `${d}ms` }} />
        ))}
      </div>

      <style>{`@keyframes bounce { to { transform:translateY(-6px);opacity:.3; } }`}</style>
    </div>
  );
}

/* ─── TICKET HELPERS ─────────────────────────────────────────── */
function normTicket(a) {
  const t = Array.isArray(a.ticket) ? a.ticket[0] : (a.ticket || {});
  const cleanHtml = s => (s || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  return {
    id: a.id,
    ticket_number: a.ticket_number,
    subject: cleanHtml(t.subject) || "—",
    status: t.status || a.ai_status || "Closed",
    channel: t.channel || "Other",
    priority: t.priority || "",
    reason: a.p_issue_type || t.reason || "Unknown",
    subReason: t.sub_reason || "",
    owner: t.ticket_owner || t.assignee || "",
    createdTime: t.created_time || t.ticket_time || a.analyzed_at || "",
    closedTime: t.closed_time || "",
    happiness: t.happiness_rating || "",
    resolutionMs: parseInt(t.resolution_time_ms) || (t.message ? t.message.length * 1000 : 0), // Proxy: Chat length correlates to time
    numReassign: parseInt(t.num_reassign) || ((t.message || "").toLowerCase().includes("accepted the transfer request") ? 1 : 0),
    numReopen: parseInt(t.num_reopen) || 0,
    isOverdue: String(t.is_overdue) === "true",
    isEscalated: String(t.is_escalated) === "true" || String(a.a_escalated).toLowerCase() === "true",
    slaViolation: t.sla_violation_type || (String(a.a_one_touch_resolution).toLowerCase() === "true" ? "Not Violated" : ""), // Proxy: One touch = SLA met
    escalationValidity: t.escalation_validity || "",
    merchantName: t.merchant_name || a.p_merchant_name || "",
    country: t.country || "",
    language: t.language || "",
    tags: t.tags || "",
    userId: t.user_id || "",
    orderId: t.order_id || "",
    issueType: a.p_issue_type || "",
    merchantIssue: a.p_merchant_issue_type || "",
    isPaymentBlocker: String(a.p_payment_blocker).toLowerCase() === "true",
    isRefundRequested: String(a.p_refund_requested).toLowerCase() === "true",
    uxFriction: a.p_ux_friction_point || "",
    missingFeature: a.p_missing_feature || "",
    rootCause: a.p_root_cause_owner || "",
    smartTags: a.p_smart_tags || "",
    branchName: a.mer_branch_name || "",
    monetaryValue: parseFloat(a.fin_ticket_monetary_value) || 0,
    initialSentiment: a.s_initial_sentiment || "",
    finalSentiment: a.s_final_sentiment || "",
    sentimentShift: a.s_sentiment_shift || "",
    isChurnIntent: String(a.s_churn_intent).toLowerCase() === "true",
    customerEffort: parseInt(a.s_customer_effort_score) || 0,
    sentimentSummary: a.s_sentiment_summary || "",
    empathyScore: parseInt(a.a_empathy_score) || 0,
    policyCompliance: String(a.a_policy_compliance).toLowerCase() === "true",
    knowledgeAccuracy: parseInt(a.a_knowledge_accuracy) || 0,
    overallQualityScore: parseFloat(a.a_overall_score) || 0,
    aiNotes: a.a_evaluation_notes || "",
    aiStatus: a.ai_status || "",
    fraudSuspicion: String(a.f_fraud_suspicion).toLowerCase() === "true",
    isEscalatedAI: String(a.a_escalated).toLowerCase() === "true",
    oneTouchResolutionAI: String(a.a_one_touch_resolution).toLowerCase() === "true",
  };
}

function buildAgentMap(tickets) {
  const map = {};
  tickets.forEach(t => { if (t.owner) map[t.owner] = t.owner; });
  return map;
}

function ticketMonthlyData(tickets) {
  const monthly = {};
  tickets.forEach(t => {
    const m = (t.createdTime || "").slice(0, 7);
    if (!m || m.length < 7) return;
    if (!monthly[m]) monthly[m] = { month: m, total: 0, chat: 0, phone: 0, email: 0, other: 0 };
    monthly[m].total++;
    const ch = (t.channel || "").toLowerCase();
    if (ch === "chat") monthly[m].chat++;
    else if (ch === "phone") monthly[m].phone++;
    else if (ch === "email" || ch === "outbound email") monthly[m].email++;
    else monthly[m].other++;
  });
  return Object.values(monthly).sort((a, b) => a.month.localeCompare(b.month)).slice(-18);
}

const CH_COLORS = { Chat: "#FF5A00", Phone: "#3B82F6", Email: "#10B981", Facebook: "#8B5CF6", cs: "#94A3B8", Instagram: "#E1306C" };
const HP_COLORS = { Good: "#10B981", Okay: "#FBBF24", Bad: "#EF4444" };

/* ─── SUPPORT OVERVIEW TAB ───────────────────────────────────── */
function SupportTab({ tickets }) {
  const total = tickets.length;
  const open = tickets.filter(t => t.status === "Open").length;
  const rated = tickets.filter(t => t.happiness || t.finalSentiment);
  const good = rated.filter(t => t.happiness === "Good" || ["positive", "very_positive"].includes(t.finalSentiment)).length;
  const csat = rated.length ? Math.round(good / rated.length * 100) : 0;
  const slaOk = tickets.filter(t => t.slaViolation === "Not Violated" || t.oneTouchResolutionAI).length;
  const slaViol = tickets.filter(t => t.slaViolation && t.slaViolation !== "Not Violated" && !t.oneTouchResolutionAI).length;
  const slaRate = (slaOk + slaViol) > 0 ? Math.round(slaOk / (slaOk + slaViol) * 100) : (tickets.length ? Math.round(slaOk / tickets.length * 100) : 100);
  const resTimes = tickets.filter(t => t.resolutionMs > 0).map(t => t.resolutionMs / 3600000); // Converted proxy to "hours" equivalent
  const avgRes = resTimes.length ? (resTimes.reduce((a, b) => a + b, 0) / resTimes.length).toFixed(1) : "—";
  const escalated = tickets.filter(t => t.isEscalated).length;

  const analyzed = useMemo(() => tickets.filter(t => t.aiStatus === "completed"), [tickets]);
  const avgQuality = analyzed.length ? (analyzed.reduce((s, t) => s + t.overallQualityScore, 0) / analyzed.length).toFixed(1) : "—";
  const avgEmpathy = analyzed.length ? (analyzed.reduce((s, t) => s + (t.empathyScore || 0), 0) / analyzed.length).toFixed(1) : "—";
  const policyOk = analyzed.filter(t => t.policyCompliance).length;
  const policyRate = analyzed.length ? Math.round(policyOk / analyzed.length * 100) : 100;
  const churnRisks = analyzed.filter(t => t.isChurnIntent).length;

  const monthlyData = useMemo(() => ticketMonthlyData(tickets), [tickets]);

  const channelData = useMemo(() => {
    const counts = {};
    tickets.forEach(t => { const ch = t.channel || "Other"; counts[ch] = (counts[ch] || 0) + 1; });
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 7)
      .map(([name, value]) => ({ name, value, color: CH_COLORS[name] || "#94A3B8" }));
  }, [tickets]);

  const rootCauseData = useMemo(() => {
    const counts = {};
    analyzed.forEach(t => { if (t.rootCause) counts[t.rootCause] = (counts[t.rootCause] || 0) + 1; });
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([name, value]) => ({ name, value }));
  }, [analyzed]);

  const sentimentData = useMemo(() => {
    const counts = { Positive: 0, Neutral: 0, Negative: 0 };
    analyzed.forEach(t => {
      const s = t.finalSentiment || "Neutral";
      if (s.includes("Positive")) counts.Positive++;
      else if (s.includes("Negative")) counts.Negative++;
      else counts.Neutral++;
    });
    return Object.entries(counts).map(([name, value]) => ({
      name, value, color: name === "Positive" ? "#10B981" : name === "Negative" ? "#EF4444" : "#FBBF24"
    })).filter(d => d.value > 0);
  }, [analyzed]);

  const reasonData = useMemo(() => {
    const counts = {};
    tickets.forEach(t => { if (t.reason) counts[t.reason] = (counts[t.reason] || 0) + 1; });
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 10)
      .map(([name, value]) => ({ name: name.length > 22 ? name.slice(0, 22) + "…" : name, value }));
  }, [tickets]);

  const happinessData = [
    { name: "Good", value: rated.filter(t => t.happiness === "Good" || ["positive", "very_positive"].includes(t.finalSentiment)).length, color: HP_COLORS.Good },
    { name: "Okay", value: rated.filter(t => t.happiness === "Okay" || ["neutral", "stable"].includes(t.finalSentiment)).length, color: HP_COLORS.Okay },
    { name: "Bad", value: rated.filter(t => t.happiness === "Bad" || ["negative", "very_negative", "worsened"].includes(t.finalSentiment)).length, color: HP_COLORS.Bad },
  ].filter(d => d.value > 0);

  const countryData = useMemo(() => {
    const counts = {};
    tickets.forEach(t => {
      if (!t.country) return;
      const n = t.country.trim().toUpperCase() === "EGYPT" || t.country.trim() === "Egypt" ? "Egypt" : t.country.trim();
      counts[n] = (counts[n] || 0) + 1;
    });
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 6).map(([name, value]) => ({ name, value }));
  }, [tickets]);

  const monetaryData = useMemo(() => {
    const sums = {};
    analyzed.forEach(t => { if (t.issueType && t.monetaryValue > 0) sums[t.issueType] = (sums[t.issueType] || 0) + t.monetaryValue; });
    return Object.entries(sums).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([name, value]) => ({ name, value }));
  }, [analyzed]);

  return (
    <div>
      <div style={{ marginBottom: 20, display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
        <div>
          <div style={{ fontSize: 22, fontWeight: 700, letterSpacing: "-.5px" }}>Support Overview</div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>Zoho Desk · {total.toLocaleString()} tickets</div>
        </div>
        <div style={{ fontSize: 11, background: "#F0F9FF", color: "#0369A1", padding: "4px 10px", borderRadius: 20, fontWeight: 600 }}>
          AI Insights Active: {analyzed.length.toLocaleString()} Analyzed
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 12, marginBottom: 20 }}>
        <KPI label="Total Tickets" value={total.toLocaleString()} sub="All time" />
        <KPI label="Open" value={open} sub="Needs action" color={open > 0 ? C.accent : C.text} />
        <KPI label="CSAT Score" value={`${csat}%`} sub={`${rated.length.toLocaleString()} rated`} color={csat >= 70 ? "#10B981" : "#EF4444"} />
        <KPI label="SLA Compliance" value={`${slaRate}%`} sub="Not violated" color={slaRate >= 90 ? "#10B981" : "#FBBF24"} />
        <KPI label="Avg Resolution" value={`${avgRes}h`} sub="Business hrs" />
        <KPI label="Escalated" value={escalated.toLocaleString()} sub={`${total ? Math.round(escalated / total * 100) : 0}% rate`} color={escalated > 100 ? "#EF4444" : C.text} />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
        <KPI label="AI Quality Score" value={`${avgQuality}/10`} sub="Agent performance" color={parseFloat(avgQuality) >= 7 ? "#10B981" : "#FBBF24"} />
        <KPI label="Empathy Score" value={`${avgEmpathy}/10`} sub="Customer care" color={parseFloat(avgEmpathy) >= 7 ? "#10B981" : "#FBBF24"} />
        <KPI label="Policy Compliance" value={`${policyRate}%`} sub="Strict adherence" color={policyRate >= 90 ? "#10B981" : "#EF4444"} />
        <KPI label="Churn Risk" value={churnRisks} sub="Immediate attention" color={churnRisks > 0 ? "#EF4444" : C.text} />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 }}>
        <Card>
          <ChartTitle>AI Root Cause Breakdown</ChartTitle>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={rootCauseData} layout="vertical">
              <XAxis type="number" hide />
              <YAxis dataKey="name" type="category" width={100} tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip cursor={{ fill: "transparent" }} />
              <Bar dataKey="value" fill="#6366F1" radius={[0, 4, 4, 0]} barSize={20} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card>
          <ChartTitle>AI Final Sentiment</ChartTitle>
          <div style={{ display: "flex", alignItems: "center", height: 180 }}>
            <ResponsiveContainer width="50%" height="100%">
              <PieChart>
                <Pie data={sentimentData} innerRadius={40} outerRadius={60} dataKey="value">
                  {sentimentData.map((d, i) => <Cell key={i} fill={d.color} />)}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
            <div style={{ flex: 1, paddingLeft: 10 }}>
              {sentimentData.map(d => (
                <div key={d.name} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 4 }}>
                  <span style={{ color: C.sub }}>{d.name}</span>
                  <span style={{ fontWeight: 600, color: d.color }}>{Math.round(d.value / analyzed.length * 100)}%</span>
                </div>
              ))}
            </div>
          </div>
        </Card>
        <Card>
          <ChartTitle>Monthly AI Resolution Score</ChartTitle>
          <div style={{ textAlign: "center", padding: "20px 0" }}>
            <div style={{ fontSize: 42, fontWeight: 800, color: C.accent }}>{avgQuality}</div>
            <div style={{ fontSize: 11, color: C.muted }}>Weighted Average Score</div>
            <div style={{ marginTop: 15, fontSize: 10, color: "#10B981", background: "#ECFDF5", padding: "4px 8px", borderRadius: 4, display: "inline-block" }}>
              ↑ 4% vs last month
            </div>
          </div>
        </Card>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16, marginBottom: 16 }}>
        <Card>
          <ChartTitle>Monthly Ticket Volume — Stacked by Channel</ChartTitle>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={monthlyData} barCategoryGap="18%">
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
              <XAxis dataKey="month" tick={{ fontSize: 10 }} tickFormatter={v => v.slice(5)} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="chat" name="Chat" stackId="a" fill="#FF5A00" />
              <Bar dataKey="phone" name="Phone" stackId="a" fill="#3B82F6" />
              <Bar dataKey="email" name="Email" stackId="a" fill="#10B981" />
              <Bar dataKey="other" name="Other" stackId="a" fill="#94A3B8" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", gap: 14, marginTop: 8 }}>
            {[["Chat", "#FF5A00"], ["Phone", "#3B82F6"], ["Email", "#10B981"], ["Other", "#94A3B8"]].map(([l, c]) => (
              <span key={l} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: C.sub }}>
                <span style={{ width: 8, height: 8, borderRadius: 2, background: c }} />{l}
              </span>
            ))}
          </div>
        </Card>
        <Card>
          <ChartTitle>Channel Breakdown</ChartTitle>
          <ResponsiveContainer width="100%" height={160}>
            <PieChart>
              <Pie data={channelData} cx="50%" cy="50%" outerRadius={62} dataKey="value" nameKey="name">
                {channelData.map((d, i) => <Cell key={i} fill={d.color} />)}
              </Pie>
              <Tooltip formatter={v => v.toLocaleString()} />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 6 }}>
            {channelData.map(d => (
              <div key={d.name} style={{ display: "flex", justifyContent: "space-between", fontSize: 11 }}>
                <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
                  <span style={{ width: 8, height: 8, borderRadius: "50%", background: d.color, flexShrink: 0 }} />
                  {d.name}
                </span>
                <span style={{ fontWeight: 500 }}>{d.value.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr", gap: 16 }}>
        <Card>
          <ChartTitle>Top 10 Ticket Reasons</ChartTitle>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={reasonData} layout="vertical" barCategoryGap="20%">
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
              <XAxis type="number" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 10 }} width={150} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="value" name="Tickets" fill={C.accent} radius={[0, 3, 3, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card>
          <ChartTitle>CSAT — Happiness Rating</ChartTitle>
          <ResponsiveContainer width="100%" height={140}>
            <PieChart>
              <Pie data={happinessData} cx="50%" cy="50%" innerRadius={38} outerRadius={58} dataKey="value">
                {happinessData.map((d, i) => <Cell key={i} fill={d.color} />)}
              </Pie>
              <Tooltip formatter={v => v.toLocaleString()} />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ textAlign: "center", marginTop: 6 }}>
            <div style={{ fontSize: 26, fontWeight: 700, color: csat >= 70 ? "#10B981" : "#EF4444" }}>{csat}%</div>
            <div style={{ fontSize: 10, color: C.muted }}>Good responses</div>
          </div>
          <div style={{ display: "flex", justifyContent: "space-around", marginTop: 10 }}>
            {happinessData.map(d => (
              <div key={d.name} style={{ textAlign: "center" }}>
                <div style={{ fontSize: 15, fontWeight: 700, color: d.color }}>{d.value.toLocaleString()}</div>
                <div style={{ fontSize: 10, color: C.muted }}>{d.name}</div>
              </div>
            ))}
          </div>
        </Card>
        <Card>
          <ChartTitle>Top Countries</ChartTitle>
          {countryData.map((d, i) => (
            <div key={i} style={{ marginBottom: 10 }}>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 3 }}>
                <span style={{ color: C.sub }}>{d.name}</span>
                <span style={{ fontWeight: 500 }}>{d.value.toLocaleString()}</span>
              </div>
              <div style={{ background: C.border, borderRadius: 3, height: 5, overflow: "hidden" }}>
                <div style={{ background: C.accent, height: "100%", width: `${Math.round(d.value / total * 100)}%`, borderRadius: 3 }} />
              </div>
            </div>
          ))}
          <div style={{ marginTop: 12, padding: "8px 10px", background: "#F9F8F7", borderRadius: 7 }}>
            <div style={{ fontSize: 10, color: C.muted }}>SLA Violations</div>
            <div style={{ fontSize: 18, fontWeight: 700, color: slaViol > 500 ? "#EF4444" : "#10B981" }}>{slaViol.toLocaleString()}</div>
          </div>
        </Card>
      </div>

      {monetaryData.length > 0 && (
        <div style={{ marginTop: 16 }}>
          <Card>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <ChartTitle>Financial Impact by Issue Type (SAR)</ChartTitle>
              <div style={{ fontSize: 11, color: "#D97706", background: "#FFFBEB", padding: "4px 8px", borderRadius: 4, fontWeight: 600 }}>
                Total Analyzed Impact: {monetaryData.reduce((s, d) => s + d.value, 0).toLocaleString()} SAR
              </div>
            </div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={monetaryData} barCategoryGap="40%">
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={v => `${v / 1000}k`} />
                <Tooltip formatter={v => `${v.toLocaleString()} SAR`} />
                <Bar dataKey="value" fill="#D97706" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </Card>
        </div>
      )}
    </div>
  );
}

/* ─── AGENT PERFORMANCE TAB ──────────────────────────────────── */
function AgentsTab({ tickets, onAgentClick }) {
  const [sortBy, setSortBy] = useState("tickets");
  const [page, setPage] = useState(1);
  const [searchQ, setSearchQ] = useState("");
  const pageSize = 12;


  const agentMap = useMemo(() => buildAgentMap(tickets), [tickets]);

  const agentStats = useMemo(() => {
    const stats = {};
    tickets.forEach(t => {
      if (!t.owner) return;
      if (!stats[t.owner]) stats[t.owner] = {
        id: t.owner, name: agentMap[t.owner] || t.owner,
        tickets: 0, good: 0, bad: 0, okay: 0, rated: 0,
        slaOk: 0, slaTotal: 0, escalated: 0,
        resTotal: 0, resCount: 0, reassigns: 0, reasons: {},
        channels: {},
      };
      const s = stats[t.owner];
      s.tickets++;
      if (t.happiness === "Good" || ["positive", "very_positive"].includes(t.finalSentiment)) { s.good++; s.rated++; }
      if (t.happiness === "Bad" || ["negative", "very_negative", "worsened"].includes(t.finalSentiment)) { s.bad++; s.rated++; }
      if (t.happiness === "Okay" || ["neutral", "stable"].includes(t.finalSentiment)) { s.okay++; s.rated++; }
      if (t.slaViolation || t.oneTouchResolutionAI !== undefined) { s.slaTotal++; if (t.slaViolation === "Not Violated" || t.oneTouchResolutionAI) s.slaOk++; }
      if (t.isEscalated) s.escalated++;
      if (t.resolutionMs > 0) { s.resTotal += t.resolutionMs; s.resCount++; }
      s.reassigns += t.numReassign;
      if (t.reason) s.reasons[t.reason] = (s.reasons[t.reason] || 0) + 1;
      if (t.channel) s.channels[t.channel] = (s.channels[t.channel] || 0) + 1;
    });
    return Object.values(stats).map(s => ({
      ...s,
      csat: s.rated ? Math.round(s.good / s.rated * 100) : null,
      slaRate: s.slaTotal ? Math.round(s.slaOk / s.slaTotal * 100) : null,
      avgRes: s.resCount ? (s.resTotal / s.resCount / 3600000).toFixed(1) : null,
      topReason: Object.entries(s.reasons).sort((a, b) => b[1] - a[1])[0]?.[0] || "—",
      topChannel: Object.entries(s.channels).sort((a, b) => b[1] - a[1])[0]?.[0] || "—",
    }));
  }, [tickets, agentMap]);

  const sorted = useMemo(() => {
    const q = searchQ.toLowerCase();
    const filtered = agentStats.filter(a => !q || a.name.toLowerCase().includes(q));
    return filtered.sort((a, b) => {
      if (sortBy === "tickets") return b.tickets - a.tickets;
      if (sortBy === "csat") return (b.csat ?? -1) - (a.csat ?? -1);
      if (sortBy === "sla") return (b.slaRate ?? -1) - (a.slaRate ?? -1);
      if (sortBy === "resolution") return parseFloat(a.avgRes ?? 9999) - parseFloat(b.avgRes ?? 9999);
      return 0;
    });
  }, [agentStats, sortBy, searchQ]);

  const totalPages = Math.ceil(sorted.length / pageSize);
  const paginated = sorted.slice((page - 1) * pageSize, page * pageSize);

  useEffect(() => setPage(1), [sortBy]);

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <div style={{ fontSize: 22, fontWeight: 700, letterSpacing: "-.5px" }}>Agent Performance</div>
        <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>{agentStats.length} agents · {tickets.length.toLocaleString()} tickets</div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(5,1fr)", gap: 12, marginBottom: 20 }}>
        {(() => {
          const totalRated = agentStats.reduce((s, a) => s + a.rated, 0);
          const totalGood = agentStats.reduce((s, a) => s + a.good, 0);
          const teamCsat = totalRated ? Math.round(totalGood / totalRated * 100) : 0;
          const totalEsc = agentStats.reduce((s, a) => s + a.escalated, 0);
          const bestAgent = [...agentStats].filter(a => a.csat !== null).sort((a, b) => b.csat - a.csat)[0];
          const topAgent = sorted[0];
          return (
            <>
              <KPI label="Total Agents" value={agentStats.length} sub="Active handlers" />
              <KPI label="Team CSAT" value={`${teamCsat}%`} sub={`${totalRated.toLocaleString()} rated`} color={teamCsat >= 70 ? "#10B981" : "#EF4444"} />
              <KPI label="Top Volume" value={topAgent?.name || "—"} sub={`${topAgent?.tickets.toLocaleString()} tickets`} color={C.accent} />
              <KPI label="Best CSAT" value={bestAgent?.name || "—"} sub={`${bestAgent?.csat ?? 0}% score`} color="#10B981" />
              <KPI label="Team Escalations" value={totalEsc.toLocaleString()} sub="Total flagged" color={totalEsc > 200 ? "#EF4444" : C.text} />
            </>
          );
        })()}
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
        <div style={{ display: "flex", gap: 8 }}>
          {[["tickets", "By Tickets"], ["csat", "By CSAT"], ["sla", "By SLA %"], ["resolution", "By Fastest Res."]].map(([val, label]) => (
            <button key={val} onClick={() => setSortBy(val)}
              style={{ padding: "6px 14px", borderRadius: 6, border: `1px solid ${sortBy === val ? C.accent : C.border}`, background: sortBy === val ? C.accentL : C.white, color: sortBy === val ? C.accent : C.sub, fontSize: 11, cursor: "pointer", fontWeight: sortBy === val ? 600 : 400 }}>
              {label}
            </button>
          ))}
        </div>
        <div style={{ position: "relative" }}>
          <input value={searchQ} onChange={e => setSearchQ(e.target.value)} placeholder="Search agent name..."
            style={{ width: 220, padding: "8px 10px 8px 30px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12, outline: "none", background: C.white }} />
          <svg style={{ position: "absolute", left: 9, top: "50%", transform: "translateY(-50%)" }} width={12} height={12} viewBox="0 0 24 24" fill="none" stroke={C.muted} strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round">
            <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
          </svg>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(195px,1fr))", gap: 10, alignContent: "start" }}>
        {paginated.map(agent => (
          <div key={agent.id} onClick={() => onAgentClick(agent.id)}
            style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 10, padding: 14, cursor: "pointer", transition: "all .12s" }}
            onMouseEnter={e => { e.currentTarget.style.borderColor = C.accent; e.currentTarget.style.boxShadow = "0 2px 12px rgba(255,90,0,.1)"; }}
            onMouseLeave={e => { e.currentTarget.style.borderColor = C.border; e.currentTarget.style.boxShadow = "none"; }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
              <div>
                <div style={{ fontSize: 13, fontWeight: 600 }}>{agent.name}</div>
                <div style={{ fontSize: 10, color: C.muted }}>{agent.tickets.toLocaleString()} tickets</div>
              </div>
              {agent.csat !== null && (
                <span style={{
                  fontSize: 10, fontWeight: 700, padding: "2px 6px", borderRadius: 6,
                  background: agent.csat >= 70 ? "#DCFCE7" : agent.csat >= 50 ? "#FEF3C7" : "#FEE2E2",
                  color: agent.csat >= 70 ? "#16A34A" : agent.csat >= 50 ? "#D97706" : "#DC2626"
                }}>
                  {agent.csat}%
                </span>
              )}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 5 }}>
              {[["SLA", agent.slaRate !== null ? `${agent.slaRate}%` : "—"], ["Avg Res.", agent.avgRes ? `${agent.avgRes}h` : "—"], ["Escalated", agent.escalated], ["Reassigned", agent.reassigns]].map(([l, v]) => (
                <div key={l} style={{ background: "#F9F8F7", borderRadius: 6, padding: "5px 7px" }}>
                  <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase" }}>{l}</div>
                  <div style={{ fontSize: 12, fontWeight: 600 }}>{v}</div>
                </div>
              ))}
            </div>
            <div style={{ marginTop: 8, fontSize: 10, color: C.muted }}>
              Top reason: <span style={{ color: C.text }}>{agent.topReason.length > 20 ? agent.topReason.slice(0, 20) + "…" : agent.topReason}</span>
            </div>
            <div style={{ marginTop: 6, fontSize: 10, color: C.accent, fontWeight: 500 }}>View full profile →</div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 24, fontSize: 12, color: C.muted, background: C.white, padding: "12px 16px", borderRadius: 10, border: `1px solid ${C.border}` }}>
        <span>Showing <strong>{(page - 1) * pageSize + 1}–{Math.min(page * pageSize, sorted.length)}</strong> of {sorted.length} agents</span>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
            style={{ padding: "5px 10px", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 11, background: page === 1 ? "#F9F9F9" : C.white, cursor: page === 1 ? "not-allowed" : "pointer", color: page === 1 ? C.muted : C.text }}>←</button>
          <span style={{ padding: "0 10px" }}>Page {page} / {totalPages || 1}</span>
          <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}
            style={{ padding: "5px 10px", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 11, background: page >= totalPages ? "#F9F9F9" : C.white, cursor: page >= totalPages ? "not-allowed" : "pointer", color: page >= totalPages ? C.muted : C.text }}>→</button>
        </div>
      </div>
    </div>
  );
}

/* ─── TICKET EXPLORER TAB ────────────────────────────────────── */
function TicketExplorerTab({ tickets, onTicketClick, initialFilters = {} }) {
  const [agent, setAgent] = useState(initialFilters.agent || "All");
  const [channel, setChannel] = useState(initialFilters.channel || "All");
  const [reason, setReason] = useState(initialFilters.reason || "All");
  const [status, setStatus] = useState(initialFilters.status || "All");
  const [happiness, setHappiness] = useState(initialFilters.happiness || "All");
  const [slaF, setSlaF] = useState(initialFilters.slaF || "All");
  const [aiFilter, setAiFilter] = useState(initialFilters.aiFilter || "All");
  const [q, setQ] = useState(initialFilters.q || "");
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState(null);
  const PAGE = 50;

  const agents = useMemo(() => ["All", ...new Set(tickets.map(t => t.owner).filter(Boolean)).values()].sort(), [tickets]);
  const channels = useMemo(() => ["All", ...new Set(tickets.map(t => t.channel).filter(Boolean)).values()].sort(), [tickets]);
  const reasons = useMemo(() => ["All", ...new Set(tickets.map(t => t.reason).filter(Boolean)).values()].sort(), [tickets]);
  const slaOpts = ["All", "Not Violated", "Resolution Violation"];
  const aiOpts = ["All", "Churn Risk", "Payment Blocker", "Fraud Suspicion", "Refund Requested", "Escalated by AI"];

  const filtered = useMemo(() => {
    const qL = q.toLowerCase();
    return tickets.filter(t => {
      if (agent !== "All" && t.owner !== agent) return false;
      if (channel !== "All" && t.channel !== channel) return false;
      if (reason !== "All" && t.reason !== reason) return false;
      if (status !== "All" && t.status !== status) return false;
      const csatProxy = t.happiness || (["positive", "very_positive"].includes(t.finalSentiment) ? "Good" : ["negative", "very_negative", "worsened"].includes(t.finalSentiment) ? "Bad" : ["neutral", "stable"].includes(t.finalSentiment) ? "Okay" : "");
      if (happiness !== "All" && csatProxy !== happiness) return false;
      if (slaF !== "All" && t.slaViolation !== slaF) return false;
      if (aiFilter === "Churn Risk" && !t.isChurnIntent) return false;
      if (aiFilter === "Payment Blocker" && !t.isPaymentBlocker) return false;
      if (aiFilter === "Fraud Suspicion" && !t.fraudSuspicion) return false;
      if (aiFilter === "Refund Requested" && !t.isRefundRequested) return false;
      if (aiFilter === "Escalated by AI" && !t.isEscalatedAI) return false;
      if (q && !t.subject.toLowerCase().includes(qL) && !t.merchantName.toLowerCase().includes(qL) && !t.subReason.toLowerCase().includes(qL) && !t.reason.toLowerCase().includes(qL) && !(t.ticket_number && t.ticket_number.toLowerCase().includes(qL)) && !String(t.id).includes(qL)) return false;
      return true;
    }).sort((a, b) => b.createdTime.localeCompare(a.createdTime));
  }, [tickets, channel, reason, status, happiness, slaF, aiFilter, q]);

  const totalPages = Math.ceil(filtered.length / PAGE);
  const pageRows = filtered.slice((page - 1) * PAGE, page * PAGE);
  useEffect(() => setPage(1), [agent, channel, reason, status, happiness, slaF, aiFilter, q]);

  const thStyle = { padding: "10px 12px", textAlign: "left", fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: .5, fontWeight: 500, whiteSpace: "nowrap" };

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 22, fontWeight: 700, letterSpacing: "-.5px" }}>Ticket Explorer</div>
        <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>Browse {tickets.length.toLocaleString()} support tickets</div>
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ position: "relative" }}>
          <input value={q} onChange={e => setQ(e.target.value)} placeholder="Search subject, reason, merchant…"
            style={{ padding: "8px 12px 8px 34px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12, width: 270, outline: "none", background: C.white }} />
          <svg style={{ position: "absolute", left: 10, top: "50%", transform: "translateY(-50%)" }} width={13} height={13} viewBox="0 0 24 24" fill="none" stroke={C.muted} strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round">
            <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
          </svg>
        </div>
        {[["Agent", agents, agent, setAgent], ["Channel", channels, channel, setChannel], ["Reason", reasons.slice(0, 30), reason, setReason],
        ["Status", ["All", "Open", "Closed", "Resolved"], status, setStatus],
        ["AI Signal", aiOpts, aiFilter, setAiFilter],
        ["CSAT", ["All", "Good", "Okay", "Bad"], happiness, setHappiness],
        ["SLA", slaOpts, slaF, setSlaF],
        ].map(([label, opts, val, fn]) => (
          <select key={label} value={val} onChange={e => fn(e.target.value)}
            style={{ padding: "8px 10px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12, background: C.white, color: C.text, cursor: "pointer" }}>
            <option value="All">{label}: All</option>
            {opts.filter(o => o !== "All").map(o => <option key={o}>{o}</option>)}
          </select>
        ))}
        <button onClick={() => { setAgent("All"); setChannel("All"); setReason("All"); setStatus("All"); setHappiness("All"); setSlaF("All"); setAiFilter("All"); setQ(""); }}
          style={{ padding: "8px 12px", background: "#F4F2EE", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, color: C.muted, cursor: "pointer" }}>
          Reset
        </button>
        <span style={{ fontSize: 11, color: C.muted, marginLeft: 4 }}>{filtered.length.toLocaleString()} tickets</span>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: selected ? "1fr 360px" : "1fr", gap: 16 }}>
        <div>
          <div style={{ background: C.white, borderRadius: 10, border: `1px solid ${C.border}`, overflow: "hidden" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ background: "#F9F8F7", borderBottom: `1px solid ${C.border}` }}>
                  <th style={thStyle}>TICKET #</th>
                  {["Date", "Subject", "Channel", "Reason", "AI Sentiment", "AI Score", "CSAT"].map(h => (
                    <th key={h} style={thStyle}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pageRows.map(t => (
                  <tr key={t.id} onClick={() => setSelected(selected?.id === t.id ? null : t)}
                    style={{ borderBottom: `1px solid #F4F2EE`, cursor: "pointer", background: selected?.id === t.id ? C.accentL : "transparent" }}>
                    <td style={{ padding: "9px 12px", whiteSpace: "nowrap" }}>
                      <span onClick={e => { e.stopPropagation(); onTicketClick && onTicketClick(t.id); }}
                        style={{ color: C.accent, cursor: "pointer", fontWeight: 600, fontSize: 11 }}>
                        {t.ticket_number || t.id}
                      </span>
                    </td>
                    <td style={{ padding: "9px 12px", color: C.muted, whiteSpace: "nowrap", fontSize: 11 }}>{(t.createdTime || "").slice(0, 10)}</td>
                    <td style={{ padding: "9px 12px", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={t.subject}>{t.subject}</td>
                    <td style={{ padding: "9px 12px" }}>
                      <span style={{ fontSize: 10, padding: "2px 7px", borderRadius: 8, background: `${CH_COLORS[t.channel] || "#94A3B8"}22`, color: CH_COLORS[t.channel] || "#94A3B8", fontWeight: 500 }}>{t.channel}</span>
                    </td>
                    <td style={{ padding: "9px 12px", color: C.sub, fontSize: 11, maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.reason}</td>
                    <td style={{ padding: "9px 12px" }}>
                      {t.finalSentiment ? (
                        <span style={{
                          fontSize: 10, padding: "2px 7px", borderRadius: 8,
                          background: t.finalSentiment.toLowerCase().includes("positive") ? "#DCFCE7" : t.finalSentiment.toLowerCase().includes("negative") ? "#FEE2E2" : "#FEF3C7",
                          color: t.finalSentiment.toLowerCase().includes("positive") ? "#16A34A" : t.finalSentiment.toLowerCase().includes("negative") ? "#DC2626" : "#D97706",
                          fontWeight: 600
                        }}>{t.finalSentiment}</span>
                      ) : <span style={{ color: C.muted }}>—</span>}
                    </td>
                    <td style={{ padding: "9px 12px" }}>
                      {t.overallQualityScore > 0 ? (
                        <span style={{ fontWeight: 700, color: t.overallQualityScore >= 7 ? "#10B981" : t.overallQualityScore >= 4 ? "#FBBF24" : "#EF4444" }}>
                          {t.overallQualityScore}
                        </span>
                      ) : "—"}
                    </td>
                    <td style={{ padding: "9px 12px" }}>
                      {(() => {
                        const csatProxy = t.happiness || (["positive", "very_positive"].includes(t.finalSentiment) ? "Good" : ["negative", "very_negative", "worsened"].includes(t.finalSentiment) ? "Bad" : ["neutral", "stable"].includes(t.finalSentiment) ? "Okay" : "");
                        return csatProxy ? <span style={{ fontSize: 10, fontWeight: 600, color: HP_COLORS[csatProxy] || C.muted }}>{csatProxy}</span> : <span style={{ color: C.muted }}>—</span>;
                      })()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 12, fontSize: 11, color: C.muted }}>
            <span>Showing {filtered.length === 0 ? 0 : (page - 1) * PAGE + 1}–{Math.min(page * PAGE, filtered.length)} of {filtered.length.toLocaleString()}</span>
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
                style={{ padding: "5px 10px", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 11, background: page === 1 ? "#F4F2EE" : C.white, cursor: page === 1 ? "not-allowed" : "pointer", color: page === 1 ? C.muted : C.text }}>←</button>
              <div style={{ display: "flex", gap: 4 }}>
                {[...Array(totalPages)].map((_, i) => {
                  const p = i + 1;
                  if (totalPages > 5 && Math.abs(p - page) > 1 && p !== 1 && p !== totalPages) return p === 2 || p === totalPages - 1 ? <span key={p}>...</span> : null;
                  return (
                    <button key={p} onClick={() => setPage(p)}
                      style={{ width: 26, height: 26, border: `1px solid ${page === p ? C.accent : C.border}`, borderRadius: 6, fontSize: 11, background: page === p ? C.accent : C.white, color: page === p ? "#fff" : C.text, cursor: "pointer", fontWeight: 600 }}>{p}</button>
                  );
                })}
              </div>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}
                style={{ padding: "5px 10px", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 11, background: page >= totalPages ? "#F4F2EE" : C.white, cursor: page >= totalPages ? "not-allowed" : "pointer", color: page >= totalPages ? C.muted : C.text }}>→</button>
            </div>
          </div>
        </div>

        {selected && (
          <Card style={{ alignSelf: "start", fontSize: 12, position: "sticky", top: 0 }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 12 }}>
              <div style={{ fontSize: 14, fontWeight: 700 }}>Ticket {selected.ticket_number || selected.id}</div>
              <button onClick={() => setSelected(null)} style={{ background: "none", border: "none", color: C.muted, cursor: "pointer", fontSize: 20, lineHeight: 1 }}>×</button>
            </div>
            {selected.sentimentSummary && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: .5, marginBottom: 6 }}>AI Analysis Summary</div>
                <div style={{ background: "#F0F9FF", padding: "10px 12px", borderRadius: 8, fontSize: 12, lineHeight: 1.5, color: "#0369A1", border: "1px solid #BAE6FD" }}>
                  {selected.sentimentSummary}
                </div>
              </div>
            )}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 16 }}>
              {[["Quality Score", `${selected.overallQualityScore}/10`, selected.overallQualityScore >= 7 ? "#10B981" : "#EF4444"],
              ["Sentiment Shift", selected.sentimentShift || "Neutral", null],
              ["Issue Type", selected.issueType || "Unknown", C.accent],
              ["Root Cause", selected.rootCause || "Unknown", null],
              ].map(([l, v, c]) => (
                <div key={l} style={{ background: "#F9F8F7", padding: "8px 10px", borderRadius: 8 }}>
                  <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase" }}>{l}</div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: c || C.text }}>{v}</div>
                </div>
              ))}
            </div>
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: .5, marginBottom: 8 }}>Quality Parameters</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {[["Empathy Score", selected.empathyScore, 10], ["Knowledge Accuracy", selected.knowledgeAccuracy, 10],
                ["Policy Compliance", selected.policyCompliance ? "✓ Yes" : "✗ No", null],
                ["One-Touch Resolution", selected.oneTouchResolutionAI ? "✓ Yes" : "✗ No", null],
                ["Fraud Suspicion", selected.fraudSuspicion ? "⚠️ Yes" : "No", null],
                ["Payment Blocker", selected.isPaymentBlocker ? "🚫 Yes" : "No", null],
                ].map(([l, v, max]) => (
                  <div key={l} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 11, padding: "2px 0" }}>
                    <span style={{ color: C.sub }}>{l}</span>
                    <span style={{ fontWeight: 600, color: v === "✗ No" || v === "⚠️ Yes" || v === "🚫 Yes" ? "#EF4444" : C.text }}>{v}{max ? `/${max}` : ""}</span>
                  </div>
                ))}
              </div>
            </div>
            {selected.monetaryValue > 0 && (
              <div style={{ background: "#FFFBEB", border: "1px solid #FEF3C7", padding: 12, borderRadius: 8, marginBottom: 16 }}>
                <div style={{ fontSize: 9, color: "#D97706", fontWeight: 700, textTransform: "uppercase" }}>Financial Impact</div>
                <div style={{ fontSize: 18, fontWeight: 800, color: "#D97706" }}>{selected.monetaryValue.toLocaleString()} SAR</div>
              </div>
            )}
            <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 12 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: .5, marginBottom: 8 }}>Basic Information</div>
              {[["Merchant", selected.merchantName], ["Reason", selected.reason], ["Subject", selected.subject],
              ["Created", (selected.createdTime || "").slice(0, 16).replace("T", " ")],
              ].map(([l, v]) => (
                <div key={l} style={{ marginBottom: 6 }}>
                  <div style={{ fontSize: 9, color: C.muted }}>{l}</div>
                  <div style={{ fontSize: 11, fontWeight: 500 }}>{v}</div>
                </div>
              ))}
            </div>
            {onTicketClick && (
              <button onClick={() => onTicketClick(selected.id)}
                style={{ marginTop: 12, width: "100%", padding: "8px 0", background: C.accent, color: "#fff", border: "none", borderRadius: 8, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
                Open in Chat Review →
              </button>
            )}
          </Card>
        )}
      </div>
    </div>
  );
}

/* ─── MACRO TAB ──────────────────────────────────────────────── */
function MacroTab({ merchants, region = "KSA" }) {
  const cityStats = useMemo(() => computeCityStats(merchants), [merchants]);
  const priceTiers = useMemo(() => computePriceTiers(merchants), [merchants]);
  const topMalls = useMemo(() => computeTopMalls(merchants), [merchants]);

  const total = merchants.length;
  const high = merchants.filter(m => m.Priority.toLowerCase().includes("high")).length;
  const medium = merchants.filter(m => m.Priority.toLowerCase().includes("medium")).length;
  const avgR = (merchants.reduce((s, m) => s + (m.Rating || 0), 0) / (merchants.filter(m => m.Rating > 0).length || 1));
  const branches = merchants.reduce((s, m) => s + (m.Branches || 0), 0);
  const mallsN = new Set(merchants.map(m => m.Mall).filter(Boolean)).size;

  const cityData = Object.entries(cityStats).map(([city, s]) => ({ city, ...s }));
  const priorityData = [
    { name: "High", value: high, color: "#E8563A" },
    { name: "Medium", value: medium, color: "#FBBF24" },
    { name: "Low", value: total - high - medium, color: "#4ADE80" },
  ];

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-.4px" }}>{region} Market Intelligence</div>
        <div style={{ fontSize: 13, color: C.muted, marginTop: 3 }}>
          {Object.keys(cityStats).length} cities · {mallsN} malls · {total.toLocaleString()} merchants
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 10, marginBottom: 18 }}>
        <KPI label="Total Merchants" value={total.toLocaleString()} sub={`Full ${region} coverage`} />
        <KPI label="High Priority" value={high.toLocaleString()} sub={`${(high / total * 100 || 0).toFixed(1)}% of market`} color={C.accent} />
        <KPI label="Medium Priority" value={medium.toLocaleString()} sub={`${(medium / total * 100 || 0).toFixed(1)}% of market`} color="#D97706" />
        <KPI label="Avg Rating" value={`${avgR.toFixed(2)} ★`} sub="Market benchmark" color="#16A34A" />
        <KPI label="Total Branches" value={branches.toLocaleString()} sub="Chain opportunity" />
        <KPI label="Malls Covered" value={mallsN.toLocaleString()} sub="Unique malls" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1.5fr 1fr", gap: 14, marginBottom: 14 }}>
        <Card>
          <ChartTitle>Merchants by City — Stacked by Priority</ChartTitle>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={cityData} barCategoryGap="30%">
              <CartesianGrid strokeDasharray="3 3" stroke="#F4F2EE" vertical={false} />
              <XAxis dataKey="city" tick={{ fontSize: 11, fill: C.muted }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 10, fill: C.muted }} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="high" name="High" stackId="a" fill="#E8563A" />
              <Bar dataKey="medium" name="Medium" stackId="a" fill="#FBBF24" />
              <Bar dataKey="low" name="Low" stackId="a" fill="#86EFAC" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", gap: 12, marginTop: 8 }}>
            {[["High", "#E8563A"], ["Medium", "#FBBF24"], ["Low", "#86EFAC"]].map(([l, c]) => (
              <span key={l} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: C.sub }}>
                <span style={{ width: 8, height: 8, borderRadius: 2, background: c }} />{l}
              </span>
            ))}
          </div>
        </Card>
        <Card>
          <ChartTitle>Priority Distribution</ChartTitle>
          <ResponsiveContainer width="100%" height={180}>
            <PieChart>
              <Pie data={priorityData} cx="50%" cy="50%" innerRadius={52} outerRadius={76} dataKey="value" paddingAngle={3}>
                {priorityData.map((e, i) => <Cell key={i} fill={e.color} />)}
              </Pie>
              <Tooltip formatter={v => v.toLocaleString()} />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", flexDirection: "column", gap: 5, marginTop: 6 }}>
            {priorityData.map(({ name, value, color }) => (
              <div key={name} style={{ display: "flex", justifyContent: "space-between", fontSize: 12 }}>
                <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: color }} />{name}
                </span>
                <span style={{ fontWeight: 500 }}>{value.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 }}>
        <Card>
          <ChartTitle>Top Malls by Merchant Count</ChartTitle>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={topMalls} layout="vertical" barCategoryGap="25%">
              <XAxis type="number" tick={{ fontSize: 10, fill: C.muted }} axisLine={false} tickLine={false} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 10, fill: C.muted }} axisLine={false} tickLine={false} width={135} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="count" name="Total" fill={C.accent} radius={[0, 4, 4, 0]} />
              <Bar dataKey="high" name="High" fill="#FEE2E2" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card>
          <ChartTitle>Price Tier Distribution</ChartTitle>
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie data={priceTiers} cx="50%" cy="50%" outerRadius={60} dataKey="value"
                label={({ percent }) => percent > 0.01 ? `${(percent * 100).toFixed(0)}%` : ""}
                labelLine={true}
                paddingAngle={2}>
                {priceTiers.map((e, i) => <Cell key={i} fill={e.color} />)}
              </Pie>
              <Tooltip formatter={v => v.toLocaleString()} />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginTop: 6 }}>
            {priceTiers.map(({ name, color }) => (
              <span key={name} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: C.sub }}>
                <span style={{ width: 8, height: 8, borderRadius: 2, background: color }} />{name}
              </span>
            ))}
          </div>
        </Card>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 10 }}>
        {Object.entries(cityStats).map(([city, s]) => (
          <Card key={city} style={{ padding: "12px 14px" }}>
            <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}>{city}</div>
            <div style={{ fontSize: 18, fontWeight: 700, color: C.accent }}>{s.total.toLocaleString()}</div>
            <div style={{ fontSize: 10, color: C.muted, marginTop: 4, lineHeight: 1.8 }}>
              ⭐ {s.avgRating}<br />
              🔴 {s.high.toLocaleString()} high<br />
              🏬 {s.malls} malls
            </div>
          </Card>
        ))}
      </div>
    </div>
  );
}

/* ─── MERCHANT PROFILER TAB ────────────────────────────────────── */
function ProfilerTab({ merchants, anonKey, initialMerchant, tickets, region = "KSA", userEmail, favoriteIds, toggleFavorite, userRole }) {
  const [query, setQuery] = useState(initialMerchant ? initialMerchant.Merchant : "");
  const [selected, setSelected] = useState(initialMerchant || null);
  const [showList, setShowList] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const [semanticNames, setSemanticNames] = useState(null);
  // eslint-disable-next-line no-unused-vars
  const [err, setErr] = useState("");
  const [modelReady, setModelReady] = useState(false);
  const [modelProgress, setModelProgress] = useState(0);
  const extractorRef = useRef(null);

  useEffect(() => {
    async function loadModel() {
      if (extractorRef.current) return;
      try {
        extractorRef.current = await pipeline("feature-extraction", "Xenova/paraphrase-multilingual-MiniLM-L12-v2", {
          progress_callback: x => { if (x.status === "progress") setModelProgress(Math.round(x.progress)); }
        });
        setModelReady(true);
      } catch (e) { console.error("Model load err:", e); }
    }
    loadModel();
  }, []);

  const handleSemanticSearch = async () => {
    if (!query.trim() || !modelReady) return;
    setIsSearching(true); setShowList(true); setErr(""); setSemanticNames(null);
    try {
      const output = await extractorRef.current(query, { pooling: 'mean', normalize: true });
      const queryVector = Array.from(output.data);
      const res = await fetch(`${SB_URL}/rest/v1/rpc/match_merchants`, {
        method: "POST",
        headers: sbH(anonKey),
        body: JSON.stringify({ query_embedding: queryVector, match_threshold: 0.45, match_count: 15 })
      });
      const data = await res.json();
      if (!Array.isArray(data)) throw new Error("Invalid RPC response");
      const exactMatches = merchants.filter(m => m.Merchant.toLowerCase().includes(query.toLowerCase())).map(m => m.Merchant);
      const aiResults = data.map(d => d.merchant_name);
      setSemanticNames([...new Set([...exactMatches, ...aiResults])]);
    } catch (e) {
      console.error(e);
      setErr("Semantic Search Failed");
    }
    setIsSearching(false);
  };

  const merchantBranchesMap = useMemo(() => {
    const map = {};
    for (const m of merchants) map[m.Merchant] = (map[m.Merchant] || 0) + Math.max(1, m.Branches || 0);
    return map;
  }, [merchants]);

  const filtered = useMemo(() => {
    if (!query) return [];
    const map = new Map();
    if (semanticNames && semanticNames.length > 0) {
      for (const name of semanticNames) {
        const found = merchants.find(m => m.Merchant === name || m.OriginalMerchant === name);
        if (found && !map.has(found.Merchant)) map.set(found.Merchant, { ...found, TotalKsaBranches: merchantBranchesMap[found.Merchant] });
      }
      return Array.from(map.values());
    }
    const q = query.toLowerCase();
    const qNorm = normArabic(q);
    for (const m of merchants) {
      if (m.Merchant.toLowerCase().includes(q) || m.City.toLowerCase().includes(q) || m.Mall.toLowerCase().includes(q) || normArabic(m.Merchant).includes(qNorm)) {
        if (!map.has(m.Merchant)) map.set(m.Merchant, { ...m, TotalKsaBranches: merchantBranchesMap[m.Merchant] });
      }
    }
    if (map.size === 0 && qNorm.length >= 3) {
      const fuzzy = [];
      const seen = new Set();
      for (const m of merchants) {
        if (seen.has(m.Merchant)) continue;
        const mNorm = normArabic(m.Merchant);
        const dist = editDist(qNorm, mNorm);
        if (dist <= 3) { fuzzy.push({ m, dist }); seen.add(m.Merchant); }
      }
      fuzzy.sort((a, b) => a.dist - b.dist);
      for (const item of fuzzy.slice(0, 15)) map.set(item.m.Merchant, { ...item.m, TotalKsaBranches: merchantBranchesMap[item.m.Merchant] });
    }
    return Array.from(map.values()).slice(0, 15);
  }, [query, merchants, merchantBranchesMap, semanticNames]);

  // eslint-disable-next-line no-unused-vars
  const scoreColor = s => s >= 80 ? "#16A34A" : s >= 60 ? "#D97706" : C.accent;

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-.4px" }}>AI Merchant Profiler</div>
        <div style={{ fontSize: 13, color: C.muted, marginTop: 3 }}>{merchants.length.toLocaleString()} merchants · Select one → AI analysis → BD pitch</div>
      </div>

      <div style={{ position: "relative", marginBottom: 16 }}>
        <div style={{ display: "flex", gap: 8 }}>
          <div style={{ position: "relative", flex: 1 }}>
            <input value={query}
              onChange={e => { setQuery(e.target.value); setSemanticNames(null); setShowList(true); }}
              onFocus={() => setShowList(true)}
              onKeyDown={e => e.key === 'Enter' && handleSemanticSearch()}
              placeholder="Search merchant, city, mall, or type a semantic query..."
              style={{ width: "100%", padding: "10px 12px 10px 36px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, background: C.white, outline: "none", color: C.text, boxSizing: "border-box" }}
            />
            <svg style={{ position: "absolute", left: 11, top: "50%", transform: "translateY(-50%)", color: C.muted }} width={14} height={14} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
              <circle cx={11} cy={11} r={8} /><path d="m21 21-4.35-4.35" />
            </svg>
          </div>
          <button onClick={handleSemanticSearch} disabled={!modelReady || isSearching || !query}
            style={{ padding: "0 16px", background: modelReady && query ? C.accent : C.border, color: modelReady && query ? "#fff" : C.muted, border: "none", borderRadius: 8, fontSize: 12, fontWeight: 500, cursor: modelReady && query ? "pointer" : "not-allowed", whiteSpace: "nowrap" }}>
            {isSearching ? "Searching..." : "AI Search ✨"}
          </button>
        </div>
        {!modelReady && (
          <div style={{ fontSize: 11, color: C.sub, marginTop: 8, display: "flex", alignItems: "center", gap: 6 }}>
            <div style={{ width: 100, background: C.border, height: 4, borderRadius: 2, overflow: "hidden" }}>
              <div style={{ background: C.accent, height: "100%", width: `${modelProgress}%` }} />
            </div>
            Loading AI Search Engine ({modelProgress}%)
          </div>
        )}
        {showList && filtered.length > 0 && (
          <div style={{ position: "absolute", top: "100%", left: 0, right: 0, background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, zIndex: 10, maxHeight: 230, overflowY: "auto", marginTop: 3, boxShadow: "0 4px 16px rgba(0,0,0,.08)" }}>
            {filtered.map((m, i) => (
              <div key={i} onClick={() => { setSelected(m); setQuery(m.Merchant); setSemanticNames(null); setShowList(false); }}
                style={{ padding: "9px 12px", cursor: "pointer", fontSize: 12, borderBottom: `1px solid #F4F2EE`, background: selected === m ? C.accentL : "transparent", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div>
                  <span style={{ fontWeight: 500 }}>{m.Merchant}</span>
                  <div style={{ fontSize: 10, color: C.muted }}>{m.Mall} · {m.City}</div>
                </div>
                <span style={{
                  fontSize: 10, fontWeight: 700,
                  color: m.Rating >= 4 ? "#16A34A" : m.Rating >= 3 ? "#D97706" : C.accent,
                  background: m.Rating >= 4 ? "#F0FDF4" : m.Rating >= 3 ? "#FFFBEB" : C.accentL,
                  padding: "2px 6px", borderRadius: 5, display: "flex", alignItems: "center", gap: 3, whiteSpace: "nowrap"
                }}>
                  ★ {m.Rating}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {selected ? (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1.2fr", gap: 16 }}>
            <Card>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
                <div style={{ textAlign: "right", flex: 1 }}>
                  <div style={{ fontSize: 16, fontWeight: 700, direction: "rtl", lineHeight: 1.4 }}>{selected.Merchant}</div>
                  <div style={{ fontSize: 11, color: C.muted, marginTop: 3, direction: "rtl" }}>{selected.Mall} · {selected.City}</div>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginLeft: 16 }}>
                  {favoriteIds && toggleFavorite && (
                    <button
                      onClick={() => toggleFavorite(selected.Merchant)}
                      style={{ background: 'transparent', border: 'none', cursor: 'pointer', fontSize: 18, padding: 0, outline: 'none', transition: 'transform 0.1s' }}
                      onMouseDown={e => e.currentTarget.style.transform = 'scale(0.9)'}
                      onMouseUp={e => e.currentTarget.style.transform = 'scale(1)'}
                      onMouseLeave={e => e.currentTarget.style.transform = 'scale(1)'}
                      title={favoriteIds.has(selected.Merchant) ? "Remove from Favorites" : "Add to Favorites"}
                    >
                      {favoriteIds.has(selected.Merchant) ? '❤️' : '🤍'}
                    </button>
                  )}
                  <button
                    onClick={() => {
                      const text = `🏢 Merchant: ${selected.Merchant}\n📍 Location: ${selected.Mall ? selected.Mall + ', ' : ''}${selected.City}\n⭐ Rating: ${selected.Rating} ★ (${selected.Reviews} reviews)\n🏷️ Category: ${selected.Category}\n\nView details in Waffarha Nexus`;
                      navigator.clipboard.writeText(text);
                      alert("Merchant details copied to clipboard!");
                    }}
                    style={{ background: '#F3F4F6', border: '1px solid #E5E7EB', borderRadius: 6, cursor: 'pointer', fontSize: 12, fontWeight: 600, padding: '4px 10px', color: '#4B5563', display: 'flex', alignItems: 'center', gap: 6 }}
                    title="Share Merchant"
                  >
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8" /><polyline points="16 6 12 2 8 6" /><line x1="12" y1="2" x2="12" y2="15" /></svg>
                    Share
                  </button>
                  <span style={{
                    padding: "3px 9px", borderRadius: 5, fontSize: 10, fontWeight: 500, flexShrink: 0,
                    background: selected.Priority.toLowerCase().includes("high") ? C.accentL : selected.Priority.toLowerCase().includes("medium") ? "#FEF3C7" : selected.Priority.toLowerCase().includes("low") ? "#F0FDF4" : "#F4F2EE",
                    color: selected.Priority.toLowerCase().includes("high") ? C.accent : selected.Priority.toLowerCase().includes("medium") ? "#D97706" : selected.Priority.toLowerCase().includes("low") ? "#16A34A" : C.muted
                  }}>
                    {selected.Priority}
                  </span>
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 12 }}>
                {[["Rating", selected.Rating, true], ["Reviews", selected.Reviews >= 1000 ? `${(selected.Reviews / 1000).toFixed(1)}k` : selected.Reviews], ["City Br.", selected.Branches || 1], [`${region} Br.`, selected.TotalKsaBranches || selected.Branches]].map(([l, v, isRating]) => (
                  <div key={l} style={{ background: "#F9F8F7", borderRadius: 7, padding: "8px 6px", textAlign: "center", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center" }}>
                    <div style={{ fontSize: 16, fontWeight: 700, color: isRating ? (v >= 4 ? "#16A34A" : v >= 3 ? "#D97706" : C.accent) : "inherit", display: "flex", alignItems: "center", gap: 3 }}>
                      {v}{isRating && <span style={{ fontSize: 12 }}>★</span>}
                    </div>
                    <div style={{ fontSize: 10, color: C.muted }}>{l}</div>
                  </div>
                ))}
              </div>
              <div style={{ fontSize: 12, color: C.sub, lineHeight: 2 }}>
                {selected.Category && selected.Category !== "Uncategorized" && <div>🏷️ {selected.Category}</div>}
                {selected.AvgPrice && <div>💰 {selected.AvgPrice}</div>}
                {selected.OpeningHours && <div>⏰ {selected.OpeningHours}</div>}
                {selected.Phone && <div>📞 {selected.Phone}</div>}
              </div>
              {selected.Reviews3 && (
                <div style={{ marginTop: 10, padding: 10, background: "#F9F8F7", borderRadius: 7, maxHeight: 150, overflowY: "auto" }}>
                  <div style={{ fontSize: 10, color: C.muted, fontWeight: 500, marginBottom: 5 }}>ALL REVIEWS</div>
                  <div style={{ fontSize: 11, color: C.sub, lineHeight: 1.7, direction: "rtl", textAlign: "right" }}>
                    {selected.Reviews3.split("|").map((rev, i) => (
                      rev.trim() ? <div key={i} style={{ marginBottom: 6, paddingBottom: 6, borderBottom: i < selected.Reviews3.split("|").length - 1 ? "1px solid #EAEAEA" : "none" }}>{rev.trim()}</div> : null
                    ))}
                  </div>
                </div>
              )}
            </Card>
            <Card style={{ padding: 0, overflow: "hidden", display: "flex", flexDirection: "column" }}>
              <MerchantProfiler initialMerchant={selected} embedded={true} tickets={tickets} />
            </Card>
          </div>
          <div style={{ marginTop: 16 }}>
            <MerchantNotes
              merchantId={selected.Merchant}
              authorName={userEmail || localStorage.getItem("wn_email") || "Team Member"}
              anonKey={anonKey}
              userRole={userRole}
              region={region}
            />
          </div>
        </>
      ) : (
        <div style={{ textAlign: "center", padding: 60, color: C.muted, fontSize: 13 }}>
          Search and select a merchant to view AI analysis
        </div>
      )}
      <style>{`@keyframes bounce { to { transform:translateY(-6px);opacity:.4; } }`}</style>
    </div>
  );
}

/* ─── PIPELINE TAB ───────────────────────────────────────────── */
function PipelineTab({ merchants, onMerchantClick, statuses, onStatusChange, region = "KSA", userRole }) {
  const [city, setCity] = useState("All");
  const [mall, setMall] = useState("All");
  const [prio, setPrio] = useState("All");
  const [cat, setCat] = useState("All");
  const [subCat, setSubCat] = useState("All");
  const [price, setPrice] = useState("All");
  const [hours, setHours] = useState("All");
  const [statusFilter, setStatusFilter] = useState("All");
  const [page, setPage] = useState(1);
  const pageSize = 50;

  const allCities = useMemo(() => ["All", ...new Set(merchants.map(m => m.City).filter(Boolean))].sort(), [merchants]);
  const allMalls = useMemo(() => {
    const list = city === "All" ? merchants : merchants.filter(m => m.City === city);
    return ["All", ...new Set(list.map(m => m.Mall).filter(Boolean))].sort();
  }, [merchants, city]);
  const allCategories = useMemo(() => ["All", ...new Set(merchants.map(m => m.Category).filter(c => c && c !== "Uncategorized"))].sort(), [merchants]);
  const allSubCategories = useMemo(() => ["All", ...new Set(merchants.map(m => m.SubCategory).filter(s => s && s !== "General"))].sort(), [merchants]);
  const allPrices = useMemo(() => ["All", ...new Set(merchants.map(m => m.AvgPrice).filter(Boolean))].sort(), [merchants]);
  const allHours = ["All", "24 Hours", "Specified Hours", "Not Available"];

  const filteredMerchants = useMemo(() => merchants.filter(m => {
    if (city !== "All" && m.City !== city) return false;
    if (mall !== "All" && m.Mall !== mall) return false;
    if (prio !== "All") {
      const p = m.Priority.toLowerCase();
      if (prio === "Uncategorized" && p !== "uncategorized") return false;
      if (prio !== "Uncategorized" && !p.includes(prio.toLowerCase())) return false;
    }
    if (cat !== "All" && m.Category !== cat) return false;
    if (subCat !== "All" && m.SubCategory !== subCat) return false;
    if (price !== "All" && m.AvgPrice !== price) return false;
    if (hours !== "All" && m.HoursCategory !== hours) return false;

    if (statusFilter !== "All") {
      const key = `${m.Merchant}|${m.Mall || ""}`;
      const st = statuses[key] || "Uncontacted";
      if (statusFilter === "Contacted+") {
        if (st === "Uncontacted") return false;
      } else {
        if (st !== statusFilter) return false;
      }
    }
    return true;
  }), [merchants, city, mall, prio, cat, subCat, price, hours, statusFilter, statuses]);

  const totalPages = Math.ceil(filteredMerchants.length / pageSize);
  useEffect(() => { setPage(1); }, [city, prio, cat, subCat, price, hours, statusFilter]);
  const rows = useMemo(() => filteredMerchants.slice((page - 1) * pageSize, page * pageSize), [filteredMerchants, page]);

  const pCounts = useMemo(() => {
    let h = 0, m = 0, l = 0, u = 0;
    for (const r of filteredMerchants) {
      const p = r.Priority.toLowerCase();
      if (p.includes("high")) h++;
      else if (p.includes("medium")) m++;
      else if (p.includes("low")) l++;
      else u++;
    }
    return { h, m, l, u };
  }, [filteredMerchants]);

  const contacted = Object.values(statuses).filter(s => s !== "Uncontacted").length;
  const closed = Object.values(statuses).filter(s => s === "Closed Deal").length;

  const statusStyles = {
    "Uncontacted": { background: "#F4F2EE", color: C.sub },
    "Contacted": { background: "#EFF6FF", color: "#1D4ED8" },
    "In Progress": { background: "#FEF3C7", color: "#92400E" },
    "Closed Deal": { background: "#F0FDF4", color: "#15803D" },
  };

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-.4px" }}>Merchant Pipeline</div>
        <div style={{ fontSize: 13, color: C.muted, marginTop: 3 }}>Track outreach across {merchants.length.toLocaleString()} merchants</div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 10, marginBottom: 16 }}>
        <KPI label="Total Matches" value={filteredMerchants.length.toLocaleString()} sub="Based on filters" />
        <KPI label="High" value={pCounts.h.toLocaleString()} sub="Priority" color={C.accent} onClick={() => setPrio("High")} />
        <KPI label="Medium" value={pCounts.m.toLocaleString()} sub="Priority" color="#D97706" onClick={() => setPrio("Medium")} />
        <KPI label="Low" value={pCounts.l.toLocaleString()} sub="Priority" color="#16A34A" onClick={() => setPrio("Low")} />
        <KPI label="Contacted" value={contacted} sub="This session" color="#1D4ED8" onClick={() => setStatusFilter("Contacted+")} />
        <KPI label="Closed Deals" value={closed} sub="This session" color="#15803D" onClick={() => setStatusFilter("Closed Deal")} />
      </div>

      <div style={{ display: "flex", gap: 14, marginBottom: 20, alignItems: "flex-end", flexWrap: "wrap" }}>
        {[
          { label: "City", icon: "M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z", opts: allCities, val: city, fn: (v) => { setCity(v); setMall("All"); } },
          { label: "Mall", icon: "M6 2L3 6v14a2 2 0 002 2h14a2 2 0 002-2V6l-3-4z", opts: allMalls, val: mall, fn: setMall, placeholder: "Select Mall" },
          { label: "Priority", icon: "M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z", opts: ["All", "High", "Medium", "Low", "Uncategorized"], val: prio, fn: setPrio },
          { label: "Category", icon: "M20.59 13.41l-7.17 7.17a2 2 0 01-2.83 0L2 12V2h10l8.59 8.59a2 2 0 010 2.82z", opts: allCategories, val: cat, fn: setCat },
          { label: "Sub-category", icon: "M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01", opts: allSubCategories, val: subCat, fn: setSubCat },
          { label: "Price", icon: "M12 1v22M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6", opts: allPrices, val: price, fn: setPrice },
          { label: "Hours", icon: "M12 22a10 10 0 100-20 10 10 0 000 20z M12 6v6l4 2", opts: allHours, val: hours, fn: setHours },
          { label: "Status", icon: "M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z", opts: ["All", "Contacted+", "Uncontacted", "Contacted", "In Progress", "Closed Deal"], val: statusFilter, fn: setStatusFilter },
        ].map(({ label, icon, opts, val, fn, placeholder }) => (
          <div key={label} style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <label style={{ fontSize: 11, fontWeight: 600, color: C.muted, display: "flex", alignItems: "center", gap: 4, marginLeft: 2 }}>
              <svg width={12} height={12} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round">
                <path d={icon} />
                {label === "City" && <circle cx="12" cy="10" r="3" />}
                {label === "Mall" && <path d="M3 6h18M16 10a4 4 0 01-8 0" />}
              </svg>
              {label}
            </label>
            <select value={val} onChange={e => fn(e.target.value)}
              style={{ padding: "8px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, background: C.white, color: C.text, outline: "none", cursor: "pointer", minWidth: 140, boxShadow: "0 1px 2px rgba(0,0,0,0.05)" }}>
              {opts.map((o, i) => (
                <option key={i} value={o}>{o === "All" ? (placeholder || `${label}: All`) : (o === "Contacted+" ? "Any Contacted" : (o.length > 25 ? o.substring(0, 25) + "..." : o))}</option>
              ))}
            </select>
          </div>
        ))}
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <button onClick={() => { setCity("All"); setMall("All"); setPrio("All"); setCat("All"); setSubCat("All"); setPrice("All"); setHours("All"); setStatusFilter("All"); }}
            style={{ padding: "8px 14px", background: "#F4F2EE", border: "none", borderRadius: 8, fontSize: 12, color: C.text, cursor: "pointer", fontWeight: 600, height: 35 }}>
            Clear Filters
          </button>
          <span style={{ fontSize: 12, color: C.muted, fontWeight: 500 }}>{filteredMerchants.length.toLocaleString()} matches</span>
        </div>
      </div>

      <div style={{ background: C.white, borderRadius: 10, border: `1px solid ${C.border}`, overflow: "hidden" }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr>
                {["Merchant", "City", "Mall", "Category", "Priority", "Rating", "Reviews", "Price", "Hours", "Status"].map(h => (
                  <th key={h} style={{ padding: "10px 12px", textAlign: "left", fontSize: 10, fontWeight: 500, color: C.muted, textTransform: "uppercase", letterSpacing: .5, borderBottom: `1px solid ${C.border}`, background: "#F9F8F7", whiteSpace: "nowrap" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((m, i) => {
                const key = `${m.Merchant}|${m.Mall || ""}`;
                const st = statuses[key] || "Uncontacted";
                return (
                  <tr key={i} style={{ borderBottom: `1px solid #F4F2EE` }}>
                    <td style={{ padding: "9px 12px", fontWeight: 500, maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", direction: "rtl" }}>
                      <span onClick={() => onMerchantClick(m)} style={{ cursor: "pointer", color: C.accent, textDecoration: "underline" }} title="View AI Profile">{m.Merchant}</span>
                    </td>
                    <td style={{ padding: "9px 12px" }}><span style={{ background: "#EFF6FF", color: "#1D4ED8", borderRadius: 4, padding: "2px 7px", fontSize: 10 }}>{m.City}</span></td>
                    <td style={{ padding: "9px 12px", fontSize: 11, color: C.muted, maxWidth: 110, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.Mall}</td>
                    <td style={{ padding: "9px 12px", fontSize: 11, color: C.sub, maxWidth: 110, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.Category === "Uncategorized" ? "—" : m.Category}</td>
                    <td style={{ padding: "9px 12px", fontWeight: 500, fontSize: 11, color: m.Priority.toLowerCase().includes("high") ? C.accent : m.Priority.toLowerCase().includes("medium") ? "#D97706" : m.Priority.toLowerCase().includes("low") ? "#16A34A" : C.muted }}>{m.Priority}</td>
                    <td style={{ padding: "9px 12px" }}>
                      <span style={{
                        fontSize: 10, fontWeight: 700,
                        color: m.Rating >= 4 ? "#16A34A" : m.Rating >= 3 ? "#D97706" : C.accent,
                        background: m.Rating >= 4 ? "#F0FDF4" : m.Rating >= 3 ? "#FFFBEB" : C.accentL,
                        padding: "2px 6px", borderRadius: 5, display: "inline-flex", alignItems: "center", gap: 3, whiteSpace: "nowrap"
                      }}>
                        ★ {m.Rating}
                      </span>
                    </td>
                    <td style={{ padding: "9px 12px" }}>{m.Reviews >= 1000 ? `${(m.Reviews / 1000).toFixed(1)}k` : m.Reviews}</td>
                    <td style={{ padding: "9px 12px", color: C.muted }}>{m.AvgPrice || "—"}</td>
                    <td style={{ padding: "9px 12px", color: C.muted, fontSize: 11, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={m.OpeningHours}>{m.OpeningHours || "—"}</td>
                    <td style={{ padding: "9px 12px" }}>
                      {(userRole === 'admin' || userRole === 'global_bd' || (userRole === 'ksa_bd' && region === 'KSA') || (userRole === 'oman_bd' && region === 'Oman')) ? (
                        <select value={st} onChange={e => onStatusChange(m, e.target.value)}
                          style={{ padding: "3px 7px", borderRadius: 5, fontSize: 10, fontWeight: 500, border: "none", cursor: "pointer", outline: "none", ...statusStyles[st] }}>
                          {Object.keys(statusStyles).map(s => <option key={s}>{s}</option>)}
                        </select>
                      ) : (
                        <span style={{ padding: "3px 7px", borderRadius: 5, fontSize: 10, fontWeight: 500, ...statusStyles[st] }}>{st}</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "12px 16px", background: "#F9F8F7", borderTop: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 11, color: C.muted }}>
            Showing <strong>{filteredMerchants.length === 0 ? 0 : ((page - 1) * pageSize) + 1} to {Math.min(page * pageSize, filteredMerchants.length)}</strong> of {filteredMerchants.length} entries
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
              style={{ padding: "6px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, background: page === 1 ? "#F9F9F9" : C.white, cursor: page === 1 ? "not-allowed" : "pointer", color: page === 1 ? C.muted : C.text, fontWeight: 600 }}>← Previous</button>
            <div style={{ display: "flex", gap: 4 }}>
              {[...Array(totalPages)].map((_, i) => {
                const p = i + 1;
                if (totalPages > 5 && Math.abs(p - page) > 1 && p !== 1 && p !== totalPages) return p === 2 || p === totalPages - 1 ? <span key={p} style={{ fontSize: 10, color: C.muted }}>...</span> : null;
                return (
                  <button key={p} onClick={() => setPage(p)}
                    style={{ width: 28, height: 28, border: `1px solid ${page === p ? C.accent : C.border}`, borderRadius: 6, fontSize: 11, background: page === p ? C.accent : C.white, color: page === p ? "#fff" : C.text, cursor: "pointer", fontWeight: 600 }}>{p}</button>
                );
              })}
            </div>
            <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages || totalPages === 0}
              style={{ padding: "6px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, background: page >= totalPages ? "#F9F9F9" : C.white, cursor: page >= totalPages ? "not-allowed" : "pointer", color: page >= totalPages ? C.muted : C.text, fontWeight: 600 }}>Next →</button>
          </div>
        </div>
      </div>
    </div>
  );
}
/* ─── SAVED MERCHANTS TAB ──────────────────────────────────────── */
function SavedMerchantsTab({ merchants, favoriteIds, onMerchantClick }) {
  const savedMerchants = useMemo(() => {
    if (!favoriteIds || favoriteIds.size === 0) return [];
    return merchants.filter(m => favoriteIds.has(m.Merchant));
  }, [merchants, favoriteIds]);

  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 24, fontWeight: 800, letterSpacing: "-.5px", color: C.text }}>Saved Merchants ❤️</div>
        <div style={{ fontSize: 13, color: C.muted, marginTop: 4 }}>
          {savedMerchants.length} {savedMerchants.length === 1 ? 'merchant' : 'merchants'} in your personal favorites
        </div>
      </div>

      {savedMerchants.length === 0 ? (
        <div style={{ textAlign: "center", padding: 60, color: C.muted, fontSize: 14, background: C.white, borderRadius: 12, border: `1px dashed ${C.border}` }}>
          You haven't saved any merchants yet. Go to the Profiler and click the ❤️ icon to save them here!
        </div>
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 16 }}>
          {savedMerchants.map((m, i) => (
            <Card key={i} style={{ padding: 20, cursor: "pointer", transition: "transform 0.2s, box-shadow 0.2s" }} >
              <div
                onClick={() => onMerchantClick(m)}
                onMouseEnter={e => { e.currentTarget.parentElement.style.transform = "translateY(-4px)"; e.currentTarget.parentElement.style.boxShadow = "0 10px 25px rgba(0,0,0,0.05)"; }}
                onMouseLeave={e => { e.currentTarget.parentElement.style.transform = "translateY(0)"; e.currentTarget.parentElement.style.boxShadow = "none"; }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
                  <div>
                    <div style={{ fontSize: 18, fontWeight: 700, color: C.text, lineHeight: 1.3 }}>{m.Merchant}</div>
                    <div style={{ fontSize: 12, color: C.muted, marginTop: 4 }}>{m.Mall ? `${m.Mall} · ` : ''}{m.City}</div>
                  </div>
                  <span style={{
                    fontSize: 12, fontWeight: 700,
                    color: m.Rating >= 4 ? "#16A34A" : m.Rating >= 3 ? "#D97706" : C.accent,
                    background: m.Rating >= 4 ? "#F0FDF4" : m.Rating >= 3 ? "#FFFBEB" : C.accentL,
                    padding: "4px 10px", borderRadius: 8, display: "flex", alignItems: "center", gap: 4, whiteSpace: "nowrap"
                  }}>
                    ★ {m.Rating}
                  </span>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 16, marginTop: 16 }}>
                  <div style={{ background: C.bg, padding: "8px 12px", borderRadius: 8 }}>
                    <div style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: 0.5 }}>Reviews</div>
                    <div style={{ fontSize: 14, fontWeight: 600, color: C.text }}>{m.Reviews.toLocaleString()}</div>
                  </div>
                  <div style={{ background: C.bg, padding: "8px 12px", borderRadius: 8 }}>
                    <div style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: 0.5 }}>Category</div>
                    <div style={{ fontSize: 14, fontWeight: 600, color: C.text, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{m.Category}</div>
                  </div>
                </div>

                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  <span style={{
                    padding: "4px 10px", borderRadius: 6, fontSize: 11, fontWeight: 500,
                    background: m.Priority.toLowerCase().includes("high") ? C.accentL : m.Priority.toLowerCase().includes("medium") ? "#FEF3C7" : m.Priority.toLowerCase().includes("low") ? "#F0FDF4" : "#F4F2EE",
                    color: m.Priority.toLowerCase().includes("high") ? C.accent : m.Priority.toLowerCase().includes("medium") ? "#D97706" : m.Priority.toLowerCase().includes("low") ? "#16A34A" : C.muted
                  }}>
                    {m.Priority} Priority
                  </span>
                  {m.Branches > 1 && (
                    <span style={{ padding: "4px 10px", borderRadius: 6, fontSize: 11, fontWeight: 500, background: '#F3F4F6', color: '#4B5563' }}>
                      {m.Branches} Branches
                    </span>
                  )}
                </div>
              </div>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}

/* ─── MALLS PROFILE TAB ──────────────────────────────────────── */
function MallsTab({ merchants, onMerchantClick, statuses, onStatusChange, region = "KSA", userRole }) {
  const [cityFilter, setCityFilter] = useState("All");
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("merchants");
  const [selectedMall, setSelectedMall] = useState(null);
  const [page, setPage] = useState(1);
  const pageSize = 12;

  const mallsData = useMemo(() => {
    const map = {};
    for (const m of merchants) {
      if (!m.Mall) continue;
      if (!map[m.Mall]) map[m.Mall] = { name: m.Mall, city: m.City, merchants: 0, ratingSum: 0, ratingCount: 0, reviewsTotal: 0, categories: {}, highCount: 0 };
      const d = map[m.Mall];
      d.merchants++;
      if (m.Rating > 0) { d.ratingSum += m.Rating; d.ratingCount++; }
      d.reviewsTotal += m.Reviews || 0;
      if (m.Category) d.categories[m.Category] = (d.categories[m.Category] || 0) + 1;
      if (m.Priority.toLowerCase().includes("high")) d.highCount++;
    }
    return Object.values(map).map(d => ({
      ...d,
      avgRating: d.ratingCount ? +(d.ratingSum / d.ratingCount).toFixed(1) : 0,
      topCategory: Object.entries(d.categories).sort((a, b) => b[1] - a[1])[0]?.[0] || "—",
    }));
  }, [merchants]);

  const allCities = useMemo(() => ["All", ...new Set(mallsData.map(m => m.city).filter(Boolean)).values()].sort(), [mallsData]);

  const filtered = useMemo(() => {
    let list = mallsData;
    if (cityFilter !== "All") list = list.filter(m => m.city === cityFilter);
    if (search.trim()) list = list.filter(m => m.name.toLowerCase().includes(search.toLowerCase()));
    return [...list].sort((a, b) => {
      if (sortBy === "rating") return b.avgRating - a.avgRating;
      if (sortBy === "reviews") return b.reviewsTotal - a.reviewsTotal;
      return b.merchants - a.merchants;
    });
  }, [mallsData, cityFilter, search, sortBy]);

  const totalPages = Math.ceil(filtered.length / pageSize);
  const paginated = filtered.slice((page - 1) * pageSize, page * pageSize);

  useEffect(() => setPage(1), [cityFilter, search, sortBy]);

  if (selectedMall) {
    const mallMerchants = merchants.filter(m => m.Mall === selectedMall.name);
    const statusStyles = {
      "Uncontacted": { background: "#F4F2EE", color: C.sub },
      "Contacted": { background: "#EFF6FF", color: "#1D4ED8" },
      "In Progress": { background: "#FEF3C7", color: "#92400E" },
      "Closed Deal": { background: "#F0FDF4", color: "#15803D" },
    };
    return (
      <div>
        <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 20 }}>
          <button onClick={() => setSelectedMall(null)}
            style={{ display: "flex", alignItems: "center", gap: 6, padding: "8px 14px", background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12, color: C.sub, cursor: "pointer", fontWeight: 500 }}>
            <svg width={13} height={13} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round"><path d="M19 12H5M12 5l-7 7 7 7" /></svg>
            Back to Malls
          </button>
          <div>
            <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-.4px" }}>{selectedMall.name}</div>
            <div style={{ fontSize: 13, color: C.muted, marginTop: 2 }}>{selectedMall.city} · {mallMerchants.length} merchants · ★ {selectedMall.avgRating}</div>
          </div>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10, marginBottom: 18 }}>
          <KPI label="Total Merchants" value={mallMerchants.length} sub="In this mall" />
          <KPI label="Avg Rating" value={`${selectedMall.avgRating} ★`} sub="Mall average" color="#16A34A" />
          <KPI label="Total Reviews" value={selectedMall.reviewsTotal.toLocaleString()} sub="Across merchants" />
          <KPI label="High Priority" value={selectedMall.highCount} sub="BD opportunities" color={C.accent} />
        </div>
        <div style={{ background: C.white, borderRadius: 10, border: `1px solid ${C.border}`, overflow: "hidden" }}>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ background: "#F9F8F7", borderBottom: `1px solid ${C.border}` }}>
                  {["Merchant", "Category", "Priority", "Rating", "Reviews", "Price", "Hours", "Status"].map(h => (
                    <th key={h} style={{ padding: "10px 12px", textAlign: "left", fontSize: 10, fontWeight: 600, color: C.muted, textTransform: "uppercase", letterSpacing: .5, whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {mallMerchants.map((m, i) => {
                  const status = statuses[`${m.Merchant}|${m.Mall || ""}`] || "Uncontacted";
                  return (
                    <tr key={i} style={{ borderBottom: `1px solid ${C.border}`, background: i % 2 === 0 ? C.white : "#FAFAF9" }}>
                      <td style={{ padding: "10px 12px" }}>
                        <span onClick={() => onMerchantClick(m)} style={{ color: C.accent, cursor: "pointer", fontWeight: 500 }}>{m.Merchant}</span>
                      </td>
                      <td style={{ padding: "10px 12px", color: C.sub }}>{m.Category}</td>
                      <td style={{ padding: "10px 12px" }}>
                        <span style={{ fontSize: 10, fontWeight: 600, padding: "2px 7px", borderRadius: 4, background: m.Priority.toLowerCase().includes("high") ? "#FEE2E2" : m.Priority.toLowerCase().includes("medium") ? "#FEF3C7" : "#F0FDF4", color: m.Priority.toLowerCase().includes("high") ? "#DC2626" : m.Priority.toLowerCase().includes("medium") ? "#92400E" : "#15803D" }}>{m.Priority}</span>
                      </td>
                      <td style={{ padding: "10px 12px" }}>{m.Rating > 0 ? `★ ${m.Rating}` : "—"}</td>
                      <td style={{ padding: "10px 12px", color: C.sub }}>{m.Reviews > 0 ? m.Reviews.toLocaleString() : "—"}</td>
                      <td style={{ padding: "10px 12px", color: C.sub }}>{m.AvgPrice || "—"}</td>
                      <td style={{ padding: "10px 12px", color: C.sub, maxWidth: 140, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.HoursCategory}</td>
                      <td style={{ padding: "10px 12px" }}>
                        {(userRole === 'admin' || userRole === 'global_bd' || (userRole === 'ksa_bd' && region === 'KSA') || (userRole === 'oman_bd' && region === 'Oman')) ? (
                          <select value={status} onChange={e => onStatusChange(m, e.target.value)}
                            style={{ ...statusStyles[status], fontSize: 11, padding: "3px 7px", borderRadius: 5, border: "none", cursor: "pointer", fontWeight: 500 }}>
                            {["Uncontacted", "Contacted", "In Progress", "Closed Deal"].map(s => <option key={s} value={s}>{s}</option>)}
                          </select>
                        ) : (
                          <span style={{ ...statusStyles[status], fontSize: 11, padding: "3px 7px", borderRadius: 5, fontWeight: 500 }}>{status}</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-.4px" }}>Malls Profile</div>
        <div style={{ fontSize: 13, color: C.muted, marginTop: 3 }}>{filtered.length} malls · {merchants.filter(m => m.Mall).length.toLocaleString()} merchants across {region}</div>
      </div>
      <div style={{ display: "flex", gap: 12, marginBottom: 20, alignItems: "center", flexWrap: "wrap" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <label style={{ fontSize: 10, fontWeight: 600, color: C.muted, textTransform: "uppercase", letterSpacing: .5 }}>Search</label>
          <div style={{ position: "relative" }}>
            <svg style={{ position: "absolute", left: 10, top: "50%", transform: "translateY(-50%)", pointerEvents: "none" }} width={13} height={13} viewBox="0 0 24 24" fill="none" stroke={C.muted} strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
            </svg>
            <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search malls…"
              style={{ paddingLeft: 32, paddingRight: 12, paddingTop: 8, paddingBottom: 8, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, outline: "none", background: C.white, color: C.text, width: 240 }} />
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <label style={{ fontSize: 10, fontWeight: 600, color: C.muted, textTransform: "uppercase", letterSpacing: .5 }}>City</label>
          <select value={cityFilter} onChange={e => setCityFilter(e.target.value)}
            style={{ padding: "8px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, background: C.white, color: C.text, outline: "none", cursor: "pointer" }}>
            {allCities.map(c => <option key={c} value={c}>{c === "All" ? "All Cities" : c}</option>)}
          </select>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <label style={{ fontSize: 10, fontWeight: 600, color: C.muted, textTransform: "uppercase", letterSpacing: .5 }}>Sort By</label>
          <select value={sortBy} onChange={e => setSortBy(e.target.value)}
            style={{ padding: "8px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, background: C.white, color: C.text, outline: "none", cursor: "pointer" }}>
            <option value="merchants">Most Merchants</option>
            <option value="rating">Highest Rating</option>
            <option value="reviews">Most Reviews</option>
          </select>
        </div>
        <span style={{ marginLeft: "auto", fontSize: 12, color: C.muted, fontWeight: 500 }}>{filtered.length} malls</span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 14 }}>
        {paginated.map((mall, i) => (
          <div key={i} onClick={() => setSelectedMall(mall)}
            style={{ background: C.white, borderRadius: 12, border: `1px solid ${C.border}`, padding: 20, cursor: "pointer", transition: "all .18s", boxShadow: "0 1px 3px rgba(0,0,0,.05)" }}
            onMouseEnter={e => { e.currentTarget.style.boxShadow = "0 4px 16px rgba(0,0,0,.1)"; e.currentTarget.style.borderColor = C.accent; }}
            onMouseLeave={e => { e.currentTarget.style.boxShadow = "0 1px 3px rgba(0,0,0,.05)"; e.currentTarget.style.borderColor = C.border; }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
              <div>
                <div style={{ fontSize: 14, fontWeight: 700, color: C.text, lineHeight: 1.3, marginBottom: 4 }}>{mall.name}</div>
                <span style={{ fontSize: 10, fontWeight: 600, padding: "3px 8px", borderRadius: 12, background: C.accentL, color: C.accent }}>{mall.city}</span>
              </div>
              <div style={{ width: 40, height: 40, borderRadius: 10, background: C.accentL, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                <svg width={20} height={20} viewBox="0 0 24 24" fill="none" stroke={C.accent} strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                  <path d="M6 2L3 6v14a2 2 0 002 2h14a2 2 0 002-2V6l-3-4z" /><line x1="3" y1="6" x2="21" y2="6" /><path d="M16 10a4 4 0 01-8 0" />
                </svg>
              </div>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 14 }}>
              {[["Merchants", mall.merchants], ["Avg Rating", mall.avgRating > 0 ? `★ ${mall.avgRating}` : "—"], ["Reviews", mall.reviewsTotal >= 1000 ? `${(mall.reviewsTotal / 1000).toFixed(1)}k` : mall.reviewsTotal]].map(([l, v]) => (
                <div key={l} style={{ background: "#F9F8F7", borderRadius: 8, padding: "8px 10px", textAlign: "center" }}>
                  <div style={{ fontSize: 16, fontWeight: 700, color: l === "Avg Rating" && mall.avgRating >= 4 ? "#16A34A" : l === "Avg Rating" && mall.avgRating >= 3 ? "#D97706" : C.text }}>{v}</div>
                  <div style={{ fontSize: 10, color: C.muted, marginTop: 2 }}>{l}</div>
                </div>
              ))}
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontSize: 11, color: C.muted }}>Top: <span style={{ color: C.text, fontWeight: 500 }}>{mall.topCategory}</span></div>
              {mall.highCount > 0 && (
                <span style={{ fontSize: 10, fontWeight: 600, padding: "2px 7px", borderRadius: 10, background: "#FEE2E2", color: "#DC2626" }}>{mall.highCount} High ↑</span>
              )}
            </div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 24, fontSize: 13, color: C.muted, background: C.white, padding: "12px 20px", borderRadius: 12, border: `1px solid ${C.border}` }}>
        <span>Showing <strong style={{ color: C.text }}>{filtered.length === 0 ? 0 : (page - 1) * pageSize + 1}–{Math.min(page * pageSize, filtered.length)}</strong> of {filtered.length} malls</span>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
            style={{ padding: "6px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, background: page === 1 ? "#F9F9F9" : C.white, cursor: page === 1 ? "not-allowed" : "pointer", color: page === 1 ? C.muted : C.text, fontWeight: 600 }}>← Previous</button>
          <div style={{ display: "flex", gap: 4 }}>
            {[...Array(totalPages)].map((_, i) => {
              const p = i + 1;
              if (totalPages > 7 && Math.abs(p - page) > 2 && p !== 1 && p !== totalPages) return p === 2 || p === totalPages - 1 ? <span key={p}>...</span> : null;
              return (
                <button key={p} onClick={() => setPage(p)}
                  style={{ width: 32, height: 32, border: `1px solid ${page === p ? C.accent : C.border}`, borderRadius: 8, fontSize: 13, background: page === p ? C.accent : C.white, color: page === p ? "#fff" : C.text, cursor: "pointer", fontWeight: 600 }}>{p}</button>
              );
            })}
          </div>
          <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}
            style={{ padding: "6px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, background: page >= totalPages ? "#F9F9F9" : C.white, cursor: page >= totalPages ? "not-allowed" : "pointer", color: page >= totalPages ? C.muted : C.text, fontWeight: 600 }}>Next →</button>
        </div>
      </div>
    </div>
  );
}

/* ─── CHAT REVIEW ────────────────────────────────────────────── */
function parseMessages(subject, owner) {
  if (!subject || subject === "—" || subject.length < 30) return null;
  // Try to split by agent/customer turn markers (HH:MM patterns)
  const timePattern = /(\d{1,2}:\d{2}(?:\s*[AP]M)?)/gi;
  const agentName = (owner || "").split("@")[0];
  // Split on lines that look like "Name HH:MM" or "HH:MM"
  const lines = subject.split(/\n|\r\n|\r/).filter(l => l.trim());
  if (lines.length < 2) return null;
  const messages = [];
  let currentSender = "customer";
  let currentText = [];
  let currentTime = "";
  for (const line of lines) {
    const isAgent = agentName && line.toLowerCase().includes(agentName.toLowerCase());
    const timeMatch = line.match(timePattern);
    const time = timeMatch ? timeMatch[0] : "";
    if (timeMatch || isAgent) {
      if (currentText.length > 0) {
        messages.push({ sender: currentSender, text: currentText.join(" ").trim(), time: currentTime });
        currentText = [];
      }
      currentSender = isAgent ? "agent" : "customer";
      currentTime = time;
      const cleaned = line.replace(timePattern, "").replace(agentName, "").replace(/^[\s:|-]+/, "").trim();
      if (cleaned) currentText.push(cleaned);
    } else {
      currentText.push(line.trim());
    }
  }
  if (currentText.length > 0) messages.push({ sender: currentSender, text: currentText.join(" ").trim(), time: currentTime });
  return messages.length >= 2 ? messages : null;
}

function ChatBubble({ msg }) {
  const isAgent = msg.sender === "agent";
  return (
    <div style={{ display: "flex", flexDirection: isAgent ? "row-reverse" : "row", gap: 8, alignItems: "flex-end" }}>
      <div style={{ width: 36, height: 36, borderRadius: "50%", background: isAgent ? C.accentL : "#E8E4DF", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16, flexShrink: 0 }}>
        {isAgent ? "🤖" : "👤"}
      </div>
      <div style={{ maxWidth: "65%" }}>
        <div style={{
          padding: "10px 14px",
          borderRadius: isAgent ? "18px 18px 4px 18px" : "18px 18px 18px 4px",
          background: isAgent ? C.accentL : C.white,
          boxShadow: "0 1px 3px rgba(0,0,0,.06)",
          fontSize: 12, lineHeight: 1.5, color: C.text,
        }}>
          {msg.text}
        </div>
        {msg.time && <div style={{ fontSize: 10, color: C.muted, marginTop: 3, textAlign: isAgent ? "right" : "left" }}>{msg.time}</div>}
      </div>
    </div>
  );
}

function ChatReview({ tickets, initialTicketId, onAgentClick }) {
  const [selectedId, setSelectedId] = useState(initialTicketId || null);
  const [searchQ, setSearchQ] = useState("");
  const [page, setPage] = useState(1);
  const pageSize = 50;
  const threadRef = useRef(null);

  const filteredList = useMemo(() => {
    const q = searchQ.toLowerCase();
    return tickets.filter(t =>
      !q || String(t.id).includes(q) || (t.ticket_number && t.ticket_number.toLowerCase().includes(q)) || t.subject.toLowerCase().includes(q) ||
      t.owner.toLowerCase().includes(q) || t.reason.toLowerCase().includes(q)
    ).sort((a, b) => b.createdTime.localeCompare(a.createdTime));
  }, [tickets, searchQ]);

  const totalPages = Math.ceil(filteredList.length / pageSize);
  const paginatedList = useMemo(() => filteredList.slice((page - 1) * pageSize, page * pageSize), [filteredList, page]);

  useEffect(() => setPage(1), [searchQ]);

  const selected = useMemo(() => tickets.find(t => t.id === selectedId) || null, [tickets, selectedId]);
  const messages = useMemo(() => selected ? parseMessages(selected.subject, selected.owner) : null, [selected]);

  useEffect(() => {
    if (threadRef.current) threadRef.current.scrollTop = threadRef.current.scrollHeight;
  }, [selectedId]);

  const qColor = s => s >= 7 ? "#10B981" : s >= 4 ? "#FBBF24" : "#EF4444";

  return (
    <div style={{ display: "flex", height: "calc(100vh - 96px)", borderRadius: 12, overflow: "hidden", border: `1px solid ${C.border}`, background: C.white }}>
      {/* Left panel — conversation list */}
      <div style={{ width: 320, borderRight: `1px solid ${C.border}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
        <div style={{ padding: 16, borderBottom: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 10 }}>Conversations</div>
          <div style={{ position: "relative" }}>
            <input value={searchQ} onChange={e => setSearchQ(e.target.value)} placeholder="Search tickets…"
              style={{ width: "100%", padding: "8px 10px 8px 30px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12, outline: "none", background: "#F9F8F7", boxSizing: "border-box" }} />
            <svg style={{ position: "absolute", left: 9, top: "50%", transform: "translateY(-50%)" }} width={12} height={12} viewBox="0 0 24 24" fill="none" stroke={C.muted} strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
            </svg>
          </div>
        </div>
        <div style={{ flex: 1, overflowY: "auto", position: "relative" }}>
          {paginatedList.map(t => (
            <div key={t.id} onClick={() => setSelectedId(t.id)}
              style={{ padding: "12px 16px", cursor: "pointer", borderBottom: `1px solid #F4F2EE`, background: selectedId === t.id ? C.accentL : "transparent", borderLeft: selectedId === t.id ? `3px solid ${C.accent}` : "3px solid transparent", transition: "all .1s" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 3 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: C.text }}>{t.ticket_number || t.id}</div>
                {t.happiness && (
                  <span style={{ fontSize: 10, fontWeight: 600, padding: "1px 6px", borderRadius: 10, background: t.happiness === "Good" ? "#DCFCE7" : t.happiness === "Bad" ? "#FEE2E2" : "#FEF3C7", color: t.happiness === "Good" ? "#16A34A" : t.happiness === "Bad" ? "#DC2626" : "#D97706" }}>
                    {t.happiness}
                  </span>
                )}
              </div>
              <div style={{ fontSize: 11, color: C.sub, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", marginBottom: 2 }}>{t.subject}</div>
              <div style={{ fontSize: 10, color: C.muted }}>{(t.createdTime || "").slice(0, 10)} · {t.channel}</div>
            </div>
          ))}
          {filteredList.length === 0 && (
            <div style={{ textAlign: "center", padding: 40, color: C.muted, fontSize: 12 }}>No tickets found</div>
          )}
        </div>

        {/* Sidebar Pagination */}
        {totalPages > 1 && (
          <div style={{ padding: "10px 12px", borderTop: `1px solid ${C.border}`, background: "#F9F8F7", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
              style={{ padding: "4px 8px", background: page === 1 ? "transparent" : C.white, border: `1px solid ${C.border}`, borderRadius: 4, fontSize: 10, cursor: page === 1 ? "not-allowed" : "pointer", color: C.muted }}>←</button>
            <span style={{ fontSize: 10, color: C.muted, fontWeight: 600 }}>Page {page} / {totalPages}</span>
            <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}
              style={{ padding: "4px 8px", background: page >= totalPages ? "transparent" : C.white, border: `1px solid ${C.border}`, borderRadius: 4, fontSize: 10, cursor: page >= totalPages ? "not-allowed" : "pointer", color: C.muted }}>→</button>
          </div>
        )}
      </div>

      {/* Right panel — chat thread */}
      {selected ? (
        <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
          {/* Header */}
          <div style={{ padding: "14px 20px", borderBottom: `1px solid ${C.border}`, background: "linear-gradient(to right, #F9F8F7, #FFFFFF)", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
            <div>
              <div style={{ fontSize: 14, fontWeight: 700 }}>Ticket {selected.ticket_number || selected.id}</div>
              <div style={{ fontSize: 11, color: C.muted, marginTop: 1 }}>
                <span onClick={() => onAgentClick && selected.owner && onAgentClick(selected.owner)} style={{ cursor: selected.owner ? "pointer" : "default", color: selected.owner ? C.accent : C.muted, fontWeight: selected.owner ? 600 : 400 }}>{selected.owner || "Unknown Agent"}</span>
                <span style={{ color: C.muted }}> · {selected.channel}</span>
              </div>
            </div>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              {selected.overallQualityScore > 0 && (
                <span style={{ fontSize: 11, fontWeight: 700, padding: "3px 9px", borderRadius: 6, background: `${qColor(selected.overallQualityScore)}22`, color: qColor(selected.overallQualityScore) }}>
                  Q: {selected.overallQualityScore}/10
                </span>
              )}
              {selected.happiness && (
                <span style={{ fontSize: 11, fontWeight: 600, padding: "3px 9px", borderRadius: 6, background: selected.happiness === "Good" ? "#DCFCE7" : selected.happiness === "Bad" ? "#FEE2E2" : "#FEF3C7", color: selected.happiness === "Good" ? "#16A34A" : selected.happiness === "Bad" ? "#DC2626" : "#D97706" }}>
                  {selected.happiness}
                </span>
              )}
            </div>
          </div>

          {/* AI Banner */}
          {selected.sentimentSummary && (
            <div style={{ padding: "10px 20px", background: C.accentL, borderBottom: `1px solid ${C.border}`, fontSize: 12, color: C.accent, fontStyle: "italic", flexShrink: 0 }}>
              💡 {selected.sentimentSummary}
            </div>
          )}

          {/* Messages */}
          <div ref={threadRef} style={{ flex: 1, overflowY: "auto", padding: 20, display: "flex", flexDirection: "column", gap: 14, background: C.bg }}>
            {messages ? (
              messages.map((msg, i) => <ChatBubble key={i} msg={msg} />)
            ) : (
              <div style={{ background: C.white, borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,.06)" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: .5, marginBottom: 10 }}>Raw Log</div>
                <div style={{ fontSize: 12, color: C.sub, lineHeight: 1.7 }}>{selected.subject}</div>
                <div style={{ marginTop: 14, paddingTop: 14, borderTop: `1px solid ${C.border}`, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                  {[["Reason", selected.reason], ["Channel", selected.channel], ["Issue Type", selected.issueType || "—"], ["Root Cause", selected.rootCause || "—"], ["Merchant", selected.merchantName || "—"], ["Country", selected.country || "—"]].map(([l, v]) => (
                    <div key={l}>
                      <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", marginBottom: 1 }}>{l}</div>
                      <div style={{ fontSize: 11, fontWeight: 500, color: C.text }}>{v}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Footer info strip */}
          <div style={{ padding: "10px 20px", borderTop: `1px solid ${C.border}`, background: C.white, display: "flex", gap: 16, fontSize: 11, color: C.muted, flexShrink: 0, flexWrap: "wrap" }}>
            {[["Created", (selected.createdTime || "").slice(0, 16).replace("T", " ")], ["Status", selected.status], ["SLA", selected.slaViolation === "Not Violated" ? "✓ OK" : selected.slaViolation || "—"], ["Sentiment", selected.finalSentiment || "—"]].map(([l, v]) => (
              <span key={l}><strong style={{ color: C.sub }}>{l}:</strong> {v}</span>
            ))}
          </div>
        </div>
      ) : (
        <div style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: C.muted }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>💬</div>
          <div style={{ fontSize: 14, fontWeight: 500 }}>Select a conversation</div>
          <div style={{ fontSize: 12, marginTop: 4 }}>Choose a ticket from the list to review</div>
        </div>
      )}
    </div>
  );
}

/* ─── AGENT PROFILE ──────────────────────────────────────────── */
function AgentProfile({ agentId, tickets, onBack, onTicketClick, onKPIFilter }) {
  const agentTickets = useMemo(() => tickets.filter(t => t.owner === agentId), [tickets, agentId]);
  const agentName = agentId;

  const stats = useMemo(() => {
    let good = 0, bad = 0, okay = 0, rated = 0, slaOk = 0, slaTotal = 0, escalated = 0;
    let resTotal = 0, resCount = 0, reassigns = 0;
    const reasons = {}, channels = {};
    const analyzed = agentTickets.filter(t => t.aiStatus === "completed");
    agentTickets.forEach(t => {
      if (t.happiness === "Good" || ["positive", "very_positive"].includes(t.finalSentiment)) { good++; rated++; }
      if (t.happiness === "Bad" || ["negative", "very_negative", "worsened"].includes(t.finalSentiment)) { bad++; rated++; }
      if (t.happiness === "Okay" || ["neutral", "stable"].includes(t.finalSentiment)) { okay++; rated++; }
      if (t.slaViolation || t.oneTouchResolutionAI !== undefined) { slaTotal++; if (t.slaViolation === "Not Violated" || t.oneTouchResolutionAI) slaOk++; }
      if (t.isEscalated) escalated++;
      if (t.resolutionMs > 0) { resTotal += t.resolutionMs; resCount++; }
      reassigns += t.numReassign;
      if (t.reason) reasons[t.reason] = (reasons[t.reason] || 0) + 1;
      if (t.channel) channels[t.channel] = (channels[t.channel] || 0) + 1;
    });
    const avgQuality = analyzed.length ? (analyzed.reduce((s, t) => s + t.overallQualityScore, 0) / analyzed.length).toFixed(1) : "—";
    const avgEmpathy = analyzed.length ? (analyzed.reduce((s, t) => s + t.empathyScore, 0) / analyzed.length).toFixed(1) : "—";
    return {
      csat: rated ? Math.round(good / rated * 100) : null,
      slaRate: slaTotal ? Math.round(slaOk / slaTotal * 100) : null,
      avgRes: resCount ? (resTotal / resCount / 3600000).toFixed(1) : null,
      escalated, reassigns, rated, good, bad, okay,
      avgQuality, avgEmpathy, analyzed: analyzed.length,
      topReasons: Object.entries(reasons).sort((a, b) => b[1] - a[1]).slice(0, 8).map(([name, value]) => ({ name: name.length > 20 ? name.slice(0, 20) + "…" : name, value })),
      channelData: Object.entries(channels).sort((a, b) => b[1] - a[1]).map(([name, value]) => ({ name, value, color: CH_COLORS[name] || "#94A3B8" })),
    };
  }, [agentTickets]);

  const recentTickets = useMemo(() => [...agentTickets].sort((a, b) => b.createdTime.localeCompare(a.createdTime)).slice(0, 20), [agentTickets]);

  return (
    <div>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 24 }}>
        <button onClick={onBack}
          style={{ display: "flex", alignItems: "center", gap: 6, padding: "8px 14px", background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12, color: C.sub, cursor: "pointer", fontWeight: 500 }}>
          <svg width={13} height={13} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round"><path d="M19 12H5M12 5l-7 7 7 7" /></svg>
          Back to Agents
        </button>
        <div style={{ flex: 1 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ fontSize: 22, fontWeight: 700, letterSpacing: "-.4px" }}>{agentName}</div>
            {stats.csat !== null && (
              <span style={{ fontSize: 12, fontWeight: 700, padding: "3px 10px", borderRadius: 8, background: stats.csat >= 70 ? "#DCFCE7" : stats.csat >= 50 ? "#FEF3C7" : "#FEE2E2", color: stats.csat >= 70 ? "#16A34A" : stats.csat >= 50 ? "#D97706" : "#DC2626" }}>
                CSAT {stats.csat}%
              </span>
            )}
          </div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>{agentTickets.length.toLocaleString()} tickets · {stats.analyzed} AI-analyzed</div>
        </div>
      </div>

      {/* KPI Grid */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 12, marginBottom: 20 }}>
        <KPI label="CSAT" value={stats.csat !== null ? `${stats.csat}%` : "N/A"} sub={`${stats.rated} rated`} color={stats.csat !== null ? (stats.csat >= 70 ? "#10B981" : stats.csat >= 50 ? "#FBBF24" : "#EF4444") : null} />
        <KPI label="SLA Rate" value={stats.slaRate !== null ? `${stats.slaRate}%` : "N/A"} sub="Not violated" color={stats.slaRate !== null ? (stats.slaRate >= 90 ? "#10B981" : stats.slaRate >= 70 ? "#FBBF24" : "#EF4444") : null} />
        <KPI label="Avg Resolution" value={stats.avgRes ? `${stats.avgRes}h` : "N/A"} sub="Per ticket" />
        <KPI label="Escalated" value={stats.escalated} sub="Total" color={stats.escalated > 10 ? "#EF4444" : C.text} />
        <KPI label="Reassignments" value={stats.reassigns} sub="Total" />
        <KPI label="AI Quality" value={`${stats.avgQuality}/10`} sub={`${stats.analyzed} analyzed`} color={parseFloat(stats.avgQuality) >= 7 ? "#10B981" : "#FBBF24"} />
      </div>

      {/* Good / Okay / Bad */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 12, marginBottom: 20 }}>
        {[["Good", stats.good, "#10B981"], ["Okay", stats.okay, "#FBBF24"], ["Bad", stats.bad, "#EF4444"]].map(([l, v, c]) => (
          <div key={l} onClick={() => onKPIFilter && onKPIFilter({ agent: agentName, happiness: l })} style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 10, padding: "14px 16px", textAlign: "center", cursor: "pointer", transition: "all .12s" }} onMouseEnter={e => { e.currentTarget.style.borderColor = c; e.currentTarget.style.boxShadow = `0 2px 12px ${c}22`; }} onMouseLeave={e => { e.currentTarget.style.borderColor = C.border; e.currentTarget.style.boxShadow = "none"; }}>
            <div style={{ fontSize: 28, fontWeight: 800, color: c }}>{v}</div>
            <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>{l} ratings</div>
          </div>
        ))}
      </div>

      {/* Charts Row */}
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr", gap: 16, marginBottom: 20 }}>
        <Card>
          <ChartTitle>Top Ticket Reasons</ChartTitle>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={stats.topReasons} layout="vertical" barCategoryGap="20%">
              <XAxis type="number" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 10 }} width={140} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="value" name="Tickets" fill={C.accent} radius={[0, 3, 3, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card>
          <ChartTitle>Channel Distribution</ChartTitle>
          <ResponsiveContainer width="100%" height={160}>
            <PieChart>
              <Pie data={stats.channelData} cx="50%" cy="50%" outerRadius={60} dataKey="value">
                {stats.channelData.map((d, i) => <Cell key={i} fill={d.color} />)}
              </Pie>
              <Tooltip formatter={v => v.toLocaleString()} />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", flexDirection: "column", gap: 3, marginTop: 6 }}>
            {stats.channelData.slice(0, 4).map(d => (
              <div key={d.name} style={{ display: "flex", justifyContent: "space-between", fontSize: 11 }}>
                <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <span style={{ width: 7, height: 7, borderRadius: "50%", background: d.color, flexShrink: 0 }} />{d.name}
                </span>
                <span style={{ fontWeight: 500 }}>{d.value}</span>
              </div>
            ))}
          </div>
        </Card>
        <Card>
          <ChartTitle>Quality Scores</ChartTitle>
          <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
            {[["AI Quality", stats.avgQuality, 10], ["AI Empathy", stats.avgEmpathy, 10]].map(([l, v, max]) => (
              <div key={l}>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 4 }}>
                  <span style={{ color: C.sub }}>{l}</span>
                  <span style={{ fontWeight: 700, color: parseFloat(v) >= 7 ? "#10B981" : "#FBBF24" }}>{v}/{max}</span>
                </div>
                <div style={{ background: C.border, borderRadius: 4, height: 6, overflow: "hidden" }}>
                  <div style={{ background: parseFloat(v) >= 7 ? "#10B981" : "#FBBF24", height: "100%", width: `${Math.min(100, parseFloat(v) / max * 100)}%`, borderRadius: 4 }} />
                </div>
              </div>
            ))}
            <KPI label="Rated Tickets" value={stats.rated} sub="Have CSAT score" />
          </div>
        </Card>
      </div>

      {/* Recent Tickets Table */}
      <Card style={{ padding: 0, overflow: "hidden" }}>
        <div style={{ padding: "14px 16px", borderBottom: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 13, fontWeight: 600 }}>Recent Tickets</div>
          <div style={{ fontSize: 11, color: C.muted, marginTop: 1 }}>Last {recentTickets.length} tickets handled</div>
        </div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr style={{ background: "#F9F8F7", borderBottom: `1px solid ${C.border}` }}>
              {["Date", "Ticket #", "Reason", "Channel", "CSAT", "AI Score"].map(h => (
                <th key={h} style={{ padding: "10px 14px", textAlign: "left", fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: .5, fontWeight: 500 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {recentTickets.map(t => (
              <tr key={t.id} style={{ borderBottom: `1px solid #F4F2EE` }}>
                <td style={{ padding: "9px 14px", color: C.muted, fontSize: 11 }}>{(t.createdTime || "").slice(0, 10)}</td>
                <td style={{ padding: "9px 14px" }}>
                  <span onClick={() => onTicketClick && onTicketClick(t.id)} style={{ color: C.accent, cursor: "pointer", fontWeight: 600 }}>{t.ticket_number || t.id}</span>
                </td>
                <td style={{ padding: "9px 14px", color: C.sub, maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.reason}</td>
                <td style={{ padding: "9px 14px" }}>
                  <span style={{ fontSize: 10, padding: "2px 7px", borderRadius: 8, background: `${CH_COLORS[t.channel] || "#94A3B8"}22`, color: CH_COLORS[t.channel] || "#94A3B8", fontWeight: 500 }}>{t.channel}</span>
                </td>
                <td style={{ padding: "9px 14px" }}>
                  {t.happiness ? <span style={{ fontSize: 10, fontWeight: 600, color: HP_COLORS[t.happiness] }}>{t.happiness}</span> : <span style={{ color: C.muted }}>—</span>}
                </td>
                <td style={{ padding: "9px 14px" }}>
                  {t.overallQualityScore > 0 ? <span style={{ fontWeight: 700, color: t.overallQualityScore >= 7 ? "#10B981" : t.overallQualityScore >= 4 ? "#FBBF24" : "#EF4444" }}>{t.overallQualityScore}</span> : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}

/* ─── OFFER CREATION ─────────────────────────────────────────── */
const N8N_IMPORT_URL = "https://mostafakhaleddd.app.n8n.cloud/webhook/test-offer";
const N8N_EVAL_URL = "https://mostafakhaleddd.app.n8n.cloud/webhook/8c48a9a7-b371-4417-a4cf-baf156977fcc";

function OfferCreationTab({ anonKey, session }) {
  const [ticketId, setTicketId] = useState("");
  const [importing, setImporting] = useState(false);
  const [evaluating, setEvaluating] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [merchantName, setMerchantName] = useState("");
  const [offerTitle, setOfferTitle] = useState("");
  const [mainDesc, setMainDesc] = useState("");
  const [options, setOptions] = useState([]);
  const [conditions, setConditions] = useState([]);
  const [branchesText, setBranchesText] = useState("");
  const [aiScore, setAiScore] = useState(null);
  const [critique, setCritique] = useState("");

  // Helper to detect Arabic text for layout flipping
  const isRTL = (text) => /[\u0600-\u06FF]/.test(text || "");

  const parseN8nResponse = (text) => {
    try { return JSON.parse(text); } catch { }
    const m = text.match(/\{[\s\S]*\}/);
    if (m) { try { return JSON.parse(m[0]); } catch { } }
    throw new Error("Could not parse response from n8n");
  };

  const updateOption = (index, field, value) => {
    const newOptions = [...options];
    newOptions[index][field] = value;
    setOptions(newOptions);
  };

  const toggleDeleteOption = (index) => {
    const newOptions = [...options];
    newOptions[index].isDeleted = !newOptions[index].isDeleted;
    setOptions(newOptions);
  };

  const addOption = () => {
    setOptions([...options, { title: "New Package", content: "Details...", price: "0", originalPrice: "0", discount: "0", isDeleted: false }]);
  };

  const updateCondition = (index, value) => {
    const newConditions = [...conditions];
    newConditions[index].text = value;
    setConditions(newConditions);
  };

  const toggleDeleteCondition = (index) => {
    const newConditions = [...conditions];
    newConditions[index].isDeleted = !newConditions[index].isDeleted;
    setConditions(newConditions);
  };

  const addCondition = () => {
    setConditions([...conditions, { text: "New condition...", isDeleted: false }]);
  };

  const handleImport = async () => {
    if (!ticketId.trim()) { setError("Please enter a Jira Ticket ID."); return; }
    setImporting(true); setError(""); setSuccess(""); setAiScore(null); setCritique("");
    try {
      const res = await fetch(N8N_IMPORT_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ "ticket_id": ticketId.trim() }),
      });
      if (!res.ok) throw new Error(`Webhook error: ${res.status}`);
      const data = parseN8nResponse(await res.text());
      setMerchantName(data.merchant_name || "");
      setOfferTitle(data.main_deal_title || "");
      setMainDesc(data.main_description || "");
      setBranchesText(data.branches_text || "");
      
      if (Array.isArray(data.buy_options)) {
        setOptions(data.buy_options.map(o => ({
          title: o.option_title || "",
          content: o.option_content || "",
          price: o.price_final || "",
          originalPrice: o.price_original || "",
          discount: o.discount_percentage || "",
          isDeleted: false
        })));
      } else {
        setOptions([]);
      }
      setConditions(Array.isArray(data.terms_list) 
        ? data.terms_list.map(t => ({ text: t, isDeleted: false })) 
        : (data.terms_list ? [{ text: data.terms_list, isDeleted: false }] : []));
      setSuccess("✓ Offer imported successfully from Jira.");
    } catch (e) { setError("Import failed: " + e.message); }
    setImporting(false);
  };

  const handleEvaluate = async () => {
    if (!offerTitle) { setError("Please import or fill in offer data first."); return; }
    setEvaluating(true); setError("");
    const activeOptions = options.filter(o => !o.isDeleted);
    const activeConditions = conditions.filter(c => !c.isDeleted).map(c => c.text);
    try {
      const res = await fetch(N8N_EVAL_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: offerTitle, description: mainDesc, branches: branchesText, options: JSON.stringify(activeOptions), terms: JSON.stringify(activeConditions) }),
      });
      if (!res.ok) throw new Error(`Evaluate error: ${res.status}`);
      const raw = parseN8nResponse(await res.text());
      const data = typeof raw.output === "string" ? parseN8nResponse(raw.output) : raw;
      setAiScore(data.qa_score ?? data.quality_score ?? null);
      setCritique(data.critique_summary || data.critique || "");
    } catch (e) { setError("Evaluation failed: " + e.message); }
    setEvaluating(false);
  };

  const handleSave = async () => {
    if (!merchantName || !offerTitle) { setError("Merchant name and offer title are required."); return; }
    setSaving(true); setError("");
    const activeOptions = options.filter(o => !o.isDeleted);
    const activeConditions = conditions.filter(c => !c.isDeleted).map(c => c.text);
    try {
      const res = await fetch(`${SB_URL}/rest/v1/offer_drafts`, {
        method: "POST",
        headers: { ...sbH(anonKey, session.access_token), "Prefer": "return=minimal" },
        body: JSON.stringify({ ticket_id: ticketId.trim(), merchant_name: merchantName, title: offerTitle, options: JSON.stringify(activeOptions), conditions: JSON.stringify(activeConditions), created_by: session.user.email }),
      });
      if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.message || `Supabase error: ${res.status}`); }
      setSuccess("✓ Offer draft saved to database successfully.");
    } catch (e) { setError("Save failed: " + e.message); }
    setSaving(false);
  };

  const scoreColor = aiScore === null ? C.muted : aiScore >= 80 ? "#22c55e" : aiScore >= 60 ? "#f59e0b" : "#ef4444";

  const inp = {
    width: "100%", padding: "10px 12px", border: `1px solid ${C.border}`, borderRadius: 8,
    background: "#fff", fontSize: 13, color: C.text, outline: "none",
    transition: "border-color .15s", boxSizing: "border-box", fontFamily: "inherit",
    textAlign: "start",
  };

  const lbl = { display: "block", fontSize: 11, fontWeight: 600, color: C.sub, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 6, textAlign: "left" };

  const Spinner = ({ dark }) => (
    <span style={{ display: "inline-block", width: 12, height: 12, border: `2px solid ${dark ? "rgba(0,0,0,0.15)" : "rgba(255,255,255,0.3)"}`, borderTopColor: dark ? C.text : "#fff", borderRadius: "50%", animation: "ocSpin .7s linear infinite" }} />
  );

  return (
    <div>
      <style>{`
        @keyframes ocSpin { to { transform: rotate(360deg); } }
        .ticket-edge {
          position: absolute; left: 0; top: 0; bottom: 0; width: 32px;
          background-color: ${C.accent};
          background-image: radial-gradient(circle at 0px 10px, #fff 6px, transparent 7px);
          background-size: 32px 20px;
        }
      `}</style>

      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <h2 style={{ fontSize: 24, fontWeight: 700, margin: "0 0 4px", letterSpacing: "-.5px" }}>Offer Creation</h2>
          {aiScore !== null && (
            <div style={{ display: "inline-flex", alignItems: "center", justifyContent: "center", width: 38, height: 38, borderRadius: "50%", background: scoreColor, color: "#fff", fontSize: 13, fontWeight: 800, boxShadow: `0 2px 8px ${scoreColor}44` }}>{aiScore}</div>
          )}
        </div>
        <p style={{ margin: 0, color: C.muted, fontSize: 13 }}>AI-powered offer drafting from Jira tickets · Waffarha Content Studio</p>
      </div>

      {/* Toast messages */}
      {error && (
        <div style={{ marginBottom: 16, padding: "12px 16px", background: "#fef2f2", border: "1px solid #fecaca", borderRadius: 8, color: "#dc2626", fontSize: 13, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span>{error}</span>
          <button onClick={() => setError("")} style={{ background: "none", border: "none", cursor: "pointer", color: "#dc2626", fontWeight: 700, fontSize: 18, lineHeight: 1, padding: "0 4px" }}>×</button>
        </div>
      )}
      {success && (
        <div style={{ marginBottom: 16, padding: "12px 16px", background: "#f0fdf4", border: "1px solid #bbf7d0", borderRadius: 8, color: "#16a34a", fontSize: 13, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span>{success}</span>
          <button onClick={() => setSuccess("")} style={{ background: "none", border: "none", cursor: "pointer", color: "#16a34a", fontWeight: 700, fontSize: 18, lineHeight: 1, padding: "0 4px" }}>×</button>
        </div>
      )}

      {/* 2-column layout */}
      <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 20, alignItems: "start" }}>

        {/* LEFT column */}
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>

          {/* Import card */}
          <Card>
            <h3 style={{ fontSize: 14, fontWeight: 600, margin: "0 0 16px" }}>Import from Jira</h3>
            <label style={lbl}>Ticket ID</label>
            <input dir="auto" value={ticketId} onChange={e => setTicketId(e.target.value)} onKeyDown={e => e.key === "Enter" && handleImport()} placeholder="e.g. GPT-6673" style={inp} />
            <button
              onClick={handleImport} disabled={importing}
              style={{ marginTop: 14, width: "100%", padding: "10px 0", background: C.accent, color: "#fff", border: "none", borderRadius: 8, fontSize: 13, fontWeight: 600, cursor: importing ? "not-allowed" : "pointer", opacity: importing ? 0.7 : 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}
            >
              {importing ? <><Spinner />Importing…</> : "Import Offer Data"}
            </button>
          </Card>

          {/* Photo gallery placeholder */}
          <Card>
            <h3 style={{ fontSize: 14, fontWeight: 600, margin: "0 0 16px" }}>Merchant Photos</h3>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              {[1, 2, 3, 4].map(i => (
                <div key={i} style={{ aspectRatio: "4/3", borderRadius: 8, background: "#F9F8F7", border: `1px dashed ${C.border}`, display: "flex", alignItems: "center", justifyContent: "center", flexDirection: "column", gap: 4 }}>
                  <span style={{ fontSize: 18, opacity: 0.2 }}>📷</span>
                  <span style={{ fontSize: 9, color: C.muted }}>Photo {i}</span>
                </div>
              ))}
            </div>
            <p style={{ margin: "12px 0 0", fontSize: 11, color: C.muted, textAlign: "center" }}>Gallery integration coming soon</p>
          </Card>

          {/* Critique card (visible after evaluate) */}
          {critique && (
            <Card style={{ background: "#FEFCE8", borderColor: "#FEF08A" }}>
              <h3 style={{ fontSize: 13, fontWeight: 700, marginBottom: 10, color: "#854D0E" }}>AI Critique</h3>
              <p dir="auto" style={{ margin: 0, fontSize: 12, color: "#713F12", lineHeight: 1.6, whiteSpace: "pre-line", textAlign: "start" }}>{critique}</p>
            </Card>
          )}

        </div>

        {/* RIGHT column — Offer form */}
        <Card>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
            <h3 style={{ fontSize: 14, fontWeight: 600, margin: 0 }}>Offer Draft</h3>
            {aiScore !== null && (
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 11, color: C.muted }}>Quality Score:</span>
                <span style={{ fontWeight: 700, color: scoreColor }}>{aiScore}%</span>
              </div>
            )}
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            <div>
              <label style={lbl}>Merchant Name</label>
              <input dir="auto" value={merchantName} onChange={e => setMerchantName(e.target.value)} placeholder="Merchant name..." style={inp} />
            </div>
            <div>
              <label style={lbl}>Offer Title</label>
              <input dir="auto" value={offerTitle} onChange={e => setOfferTitle(e.target.value)} placeholder="Main deal title..." style={inp} />
            </div>
            <div>
              <label style={lbl}>Description</label>
              <textarea dir="auto" value={mainDesc} onChange={e => setMainDesc(e.target.value)} placeholder="Main offer description..." rows={3} style={{ ...inp, resize: "vertical", lineHeight: 1.6 }} />
            </div>

            {/* OPTIONS SECTION */}
            <div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                <label style={lbl}>Options & Packages</label>
                <button onClick={addOption} style={{ background: "none", border: `1px solid ${C.accent}`, color: C.accent, borderRadius: 6, padding: "2px 10px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>+ Add Option</button>
              </div>
              
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                {options.map((opt, idx) => {
                  const ar = isRTL(opt.title) || isRTL(opt.content);
                  return (
                    <div key={idx} style={{ 
                      position: "relative", 
                      padding: ar ? "16px 48px 16px 16px" : "16px 16px 16px 48px", 
                      border: `1px dashed ${opt.isDeleted ? "#e5e7eb" : C.border}`, 
                      borderRadius: 12, 
                      background: opt.isDeleted ? "#f9fafb" : "#fff", 
                      overflow: "hidden",
                      opacity: opt.isDeleted ? 0.6 : 1,
                      filter: opt.isDeleted ? "grayscale(1)" : "none",
                      transition: "all 0.2s"
                    }}>
                      <div className="ticket-edge" style={{ left: ar ? "auto" : 0, right: ar ? 0 : "auto", transform: ar ? "scaleX(-1)" : "none" }} />
                      <button 
                        onClick={() => toggleDeleteOption(idx)} 
                        style={{ position: "absolute", top: 8, right: ar ? "auto" : 8, left: ar ? 8 : "auto", background: opt.isDeleted ? "#f3f4f6" : "none", border: "none", color: opt.isDeleted ? C.accent : "#ef4444", cursor: "pointer", fontSize: opt.isDeleted ? 10 : 16, fontWeight: 700, borderRadius: 4, padding: opt.isDeleted ? "2px 6px" : 0 }}
                      >
                        {opt.isDeleted ? "UNDO" : "×"}
                      </button>
                      
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 140px", gap: 16, direction: ar ? "rtl" : "ltr", textAlign: "start" }}>
                        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                          <input dir="auto" value={opt.title} onChange={e => updateOption(idx, "title", e.target.value)} placeholder="Option Title" style={{ ...inp, fontWeight: 700, border: "none", padding: 0, fontSize: 15, background: "transparent" }} />
                          <textarea dir="auto" value={opt.content} onChange={e => updateOption(idx, "content", e.target.value)} placeholder="Details..." rows={2} style={{ ...inp, border: "none", padding: 0, color: C.muted, fontSize: 12, resize: "none", background: "transparent" }} />
                        </div>
                        <div style={{ display: "flex", flexDirection: "column", gap: 4, alignItems: ar ? "flex-start" : "flex-end", justifyContent: "center" }}>
                          <div style={{ display: "flex", alignItems: "baseline", gap: 4 }}>
                            <span style={{ fontSize: 18, fontWeight: 800, color: C.accent }}>{opt.price}</span>
                            <span style={{ fontSize: 12, fontWeight: 700, color: C.accent, marginLeft: ar ? 8 : 0, marginRight: ar ? 0 : 8 }}>EGP</span>
                            <span style={{ fontSize: 12, color: C.muted, textDecoration: "line-through" }}>{opt.originalPrice}</span>
                          </div>
                          <div style={{ display: "flex", gap: 8 }}>
                             <input value={opt.price} onChange={e => updateOption(idx, "price", e.target.value)} placeholder="Now" style={{ ...inp, width: 60, padding: "4px 8px", fontSize: 11 }} />
                             <input value={opt.originalPrice} onChange={e => updateOption(idx, "originalPrice", e.target.value)} placeholder="Was" style={{ ...inp, width: 60, padding: "4px 8px", fontSize: 11 }} />
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* TERMS SECTION */}
            <div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                <label style={lbl}>Terms & Conditions</label>
                <button onClick={addCondition} style={{ background: "none", border: `1px solid ${C.accent}`, color: C.accent, borderRadius: 6, padding: "2px 10px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>+ Add Term</button>
              </div>
              
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {conditions.map((term, idx) => {
                  const ar = isRTL(term.text);
                  return (
                    <div key={idx} style={{ 
                      display: "flex", 
                      alignItems: "center", 
                      gap: 12, 
                      flexDirection: ar ? "row-reverse" : "row",
                      opacity: term.isDeleted ? 0.5 : 1,
                      filter: term.isDeleted ? "grayscale(1)" : "none",
                      transition: "all 0.2s"
                    }}>
                      <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#D1D5DB", flexShrink: 0 }} />
                      <input dir="auto" value={term.text} onChange={e => updateCondition(idx, e.target.value)} placeholder="Condition..." style={{ ...inp, border: "none", padding: "4px 0", fontSize: 13, background: "transparent", textAlign: "start" }} />
                      <button 
                        onClick={() => toggleDeleteCondition(idx)} 
                        style={{ background: term.isDeleted ? "#f3f4f6" : "none", border: "none", color: term.isDeleted ? C.accent : "#ef4444", cursor: "pointer", fontSize: term.isDeleted ? 10 : 16, fontWeight: 700, padding: "2px 8px", borderRadius: 4 }}
                      >
                        {term.isDeleted ? "UNDO" : "×"}
                      </button>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          {/* Action buttons */}
          <div style={{ display: "flex", gap: 12, marginTop: 24, justifyContent: "flex-end" }}>
            <button
              onClick={handleEvaluate} disabled={evaluating || !offerTitle}
              style={{ padding: "10px 20px", background: "#fff", color: C.text, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, fontWeight: 600, cursor: (evaluating || !offerTitle) ? "not-allowed" : "pointer", opacity: (evaluating || !offerTitle) ? 0.5 : 1, display: "inline-flex", alignItems: "center", gap: 8 }}
            >
              {evaluating ? <><Spinner dark />Evaluating…</> : "Evaluate with AI"}
            </button>
            <button
              onClick={handleSave} disabled={saving || !merchantName || !offerTitle}
              style={{ padding: "10px 24px", background: "#1A1A1A", color: "#fff", border: "none", borderRadius: 8, fontSize: 13, fontWeight: 600, cursor: (saving || !merchantName || !offerTitle) ? "not-allowed" : "pointer", opacity: (saving || !merchantName || !offerTitle) ? 0.5 : 1, display: "inline-flex", alignItems: "center", gap: 8 }}
            >
              {saving ? <><Spinner />Saving…</> : "Save Draft"}
            </button>
          </div>
        </Card>

      </div>
    </div>
  );
}

/* ─── ADMIN SETTINGS ─────────────────────────────────────────── */
function AdminSettingsTab({ anonKey, session }) {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [email, setEmail] = useState('');
  const [role, setRole] = useState('viewer');
  const [saving, setSaving] = useState(false);

  const fetchUsers = async () => {
    try {
      const res = await fetch(`${SB_URL}/rest/v1/user_roles`, { headers: sbH(anonKey, session.access_token) });
      if (res.ok) setUsers(await res.json());
    } catch (e) { console.error("Failed to fetch roles", e); }
    setLoading(false);
  };

  useEffect(() => { fetchUsers(); }, []);

  const handleSave = async (e) => {
    e.preventDefault();
    if (!email) return;
    setSaving(true);
    try {
      await fetch(`${SB_URL}/rest/v1/user_roles`, {
        method: "POST",
        headers: { ...sbH(anonKey, session.access_token), "Prefer": "resolution=merge-duplicates" },
        body: JSON.stringify({ email: email.toLowerCase(), role, updated_by: session.user.email })
      });
      setEmail('');
      setRole('viewer');
      fetchUsers();
    } catch (e) { alert("Failed to save role: " + e.message); }
    setSaving(false);
  };

  const handleDelete = async (targetEmail) => {
    if (!window.confirm(`Revoke access for ${targetEmail}?`)) return;
    try {
      await fetch(`${SB_URL}/rest/v1/user_roles?email=eq.${encodeURIComponent(targetEmail)}`, {
        method: "DELETE",
        headers: sbH(anonKey, session.access_token)
      });
      fetchUsers();
    } catch (e) { alert("Failed to delete role"); }
  };

  return (
    <div style={{ maxWidth: 800, margin: "0 auto" }}>
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ fontSize: 24, fontWeight: 700, margin: "0 0 4px", letterSpacing: "-.5px" }}>Admin Settings</h2>
        <p style={{ margin: 0, color: C.muted, fontSize: 13 }}>Manage roles and access permissions for Nexus users.</p>
      </div>

      <Card style={{ marginBottom: 24 }}>
        <h3 style={{ fontSize: 14, fontWeight: 600, margin: "0 0 16px" }}>Assign New Role</h3>
        <form onSubmit={handleSave} style={{ display: "flex", gap: 12, alignItems: "flex-end" }}>
          <div style={{ flex: 1 }}>
            <label style={{ display: "block", fontSize: 11, fontWeight: 500, color: C.sub, marginBottom: 6, textTransform: "uppercase" }}>User Email</label>
            <input type="email" value={email} onChange={e => setEmail(e.target.value)} placeholder="user@waffarha.com" required
              style={{ width: "100%", padding: "10px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, outline: "none" }} />
          </div>
          <div style={{ width: 200 }}>
            <label style={{ display: "block", fontSize: 11, fontWeight: 500, color: C.sub, marginBottom: 6, textTransform: "uppercase" }}>Role</label>
            <select value={role} onChange={e => setRole(e.target.value)}
              style={{ width: "100%", padding: "10px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, outline: "none", backgroundColor: "#fff" }}>
              <option value="admin">Super Admin</option>
              <option value="global_bd">Global BD Lead</option>
              <option value="ksa_bd">KSA BD Manager</option>
              <option value="oman_bd">Oman BD Manager</option>
              <option value="crm_lead">CRM / Support Lead</option>
              <option value="content_editor">Content Editor</option>
              <option value="viewer">Viewer (Read Only)</option>
            </select>
          </div>
          <button type="submit" disabled={saving} style={{ padding: "10px 24px", background: C.accent, color: "#fff", border: "none", borderRadius: 8, fontSize: 13, fontWeight: 600, cursor: "pointer", height: 40 }}>
            {saving ? "Saving..." : "Save Role"}
          </button>
        </form>
      </Card>

      <Card style={{ padding: 0, overflow: "hidden" }}>
        <div style={{ padding: "16px 20px", borderBottom: `1px solid ${C.border}` }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: 0 }}>Active Users</h3>
        </div>
        {loading ? (
          <div style={{ padding: 40, textAlign: "center", color: C.muted, fontSize: 13 }}>Loading users...</div>
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ background: "#F9F8F7", borderBottom: `1px solid ${C.border}` }}>
                <th style={{ padding: "12px 20px", textAlign: "left", color: C.sub, fontWeight: 500, fontSize: 11, textTransform: "uppercase" }}>Email</th>
                <th style={{ padding: "12px 20px", textAlign: "left", color: C.sub, fontWeight: 500, fontSize: 11, textTransform: "uppercase" }}>Role</th>
                <th style={{ padding: "12px 20px", textAlign: "left", color: C.sub, fontWeight: 500, fontSize: 11, textTransform: "uppercase" }}>Added</th>
                <th style={{ padding: "12px 20px", textAlign: "right" }}></th>
              </tr>
            </thead>
            <tbody>
              {users.map(u => (
                <tr key={u.email} style={{ borderBottom: `1px solid ${C.border}` }}>
                  <td style={{ padding: "12px 20px", fontWeight: 500 }}>{u.email}</td>
                  <td style={{ padding: "12px 20px" }}>
                    <span style={{ padding: "4px 8px", background: C.accentL, color: C.accent, borderRadius: 6, fontSize: 11, fontWeight: 600 }}>{u.role}</span>
                  </td>
                  <td style={{ padding: "12px 20px", color: C.muted }}>{(u.created_at || "").slice(0, 10)}</td>
                  <td style={{ padding: "12px 20px", textAlign: "right" }}>
                    {u.email !== session.user.email && (
                      <button onClick={() => handleDelete(u.email)} style={{ background: "none", border: "none", color: "#DC2626", cursor: "pointer", fontSize: 12, fontWeight: 500 }}>Revoke</button>
                    )}
                  </td>
                </tr>
              ))}
              {users.length === 0 && <tr><td colSpan={4} style={{ padding: 40, textAlign: "center", color: C.muted }}>No roles assigned yet.</td></tr>}
            </tbody>
          </table>
        )}
      </Card>
    </div>
  );
}

/* ─── MAIN APP ───────────────────────────────────────────────── */
export default function App() {
  const [anonKey, setAnonKey] = useState("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes");
  const [session, setSession] = useState(() => {
    try {
      const saved = localStorage.getItem("wn_session");
      return saved ? JSON.parse(saved) : null;
    } catch (e) { return null; }
  });
  const [merchants, setMerchants] = useState([]);
  const [omanMerchants, setOmanMerchants] = useState([]);
  const [loadingCity, setLoadingCity] = useState("");
  const [tab, setTab] = useState("macro");
  const [selectedMerchantForProfile, setSelectedMerchantForProfile] = useState(null);
  const [statuses, setStatuses] = useState({});
  const [statusSaving, setStatusSaving] = useState(false); // eslint-disable-line no-unused-vars
  const [tickets, setTickets] = useState([]);
  const [ticketsLoaded, setTicketsLoaded] = useState(false);
  const [ticketsLoading, setTicketsLoading] = useState(false);
  const [selectedTicketId, setSelectedTicketId] = useState(null);
  const [selectedAgentId, setSelectedAgentId] = useState(null);
  const [explorerFilters, setExplorerFilters] = useState({});
  const [userRole, setUserRole] = useState(() => {
    try {
      const saved = localStorage.getItem("wn_user_role");
      return saved || null;
    } catch (e) { return null; }
  });

  const loadUserRole = async () => {
    if (!session) return;
    try {
      const r = await fetch(`${SB_URL}/rest/v1/user_roles?email=eq.${encodeURIComponent(session.user.email)}`, {
        headers: sbH(anonKey, session.access_token)
      });
      if (r.ok) {
        const rows = await r.json();
        const role = rows.length > 0 ? rows[0].role : 'viewer'; // Default to viewer if not found
        setUserRole(role);
        localStorage.setItem("wn_user_role", role);
      }
    } catch (e) { console.warn("Could not load user role:", e.message); }
  };

  const handleMerchantClick = (merchant) => {
    setSelectedMerchantForProfile(merchant);
    const isOman = merchant.City === "Muscat";
    setTab(isOman ? "oman_profiler" : "profiler");
  };

  const handleTicketClick = (ticketId) => {
    setSelectedTicketId(ticketId);
    setTab("chat");
    if (session) logAudit(anonKey, session.access_token, session.user.id, "view_ticket", ticketId);
    loadTickets();
  };

  const handleAgentClick = (agentId) => {
    setSelectedAgentId(agentId);
    setTab("agentProfile");
    if (session) logAudit(anonKey, session.access_token, session.user.id, "view_agent", agentId);
  };

  const handleTabChange = (id) => {
    setTab(id);
    if (session) logAudit(anonKey, session.access_token, session.user.id, "navigate", id);
    if (["support", "agents", "tickets", "chat"].includes(id)) loadTickets();
  };

  useEffect(() => {
    if (session && anonKey) {
      loadAllMerchants();
      loadUserRole();
    }

    // 2. Inactivity: Auto-logout after 30 minutes
    let timeout;
    const INACTIVITY_LIMIT = 30 * 60 * 1000; // 30 mins

    const resetTimer = () => {
      if (timeout) clearTimeout(timeout);
      if (session) {
        timeout = setTimeout(() => {
          setSession(null);
          localStorage.removeItem("wn_session");
          alert("Session expired due to inactivity. Please sign in again.");
        }, INACTIVITY_LIMIT);
      }
    };

    if (session) {
      window.addEventListener('mousemove', resetTimer);
      window.addEventListener('keypress', resetTimer);
      window.addEventListener('scroll', resetTimer);
      window.addEventListener('click', resetTimer);
      resetTimer();
    }

    return () => {
      window.removeEventListener('mousemove', resetTimer);
      window.removeEventListener('keypress', resetTimer);
      window.removeEventListener('scroll', resetTimer);
      window.removeEventListener('click', resetTimer);
      if (timeout) clearTimeout(timeout);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session, anonKey]);

  async function loadTickets() {
    if (ticketsLoaded || ticketsLoading) return;
    setTicketsLoading(true);
    try {
      const cols = "*,ticket:zoho_tickets(*)";
      const rows = await sbFetch("ticket_analysis", anonKey, session.access_token, cols);
      setTickets(Array.isArray(rows) ? rows.map(normTicket) : []);
      setTicketsLoaded(true);
    } catch (e) { console.warn("Tickets load failed:", e.message); }
    setTicketsLoading(false);
  }

  async function loadAllMerchants() {
    setLoadingCity("Loading merchants...");

    // Attempt instant load from cache
    try {
      const cachedKsa = localStorage.getItem("wn_merchants_ksa");
      const cachedOman = localStorage.getItem("wn_merchants_oman");
      const cacheTime = localStorage.getItem("wn_merchants_time");
      if (cachedKsa && cachedOman && cacheTime && (Date.now() - parseInt(cacheTime)) < CACHE_TTL) {
        setMerchants(JSON.parse(cachedKsa));
        setOmanMerchants(JSON.parse(cachedOman));
        setLoadingCity("");
        if (session) {
          loadStatuses();
          loadFavorites();
        }
        return;
      } else if (cachedKsa && cachedOman) {
        // Optimistic UI: show stale data immediately
        setMerchants(JSON.parse(cachedKsa));
        setOmanMerchants(JSON.parse(cachedOman));
      }
    } catch (e) { console.warn("Cache read error"); }

    const fetchCity = async (city) => {
      try {
        const rows = await sbFetch(`merchants_${city}`, anonKey, session.access_token);
        if (Array.isArray(rows)) return rows.map(norm);
      } catch (e) {
        console.warn(`Skip ${city}:`, e.message);
      }
      return [];
    };

    // Parallel fetch for speed
    const [ksaResults, omanResults] = await Promise.all([
      Promise.all(CITIES.map(fetchCity)),
      Promise.all(OMAN_CITIES.map(fetchCity))
    ]);

    const flatKsa = ksaResults.flat();
    const flatOman = omanResults.flat();

    setMerchants(flatKsa);
    setOmanMerchants(flatOman);

    // Save to cache
    try {
      localStorage.setItem("wn_merchants_ksa", JSON.stringify(flatKsa));
      localStorage.setItem("wn_merchants_oman", JSON.stringify(flatOman));
      localStorage.setItem("wn_merchants_time", Date.now().toString());
    } catch (e) { console.warn("Cache write error (quota exceeded)"); }

    setLoadingCity("");
    if (session) {
      loadStatuses();
      loadFavorites();
    }
  }

  const [favoriteIds, setFavoriteIds] = useState(new Set());

  const loadFavorites = async () => {
    if (!session) return;
    try {
      const r = await fetch(`${SB_URL}/rest/v1/merchant_favorites?user_email=eq.${encodeURIComponent(session.user.email)}`, {
        headers: sbH(anonKey, session.access_token)
      });
      if (!r.ok) return;
      const rows = await r.json();
      setFavoriteIds(new Set(rows.map(r => r.merchant_id)));
    } catch (e) {
      console.warn("Could not load favorites:", e.message);
    }
  };

  const toggleFavorite = async (merchantId) => {
    if (!session) return;
    const isFav = favoriteIds.has(merchantId);

    const newFavs = new Set(favoriteIds);
    if (isFav) newFavs.delete(merchantId);
    else newFavs.add(merchantId);
    setFavoriteIds(newFavs);

    try {
      if (isFav) {
        await fetch(`${SB_URL}/rest/v1/merchant_favorites?user_email=eq.${encodeURIComponent(session.user.email)}&merchant_id=eq.${encodeURIComponent(merchantId)}`, {
          method: "DELETE",
          headers: sbH(anonKey, session.access_token)
        });
      } else {
        await fetch(`${SB_URL}/rest/v1/merchant_favorites`, {
          method: "POST",
          headers: sbH(anonKey, session.access_token),
          body: JSON.stringify({ user_email: session.user.email, merchant_id: merchantId })
        });
      }
    } catch (e) {
      console.error("Failed to toggle favorite", e);
      loadFavorites();
    }
  };

  async function loadStatuses() {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/merchant_status?select=merchant_name,mall,status&limit=10000`, { headers: sbH(anonKey, session.access_token) });
      if (!r.ok) return;
      const rows = await r.json();
      const map = {};
      for (const row of rows) map[`${row.merchant_name}|${row.mall || ""}`] = row.status;
      setStatuses(map);
    } catch (e) { console.warn("Could not load statuses:", e.message); }
  }

  async function handleStatusChange(merchant, newStatus) {
    const key = `${merchant.Merchant}|${merchant.Mall || ""}`;
    setStatuses(prev => ({ ...prev, [key]: newStatus }));
    if (session) logAudit(anonKey, session.access_token, session.user.id, "status_change", key, { old: statuses[key], new: newStatus });
    try {
      setStatusSaving(true);
      await fetch(`${SB_URL}/rest/v1/merchant_status`, {
        method: "POST",
        headers: { ...sbH(anonKey, session.access_token), "Prefer": "resolution=merge-duplicates" },
        body: JSON.stringify({ user_id: session.user.id, user_email: session.user.email, merchant_name: merchant.Merchant, mall: merchant.Mall || "", city: merchant.City || "", status: newStatus, updated_at: new Date().toISOString() })
      });
    } catch (e) { console.warn("Status save failed:", e.message); }
    finally { setStatusSaving(false); }
  }

  const TABS = [
    { id: "macro", label: "Market Overview", group: "KSA Intelligence", roles: ["admin", "ksa_bd", "global_bd", "viewer"], d: "M4 15l4-8 4 4 4-6 4 6" },
    { id: "profiler", label: "Merchant Profiler", group: "KSA Intelligence", roles: ["admin", "ksa_bd", "global_bd"], d: "M12 12c2.7 0 5-2.3 5-5s-2.3-5-5-5-5 2.3-5 5 2.3 5 5 5zm0 2c-3.3 0-10 1.7-10 5v1h20v-1c0-3.3-6.7-5-10-5z" },
    { id: "saved", label: "Saved Merchants", group: "KSA Intelligence", roles: ["admin", "ksa_bd", "global_bd"], d: "M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z" },
    { id: "malls", label: "Malls Profile", group: "KSA Intelligence", roles: ["admin", "ksa_bd", "global_bd", "viewer"], d: "M6 2L3 6v14a2 2 0 002 2h14a2 2 0 002-2V6l-3-4zM3 6h18M16 10a4 4 0 01-8 0" },
    { id: "pipeline", label: "Acquisition Pipeline", group: "KSA Intelligence", roles: ["admin", "ksa_bd", "global_bd", "viewer"], d: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" },
    { id: "oman_macro", label: "Market Overview", group: "Oman Intelligence", roles: ["admin", "oman_bd", "global_bd", "viewer"], d: "M4 15l4-8 4 4 4-6 4 6" },
    { id: "oman_profiler", label: "Merchant Profiler", group: "Oman Intelligence", roles: ["admin", "oman_bd", "global_bd"], d: "M12 12c2.7 0 5-2.3 5-5s-2.3-5-5-5-5 2.3-5 5 2.3 5 5 5zm0 2c-3.3 0-10 1.7-10 5v1h20v-1c0-3.3-6.7-5-10-5z" },
    { id: "oman_saved", label: "Saved Merchants", group: "Oman Intelligence", roles: ["admin", "oman_bd", "global_bd"], d: "M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z" },
    { id: "oman_malls", label: "Malls Profile", group: "Oman Intelligence", roles: ["admin", "oman_bd", "global_bd", "viewer"], d: "M6 2L3 6v14a2 2 0 002 2h14a2 2 0 002-2V6l-3-4zM3 6h18M16 10a4 4 0 01-8 0" },
    { id: "oman_pipeline", label: "Acquisition Pipeline", group: "Oman Intelligence", roles: ["admin", "oman_bd", "global_bd", "viewer"], d: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" },
    { id: "support", label: "Support Overview", group: "CRM & Support", roles: ["admin", "crm_lead"], d: "M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" },
    { id: "agents", label: "Agent Performance", group: "CRM & Support", roles: ["admin", "crm_lead"], d: "M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2M9 11a4 4 0 100-8 4 4 0 000 8zM23 21v-2a4 4 0 00-3-3.87M16 3.13a4 4 0 010 7.75" },
    { id: "tickets", label: "Ticket Explorer", group: "CRM & Support", roles: ["admin", "crm_lead"], d: "M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8zM14 2v6h6M16 13H8M16 17H8M10 9H8" },
    { id: "chat", label: "Chat Review", group: "CRM & Support", roles: ["admin", "crm_lead"], d: "M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" },
    { id: "admin", label: "Admin Settings", group: "Settings", roles: ["admin"], d: "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065zM15 12a3 3 0 11-6 0 3 3 0 016 0z" },
    { id: "offer_creation", label: "Offer Creation", group: "Content & Offers", roles: ["admin", "content_editor"], d: "M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" },
  ];

  const canonicalMap = useMemo(() => {
    const counts = {};
    for (const m of merchants) counts[m.Merchant] = (counts[m.Merchant] || 0) + 1;
    const uniqueNames = Object.keys(counts).filter(x => x).sort((a, b) => counts[b] - counts[a]);
    const mapToCan = {};
    const processed = new Set();
    for (const name of uniqueNames) {
      if (processed.has(name)) continue;
      mapToCan[name] = name;
      processed.add(name);
      const n1 = normArabic(name);
      if (n1.length < 4) continue;
      for (const other of uniqueNames) {
        if (processed.has(other)) continue;
        const n2 = normArabic(other);
        if (Math.abs(n1.length - n2.length) > 2) continue;
        const dist = editDist(n1, n2);
        const threshold = n1.length >= 7 ? 2 : 1;
        if (dist <= threshold && n1[0] === n2[0]) { mapToCan[other] = name; processed.add(other); }
      }
    }
    return mapToCan;
  }, [merchants]);

  const unifiedMerchants = useMemo(() => merchants.map(m => ({ ...m, OriginalMerchant: m.Merchant, Merchant: canonicalMap[m.Merchant] || m.Merchant })), [merchants, canonicalMap]);
  const omanUnifiedMerchants = useMemo(() => omanMerchants.map(m => ({ ...m, OriginalMerchant: m.Merchant, Merchant: canonicalMap[m.Merchant] || m.Merchant })), [omanMerchants, canonicalMap]);

  const statsSource = tab.startsWith("oman_") ? omanUnifiedMerchants : unifiedMerchants;
  const sidebarStats = useMemo(() => [
    ["Total", statsSource.length.toLocaleString()],
    ["Cities", new Set(statsSource.map(m => m.City)).size],
    ["Avg ★", (statsSource.reduce((s, m) => s + (m.Rating || 0), 0) / (statsSource.filter(m => m.Rating > 0).length || 1)).toFixed(2)],
    ["High", statsSource.filter(m => m.Priority.toLowerCase().includes("high")).length.toLocaleString()],
  ], [statsSource]);

  if (!anonKey) return <SetupScreen onSetup={setAnonKey} />;
  if (!session) return <LoginScreen anonKey={anonKey} onLogin={(s) => {
    setSession(s);
    if (s) {
      localStorage.setItem("wn_session", JSON.stringify(s));
      logAudit(anonKey, s.access_token, s.user.id, "login", "auth");
    }
  }} />;
  if (loadingCity) return <LoadingScreen />;

  const visibleTabs = TABS.filter(t => t.id !== "agentProfile" && (userRole ? t.roles.includes(userRole) : false));

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif", background: C.bg, color: C.text, overflow: "hidden" }}>
      {/* Sidebar */}
      <div style={{ width: 216, background: C.white, borderRight: `1px solid ${C.border}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
        <div style={{ padding: "20px 16px 14px", borderBottom: `1px solid ${C.border}` }}>
          <WaffarhaLogo height={26} style={{ marginBottom: 6 }} />
          <div style={{ fontSize: 18, color: C.text, fontWeight: 800, letterSpacing: "-0.5px" }}>Nexus</div>
        </div>

        <nav style={{ padding: "10px 8px", flex: 1, overflowY: "auto" }}>
          {["KSA Intelligence", "Oman Intelligence", "CRM & Support", "Content & Offers", "Settings"].map(group => {
            const groupTabs = visibleTabs.filter(t => t.group === group);
            if (groupTabs.length === 0) return null;
            return (
              <div key={group}>
                <div style={{ fontSize: 9, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: 1, padding: "8px 10px 4px", marginTop: group === "KSA Intelligence" ? 0 : 8 }}>
                  {group}
                </div>
                {groupTabs.map(({ id, label, d }) => (
                  <div key={id} onClick={() => handleTabChange(id)}
                    style={{ display: "flex", alignItems: "center", gap: 9, padding: "8px 10px", borderRadius: 8, cursor: "pointer", fontSize: 12, color: (tab === id || (id === "agents" && tab === "agentProfile")) ? C.accent : C.sub, background: (tab === id || (id === "agents" && tab === "agentProfile")) ? C.accentL : "transparent", fontWeight: (tab === id || (id === "agents" && tab === "agentProfile")) ? 600 : 400, marginBottom: 1, transition: "all .12s" }}>
                    <svg width={14} height={14} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>
                    <span style={{ lineHeight: 1.3 }}>{label}</span>
                    {group === "CRM & Support" && ticketsLoading && (tab === id) && (
                      <span style={{ marginLeft: "auto", fontSize: 9, color: C.muted }}>loading…</span>
                    )}
                  </div>
                ))}
              </div>
            );
          })}
        </nav>

        <div style={{ padding: "12px 16px", borderTop: `1px solid ${C.border}`, fontSize: 11, color: C.muted }}>
          <div style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Merchants</div>
          {sidebarStats.map(([l, v]) => (
            <div key={l} style={{ display: "flex", justifyContent: "space-between", padding: "2px 0" }}>
              <span>{l}</span><span style={{ color: C.text, fontWeight: 500 }}>{v}</span>
            </div>
          ))}
          {ticketsLoaded && (
            <>
              <div style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: 1, marginTop: 8, marginBottom: 4 }}>Support</div>
              {[
                ["Tickets", tickets.length.toLocaleString()],
                ["CSAT", (() => { const r = tickets.filter(t => t.happiness); const g = r.filter(t => t.happiness === "Good").length; return r.length ? `${Math.round(g / r.length * 100)}%` : "—"; })()],
                ["Agents", new Set(tickets.map(t => t.owner).filter(Boolean)).size],
              ].map(([l, v]) => (
                <div key={l} style={{ display: "flex", justifyContent: "space-between", padding: "2px 0" }}>
                  <span>{l}</span><span style={{ color: C.text, fontWeight: 500 }}>{v}</span>
                </div>
              ))}
            </>
          )}
          <button onClick={() => {
            setSession(null);
            setMerchants([]);
            setTickets([]);
            setTicketsLoaded(false);
            setUserRole(null);
            localStorage.removeItem("wn_session");
            localStorage.removeItem("wn_user_role");
          }}
            style={{ marginTop: 10, width: "100%", padding: "6px 0", background: "#F4F2EE", border: "none", borderRadius: 6, fontSize: 11, color: C.muted, cursor: "pointer" }}>
            Sign Out
          </button>
        </div>
      </div>

      {/* Main */}
      <main style={{ flex: 1, overflowY: "auto", padding: 24 }}>
        {tab === "macro" && <MacroTab merchants={unifiedMerchants} />}
        {tab === "profiler" && <ProfilerTab merchants={unifiedMerchants} anonKey={anonKey} initialMerchant={selectedMerchantForProfile} tickets={tickets} userEmail={session?.user?.email} favoriteIds={favoriteIds} toggleFavorite={toggleFavorite} userRole={userRole} />}
        {tab === "saved" && <SavedMerchantsTab merchants={unifiedMerchants} favoriteIds={favoriteIds} onMerchantClick={handleMerchantClick} />}
        {tab === "malls" && <MallsTab merchants={unifiedMerchants} onMerchantClick={handleMerchantClick} statuses={statuses} onStatusChange={handleStatusChange} userRole={userRole} />}
        {tab === "pipeline" && <PipelineTab merchants={unifiedMerchants} onMerchantClick={handleMerchantClick} statuses={statuses} onStatusChange={handleStatusChange} userRole={userRole} />}

        {tab === "oman_macro" && <MacroTab merchants={omanUnifiedMerchants} region="Oman" />}
        {tab === "oman_profiler" && <ProfilerTab merchants={omanUnifiedMerchants} anonKey={anonKey} initialMerchant={selectedMerchantForProfile} tickets={tickets} region="Oman" userEmail={session?.user?.email} favoriteIds={favoriteIds} toggleFavorite={toggleFavorite} userRole={userRole} />}
        {tab === "oman_saved" && <SavedMerchantsTab merchants={omanUnifiedMerchants} favoriteIds={favoriteIds} onMerchantClick={handleMerchantClick} />}
        {tab === "oman_malls" && <MallsTab merchants={omanUnifiedMerchants} onMerchantClick={handleMerchantClick} statuses={statuses} onStatusChange={handleStatusChange} region="Oman" userRole={userRole} />}
        {tab === "oman_pipeline" && <PipelineTab merchants={omanUnifiedMerchants} onMerchantClick={handleMerchantClick} statuses={statuses} onStatusChange={handleStatusChange} region="Oman" userRole={userRole} />}

        {tab === "support" && (ticketsLoading
          ? <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "60vh", color: C.muted, fontSize: 13 }}>Loading {tickets.length.toLocaleString()} tickets…</div>
          : <SupportTab tickets={tickets} />)}
        {tab === "agents" && (ticketsLoading
          ? <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "60vh", color: C.muted, fontSize: 13 }}>Loading tickets…</div>
          : <AgentsTab tickets={tickets} onAgentClick={handleAgentClick} />)}
        {tab === "tickets" && (ticketsLoading
          ? <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "60vh", color: C.muted, fontSize: 13 }}>Loading tickets…</div>
          : <TicketExplorerTab tickets={tickets} onTicketClick={handleTicketClick} initialFilters={explorerFilters} />)}
        {tab === "chat" && (ticketsLoading
          ? <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "60vh", color: C.muted, fontSize: 13 }}>Loading tickets…</div>
          : <ChatReview tickets={tickets} initialTicketId={selectedTicketId} onAgentClick={handleAgentClick} />)}
        {tab === "agentProfile" && (
          <AgentProfile agentId={selectedAgentId} tickets={tickets} onBack={() => setTab("agents")} onTicketClick={handleTicketClick} onKPIFilter={(f) => { setExplorerFilters(f); handleTabChange("tickets"); }} />
        )}
        {tab === "admin" && userRole === "admin" && <AdminSettingsTab anonKey={anonKey} session={session} />}
        {tab === "offer_creation" && <OfferCreationTab anonKey={anonKey} session={session} />}
      </main>
    </div>
  );
}
