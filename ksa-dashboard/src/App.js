import { useState, useEffect, useMemo, useRef } from "react";
import { pipeline, env } from "@xenova/transformers";
import MerchantProfiler from "./MerchantProfiler";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, CartesianGrid,
} from "recharts";

env.allowLocalModels = false;

/* ─── CONFIG ─────────────────────────────────────────────────── */
const SB_URL = "https://omowdfzyudedrtcuhnvy.supabase.co";
const CITIES = ["riyadh", "jeddah", "dammam", "khobar", "mecca", "medina"];
const C = {
  accent: "#FF5A00", accentL: "#FFF0ED", bg: "#F5F2EE",
  white: "#FFFFFF", border: "#E8E4DF", text: "#1A1A1A",
  muted: "#9B9792", sub: "#6B6B6B",
};

// --- Branding ---
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

async function sbFetch(table, key, token) {
  let allRows = [];
  let offset = 0;
  const limit = 1000;
  while (true) {
    const r = await fetch(
      `${SB_URL}/rest/v1/${table}?select=*&limit=${limit}&offset=${offset}`,
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
    Branches: parseInt(m.branches_ksa) || 0,
    Phone: m.phone || "",
    Website: m.website || "",
    Reviews3: m.top_reviews || "",
    Category: m.category || "Uncategorized",
    OpeningHours: hRaw.toLowerCase() === "none" || hRaw.toLowerCase() === "null" || hRaw.toLowerCase() === "nan" ? "" : hRaw,
    HoursCategory: hCat,
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
    r[city] = {
      total: d.total, high: d.high, medium: d.medium, low: d.low,
      avgRating: d.rN ? +(d.rSum / d.rN).toFixed(2) : 0, malls: d.malls.size
    };
  }
  return r;
}

// eslint-disable-next-line no-unused-vars
function price_to_range(level) {
  return {0:"10–20 SAR",1:"20–40 SAR",2:"40–80 SAR",3:"80–150 SAR",4:"150+ SAR"}[level] || "";
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
function KPI({ label, value, sub, color }) {
  return (
    <div style={{ background: C.white, borderRadius: 10, padding: "14px 16px", border: `1px solid ${C.border}` }}>
      <div style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: .5 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700, color: color || C.text, letterSpacing: "-.5px", margin: "4px 0 2px" }}>{value}</div>
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
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  async function handleLogin() {
    if (!email || !password) return setErr("Both fields required");
    setLoading(true); setErr("");
    try {
      const session = await sbLogin(anonKey, email, password);
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

        {[["Email", "email", email, setEmail, "bd@waffarha.com"], ["Password", "password", password, setPassword, "••••••••"]].map(([l, t, v, fn, ph]) => (
          <div key={l} style={{ marginBottom: 14 }}>
            <label style={{ display: "block", fontSize: 11, fontWeight: 500, color: C.sub, marginBottom: 6, textTransform: "uppercase", letterSpacing: .5 }}>{l}</label>
            <input type={t} value={v} onChange={e => fn(e.target.value)} placeholder={ph}
              onKeyDown={e => e.key === "Enter" && handleLogin()}
              style={{ width: "100%", padding: "10px 12px", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 13, outline: "none", color: C.text, boxSizing: "border-box" }}
            />
          </div>
        ))}

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
function LoadingScreen({ city }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh", alignItems: "center", justifyContent: "center", background: C.bg, gap: 14 }}>
      <div style={{ display: "flex", gap: 7 }}>
        {[0, 150, 300].map(d => (
          <div key={d} style={{ width: 10, height: 10, borderRadius: "50%", background: C.accent, animation: "bounce .7s infinite alternate", animationDelay: `${d}ms` }} />
        ))}
      </div>
      <div style={{ fontSize: 13, color: C.muted }}>
        Loading <strong style={{ color: C.text }}>{city}</strong> merchants…
      </div>
      <style>{`@keyframes bounce { to { transform:translateY(-8px);opacity:.3; } }`}</style>
    </div>
  );
}

/* ─── MACRO TAB ──────────────────────────────────────────────── */
function MacroTab({ merchants }) {
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
        <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-.4px" }}>KSA Market Intelligence</div>
        <div style={{ fontSize: 13, color: C.muted, marginTop: 3 }}>
          {Object.keys(cityStats).length} cities · {mallsN} malls · {total.toLocaleString()} merchants
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 10, marginBottom: 18 }}>
        <KPI label="Total Merchants" value={total.toLocaleString()} sub="Full KSA coverage" />
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
          <ResponsiveContainer width="100%" height={180}>
            <PieChart>
              <Pie data={priceTiers} cx="50%" cy="50%" outerRadius={72} dataKey="value"
                label={({ name, percent }) => `${(percent * 100).toFixed(0)}%`} labelLine={false}>
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

/* ─── PROFILER TAB ───────────────────────────────────────────── */
function ProfilerTab({ merchants, anonKey, initialMerchant }) {
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
    for (const m of merchants) {
      map[m.Merchant] = (map[m.Merchant] || 0) + Math.max(1, m.Branches || 0);
    }
    return map;
  }, [merchants]);

  const filtered = useMemo(() => {
    if (!query) return [];
    const map = new Map();

    if (semanticNames && semanticNames.length > 0) {
      for (const name of semanticNames) {
        const found = merchants.find(m => m.Merchant === name || m.OriginalMerchant === name);
        if (found && !map.has(found.Merchant)) {
          map.set(found.Merchant, { ...found, TotalKsaBranches: merchantBranchesMap[found.Merchant] });
        }
      }
      return Array.from(map.values());
    }

    const q = query.toLowerCase();
    const qNorm = normArabic(q);

    // 1. Exact Matches
    for (const m of merchants) {
      if (m.Merchant.toLowerCase().includes(q) || m.City.toLowerCase().includes(q) || m.Mall.toLowerCase().includes(q) || normArabic(m.Merchant).includes(qNorm)) {
        if (!map.has(m.Merchant)) {
          map.set(m.Merchant, { ...m, TotalKsaBranches: merchantBranchesMap[m.Merchant] });
        }
      }
    }

    // 2. Fuzzy Fallback (if no exact matches found)
    if (map.size === 0 && qNorm.length >= 3) {
      const fuzzy = [];
      const seen = new Set();
      for (const m of merchants) {
        if (seen.has(m.Merchant)) continue;
        const mNorm = normArabic(m.Merchant);
        const dist = editDist(qNorm, mNorm);
        if (dist <= 3) {
          fuzzy.push({ m, dist });
          seen.add(m.Merchant);
        }
      }
      fuzzy.sort((a, b) => a.dist - b.dist);
      for (const item of fuzzy.slice(0, 15)) {
        map.set(item.m.Merchant, { ...item.m, TotalKsaBranches: merchantBranchesMap[item.m.Merchant] });
      }
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
                <span style={{ fontSize: 10, fontWeight: 500, color: m.Priority.toLowerCase().includes("high") ? C.accent : m.Priority.toLowerCase().includes("medium") ? "#D97706" : m.Priority.toLowerCase().includes("low") ? "#16A34A" : C.muted }}>
                  ★ {m.Rating}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {selected ? (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1.2fr", gap: 16 }}>
          <Card>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
              <div style={{ textAlign: "right", flex: 1 }}>
                <div style={{ fontSize: 16, fontWeight: 700, direction: "rtl", lineHeight: 1.4 }}>{selected.Merchant}</div>
                <div style={{ fontSize: 11, color: C.muted, marginTop: 3, direction: "rtl" }}>{selected.Mall} · {selected.City}</div>
              </div>
              <span style={{
                marginLeft: 10, padding: "3px 9px", borderRadius: 5, fontSize: 10, fontWeight: 500, flexShrink: 0,
                background: selected.Priority.toLowerCase().includes("high") ? C.accentL : selected.Priority.toLowerCase().includes("medium") ? "#FEF3C7" : selected.Priority.toLowerCase().includes("low") ? "#F0FDF4" : "#F4F2EE",
                color: selected.Priority.toLowerCase().includes("high") ? C.accent : selected.Priority.toLowerCase().includes("medium") ? "#D97706" : selected.Priority.toLowerCase().includes("low") ? "#16A34A" : C.muted
              }}>
                {selected.Priority}
              </span>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 12 }}>
              {[["Rating", `${selected.Rating} ★`], ["Reviews", selected.Reviews >= 1000 ? `${(selected.Reviews / 1000).toFixed(1)}k` : selected.Reviews], ["City Br.", selected.Branches || 1], ["KSA Br.", selected.TotalKsaBranches]].map(([l, v]) => (
                <div key={l} style={{ background: "#F9F8F7", borderRadius: 7, padding: "8px 6px", textAlign: "center" }}>
                  <div style={{ fontSize: 16, fontWeight: 700 }}>{v}</div>
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
            <MerchantProfiler initialMerchant={selected} embedded={true} />
          </Card>
        </div>
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
function PipelineTab({ merchants, onMerchantClick }) {
  const [city, setCity] = useState("All");
  const [mall, setMall] = useState("All");
  const [prio, setPrio] = useState("All");
  const [cat, setCat] = useState("All");
  const [price, setPrice] = useState("All");
  const [hours, setHours] = useState("All");
  const [statuses, setStatuses] = useState({});
  const [page, setPage] = useState(1);
  const pageSize = 100;

  const allCities = useMemo(() => ["All", ...new Set(merchants.map(m => m.City).filter(Boolean))].sort(), [merchants]);
  
  // Mall list depends on selected city
  const allMalls = useMemo(() => {
    const list = city === "All" ? merchants : merchants.filter(m => m.City === city);
    return ["All", ...new Set(list.map(m => m.Mall).filter(Boolean))].sort();
  }, [merchants, city]);

  const allCategories = useMemo(() => ["All", ...new Set(merchants.map(m => m.Category).filter(c => c && c !== "Uncategorized"))].sort(), [merchants]);
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
    if (price !== "All" && m.AvgPrice !== price) return false;
    if (hours !== "All" && m.HoursCategory !== hours) return false;
    return true;
  }), [merchants, city, mall, prio, cat, price, hours]);

  const totalPages = Math.ceil(filteredMerchants.length / pageSize);
  useEffect(() => { setPage(1); }, [city, prio, cat, price, hours]);
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
        <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-.4px" }}>BD Pipeline</div>
        <div style={{ fontSize: 13, color: C.muted, marginTop: 3 }}>Track outreach across {merchants.length.toLocaleString()} merchants</div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 10, marginBottom: 16 }}>
        <KPI label="Total Matches" value={filteredMerchants.length.toLocaleString()} sub="Based on filters" />
        <KPI label="High" value={pCounts.h.toLocaleString()} sub="Priority" color={C.accent} />
        <KPI label="Medium" value={pCounts.m.toLocaleString()} sub="Priority" color="#D97706" />
        <KPI label="Low" value={pCounts.l.toLocaleString()} sub="Priority" color="#16A34A" />
        <KPI label="Contacted" value={contacted} sub="This session" color="#1D4ED8" />
        <KPI label="Closed Deals" value={closed} sub="This session" color="#15803D" />
      </div>

      <div style={{ display: "flex", gap: 14, marginBottom: 20, alignItems: "flex-end", flexWrap: "wrap" }}>
        {[
          { label: "City", icon: "M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z", opts: allCities, val: city, fn: (v) => { setCity(v); setMall("All"); } },
          { label: "Mall", icon: "M6 2L3 6v14a2 2 0 002 2h14a2 2 0 002-2V6l-3-4z", opts: allMalls, val: mall, fn: setMall, placeholder: "Select Mall" },
          { label: "Priority", icon: "M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z", opts: ["All", "High", "Medium", "Low", "Uncategorized"], val: prio, fn: setPrio },
          { label: "Category", icon: "M20.59 13.41l-7.17 7.17a2 2 0 01-2.83 0L2 12V2h10l8.59 8.59a2 2 0 010 2.82z", opts: allCategories, val: cat, fn: setCat },
          { label: "Price", icon: "M12 1v22M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6", opts: allPrices, val: price, fn: setPrice },
          { label: "Hours", icon: "M12 22a10 10 0 100-20 10 10 0 000 20z M12 6v6l4 2", opts: allHours, val: hours, fn: setHours },
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
                <option key={i} value={o}>
                  {o === "All" ? (placeholder || `${label}: All`) : (o.length > 25 ? o.substring(0, 25) + "..." : o)}
                </option>
              ))}
            </select>
          </div>
        ))}

        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <button onClick={() => { setCity("All"); setMall("All"); setPrio("All"); setCat("All"); setPrice("All"); setHours("All"); }}
            style={{ padding: "8px 14px", background: "#F4F2EE", border: "none", borderRadius: 8, fontSize: 12, color: C.text, cursor: "pointer", fontWeight: 600, transition: "background .2s", height: 35 }}>
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
                const key = m.Merchant + m.Mall;
                const st = statuses[key] || "Uncontacted";
                return (
                  <tr key={i} style={{ borderBottom: `1px solid #F4F2EE` }}>
                    <td style={{ padding: "9px 12px", fontWeight: 500, maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", direction: "rtl" }}>
                      <span onClick={() => onMerchantClick(m)} style={{ cursor: "pointer", color: C.accent, textDecoration: "underline" }} title="View AI Profile">
                        {m.Merchant}
                      </span>
                    </td>
                    <td style={{ padding: "9px 12px" }}><span style={{ background: "#EFF6FF", color: "#1D4ED8", borderRadius: 4, padding: "2px 7px", fontSize: 10 }}>{m.City}</span></td>
                    <td style={{ padding: "9px 12px", fontSize: 11, color: C.muted, maxWidth: 110, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.Mall}</td>
                    <td style={{ padding: "9px 12px", fontSize: 11, color: C.sub, maxWidth: 110, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.Category === "Uncategorized" ? "—" : m.Category}</td>
                    <td style={{ padding: "9px 12px", fontWeight: 500, fontSize: 11, color: m.Priority.toLowerCase().includes("high") ? C.accent : m.Priority.toLowerCase().includes("medium") ? "#D97706" : m.Priority.toLowerCase().includes("low") ? "#16A34A" : C.muted }}>{m.Priority}</td>
                    <td style={{ padding: "9px 12px", color: "#D97706" }}>★ {m.Rating}</td>
                    <td style={{ padding: "9px 12px" }}>{m.Reviews >= 1000 ? `${(m.Reviews / 1000).toFixed(1)}k` : m.Reviews}</td>
                    <td style={{ padding: "9px 12px", color: C.muted }}>{m.AvgPrice || "—"}</td>
                    <td style={{ padding: "9px 12px", color: C.muted, fontSize: 11, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={m.OpeningHours}>{m.OpeningHours || "—"}</td>
                    <td style={{ padding: "9px 12px" }}>
                      <select value={st} onChange={e => setStatuses(prev => ({ ...prev, [key]: e.target.value }))}
                        style={{ padding: "3px 7px", borderRadius: 5, fontSize: 10, fontWeight: 500, border: "none", cursor: "pointer", outline: "none", ...statusStyles[st] }}>
                        {Object.keys(statusStyles).map(s => <option key={s}>{s}</option>)}
                      </select>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "12px 16px", background: "#F9F8F7", borderTop: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 11, color: C.muted }}>
            Showing {filteredMerchants.length === 0 ? 0 : ((page - 1) * pageSize) + 1} to {Math.min(page * pageSize, filteredMerchants.length)} of {filteredMerchants.length} entries
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
              style={{ padding: "6px 12px", background: page === 1 ? "#F4F2EE" : C.white, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 11, color: page === 1 ? C.muted : C.text, cursor: page === 1 ? "not-allowed" : "pointer" }}>
              Previous
            </button>
            <div style={{ padding: "0 10px", fontSize: 11, fontWeight: 500, color: C.text }}>
              Page {page} of {totalPages || 1}
            </div>
            <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages || totalPages === 0}
              style={{ padding: "6px 12px", background: page >= totalPages ? "#F4F2EE" : C.white, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 11, color: page >= totalPages ? C.muted : C.text, cursor: page >= totalPages ? "not-allowed" : "pointer" }}>
              Next
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ─── MAIN APP ───────────────────────────────────────────────── */
export default function App() {
  const [anonKey, setAnonKey] = useState(null);
  const [session, setSession] = useState(null);
  const [merchants, setMerchants] = useState([]);
  const [loadingCity, setLoadingCity] = useState("");
  const [tab, setTab] = useState("macro");
  const [selectedMerchantForProfile, setSelectedMerchantForProfile] = useState(null);

  const handleMerchantClick = (merchant) => {
    setSelectedMerchantForProfile(merchant);
    setTab("profiler");
  };

  useEffect(() => {
    if (session && anonKey) loadAllMerchants();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session]);

  async function loadAllMerchants() {
    const all = [];
    for (const city of CITIES) {
      setLoadingCity(city);
      try {
        const rows = await sbFetch(`merchants_${city}`, anonKey, session.access_token);
        if (Array.isArray(rows)) all.push(...rows.map(norm));
      } catch (e) { console.warn(`Skip ${city}:`, e.message); }
    }
    setMerchants(all);
    setLoadingCity("");
  }

  const TABS = [
    { id: "macro", label: "Market Overview", d: "M4 15l4-8 4 4 4-6 4 6" },
    { id: "profiler", label: "Merchant Profiler", d: "M12 12c2.7 0 5-2.3 5-5s-2.3-5-5-5-5 2.3-5 5 2.3 5 5 5zm0 2c-3.3 0-10 1.7-10 5v1h20v-1c0-3.3-6.7-5-10-5z" },
    { id: "pipeline", label: "BD Pipeline", d: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" },
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
        if (dist <= threshold && n1[0] === n2[0]) {
          mapToCan[other] = name;
          processed.add(other);
        }
      }
    }
    return mapToCan;
  }, [merchants]);

  const unifiedMerchants = useMemo(() => {
    return merchants.map(m => ({
      ...m,
      OriginalMerchant: m.Merchant,
      Merchant: canonicalMap[m.Merchant] || m.Merchant
    }));
  }, [merchants, canonicalMap]);

  const sidebarStats = useMemo(() => [
    ["Total", unifiedMerchants.length.toLocaleString()],
    ["Cities", new Set(unifiedMerchants.map(m => m.City)).size],
    ["Avg ★", (unifiedMerchants.reduce((s, m) => s + (m.Rating || 0), 0) / (unifiedMerchants.filter(m => m.Rating > 0).length || 1)).toFixed(2)],
    ["High", unifiedMerchants.filter(m => m.Priority.toLowerCase().includes("high")).length.toLocaleString()],
  ], [unifiedMerchants]);

  if (!anonKey) return <SetupScreen onSetup={setAnonKey} />;
  if (!session) return <LoginScreen anonKey={anonKey} onLogin={setSession} />;
  if (loadingCity) return <LoadingScreen city={loadingCity} />;

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif", background: C.bg, color: C.text, overflow: "hidden" }}>
      {/* Sidebar */}
      <div style={{ width: 216, background: C.white, borderRight: `1px solid ${C.border}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
        <div style={{ padding: "20px 16px 14px", borderBottom: `1px solid ${C.border}` }}>
          <WaffarhaLogo height={26} style={{ marginBottom: 6 }} />
          <div style={{ fontSize: 11, color: C.muted, fontWeight: 500 }}>KSA Merchant Intelligence</div>
        </div>

        <nav style={{ padding: "10px 8px", flex: 1 }}>
          {TABS.map(({ id, label, d }) => (
            <div key={id} onClick={() => setTab(id)} style={{ display: "flex", alignItems: "center", gap: 9, padding: "9px 10px", borderRadius: 8, cursor: "pointer", fontSize: 13, color: tab === id ? C.accent : C.sub, background: tab === id ? C.accentL : "transparent", fontWeight: tab === id ? 500 : 400, marginBottom: 2, transition: "all .12s" }}>
              <svg width={15} height={15} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>
              {label}
            </div>
          ))}
        </nav>

        <div style={{ padding: "12px 16px", borderTop: `1px solid ${C.border}`, fontSize: 11, color: C.muted }}>
          {sidebarStats.map(([l, v]) => (
            <div key={l} style={{ display: "flex", justifyContent: "space-between", padding: "2px 0" }}>
              <span>{l}</span><span style={{ color: C.text, fontWeight: 500 }}>{v}</span>
            </div>
          ))}
          <button onClick={() => { setSession(null); setMerchants([]); }}
            style={{ marginTop: 10, width: "100%", padding: "6px 0", background: "#F4F2EE", border: "none", borderRadius: 6, fontSize: 11, color: C.muted, cursor: "pointer" }}>
            Sign Out
          </button>
        </div>
      </div>

      {/* Main */}
      <main style={{ flex: 1, overflowY: "auto", padding: 24 }}>
        {tab === "macro" && <MacroTab merchants={unifiedMerchants} />}
        {tab === "profiler" && <ProfilerTab merchants={unifiedMerchants} anonKey={anonKey} initialMerchant={selectedMerchantForProfile} />}
        {tab === "pipeline" && <PipelineTab merchants={unifiedMerchants} onMerchantClick={handleMerchantClick} />}
      </main>
    </div>
  );
}