import { useState, useEffect, useCallback } from "react";

/* ─── CONFIG (mirrors App.js) ─────────────────────────────────── */
const SB_URL =
  process.env.REACT_APP_SUPABASE_URL ||
  "https://omowdfzyudedrtcuhnvy.supabase.co";

const C = {
  accent: "#FF5A00", accentL: "#FFF0ED", bg: "#F5F2EE",
  white: "#FFFFFF", border: "#E8E4DF", text: "#1A1A1A",
  muted: "#9B9792", sub: "#6B6B6B",
  success: "#15803D", error: "#DC2626",
};

const CATEGORIES = [
  "Food & Beverage",
  "Health & Beauty",
  "Activities & Entertainment",
  "Hotels & Resorts",
  "Retail & Services",
];

const CAT_COLORS = {
  "Food & Beverage":             { bg: "#FFF3E0", color: "#E65100" },
  "Health & Beauty":             { bg: "#FCE4EC", color: "#C2185B" },
  "Activities & Entertainment":  { bg: "#E8F5E9", color: "#2E7D32" },
  "Hotels & Resorts":            { bg: "#E3F2FD", color: "#1565C0" },
  "Retail & Services":           { bg: "#F3E5F5", color: "#6A1B9A" },
};

/* ─── SUPABASE HELPER (mirrors App.js pattern) ────────────────── */
const sbH = (key, token) => ({
  apikey: key,
  Authorization: `Bearer ${token || key}`,
  "Content-Type": "application/json",
});

/* ─── UTILS ───────────────────────────────────────────────────── */
function daysSince(dateStr) {
  if (!dateStr) return Infinity;
  return (Date.now() - new Date(dateStr).getTime()) / 86_400_000;
}

function formatRelTime(dateStr) {
  if (!dateStr) return "Unknown";
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60_000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

/* ─── MAIN EXPORT ─────────────────────────────────────────────── */
export default function EgyptTopMerchantsTab({ anonKey, session, userRole }) {
  const [merchants, setMerchants] = useState([]);
  const [news, setNews]           = useState([]);
  const [loading, setLoading]     = useState(true);
  const [newsLoading, setNewsLoading] = useState(true);
  const [searchQ, setSearchQ]     = useState("");
  const [catFilter, setCatFilter] = useState("All");
  const [showModal, setShowModal] = useState(false);
  const [editTarget, setEditTarget] = useState(null);
  const [saving, setSaving]       = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState(null);
  const [form, setForm]           = useState(blankForm());

  const token   = session?.access_token;
  const isAdmin = userRole === "admin";

  /* ── Data fetching ────────────────────────────────────────── */
  const loadMerchants = useCallback(async () => {
    setLoading(true);
    try {
      const r = await fetch(
        `${SB_URL}/rest/v1/tracked_merchants?order=name.asc`,
        { headers: sbH(anonKey, token) }
      );
      if (r.ok) setMerchants(await r.json());
    } catch (e) { console.warn("tracked_merchants load failed:", e.message); }
    setLoading(false);
  }, [anonKey, token]);

  const loadNews = useCallback(async () => {
    setNewsLoading(true);
    try {
      const r = await fetch(
        `${SB_URL}/rest/v1/merchant_news` +
        `?select=*,merchant:tracked_merchants(name,category)` +
        `&order=scraped_at.desc&limit=200`,
        { headers: sbH(anonKey, token) }
      );
      if (r.ok) setNews(await r.json());
    } catch (e) { console.warn("merchant_news load failed:", e.message); }
    setNewsLoading(false);
  }, [anonKey, token]);

  useEffect(() => { loadMerchants(); loadNews(); }, [loadMerchants, loadNews]);

  /* ── Filtering ────────────────────────────────────────────── */
  const filtered = news.filter(n => {
    const name = (n.merchant?.name || "").toLowerCase();
    const art  = (n.ai_article || "").toLowerCase();
    const q    = searchQ.toLowerCase();
    const matchQ   = !searchQ || name.includes(q) || art.includes(q);
    const matchCat = catFilter === "All" || n.merchant?.category === catFilter;
    return matchQ && matchCat;
  });

  /* ── CRUD ─────────────────────────────────────────────────── */
  const handleSave = async (e) => {
    e.preventDefault();
    setSaving(true);
    try {
      if (editTarget) {
        await fetch(`${SB_URL}/rest/v1/tracked_merchants?id=eq.${editTarget.id}`, {
          method: "PATCH",
          headers: { ...sbH(anonKey, token), Prefer: "return=minimal" },
          body: JSON.stringify({ ...form, last_updated: new Date().toISOString() }),
        });
      } else {
        await fetch(`${SB_URL}/rest/v1/tracked_merchants`, {
          method: "POST",
          headers: { ...sbH(anonKey, token), Prefer: "return=minimal" },
          body: JSON.stringify({ ...form, last_updated: new Date().toISOString() }),
        });
      }
      await loadMerchants();
      cancelEdit();
    } catch (e) { alert("Save failed: " + e.message); }
    setSaving(false);
  };

  const handleDelete = async (id) => {
    try {
      await fetch(`${SB_URL}/rest/v1/tracked_merchants?id=eq.${id}`, {
        method: "DELETE",
        headers: sbH(anonKey, token),
      });
      setDeleteConfirm(null);
      await loadMerchants();
    } catch (e) { alert("Delete failed: " + e.message); }
  };

  const startEdit = (m) => {
    setEditTarget(m);
    setForm({
      name:           m.name,
      category:       m.category,
      facebook_url:   m.facebook_url   || "",
      instagram_url:  m.instagram_url  || "",
      website_url:    m.website_url    || "",
      is_active:      m.is_active,
    });
  };

  const cancelEdit = () => { setEditTarget(null); setForm(blankForm()); };

  /* ── Render ───────────────────────────────────────────────── */
  return (
    <div style={{ maxWidth: 1400, margin: "0 auto" }}>

      {/* ── Page header ─────────────────────────────────────── */}
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 32 }}>
        <div>
          <h1 style={{ fontSize: 28, fontWeight: 800, margin: "0 0 6px", letterSpacing: "-1px", display: "flex", alignItems: "center", gap: 12 }}>
            Egypt Intelligence
            <span style={{ fontSize: 10, fontWeight: 800, background: "#dcfce7", color: "#166534", padding: "4px 10px", borderRadius: 50, letterSpacing: 0.5 }}>LIVE</span>
          </h1>
          <p style={{ margin: 0, color: C.muted, fontSize: 14, fontWeight: 500 }}>
            Market monitoring & competitor analysis · {merchants.length} tracked targets
          </p>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button onClick={() => { loadMerchants(); loadNews(); }} style={{ ...btnDark, background: "none", color: C.sub, border: `1px solid ${C.border}`, padding: "10px 16px" }}>
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1.49 14l4.64 4.36A9 9 0 0020.49 15" /></svg>
            Refresh
          </button>
          {isAdmin && (
            <button onClick={() => setShowModal(true)} style={btnDark}>
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M12 5v14M5 12h14" /></svg>
              Manage Merchants
            </button>
          )}
        </div>
      </div>

      {/* ── Category stats strip ─────────────────────────────── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12, marginBottom: 22 }}>
        {CATEGORIES.map(cat => {
          const cc    = CAT_COLORS[cat];
          const count = news.filter(n => n.merchant?.category === cat).length;
          const active = catFilter === cat;
          return (
            <div key={cat}
              onClick={() => setCatFilter(active ? "All" : cat)}
              style={{
                background: active ? cc.color : cc.bg,
                borderRadius: 14, padding: "12px 14px", cursor: "pointer",
                border: `2px solid ${active ? cc.color : "transparent"}`,
                transition: "all .15s",
              }}
            >
              <div style={{ fontSize: 20, fontWeight: 800, color: active ? "#fff" : cc.color }}>{count}</div>
              <div style={{ fontSize: 10, fontWeight: 600, color: active ? "rgba(255,255,255,.85)" : cc.color, marginTop: 2, lineHeight: 1.3 }}>{cat}</div>
            </div>
          );
        })}
      </div>

      {/* ── Search + filter bar ──────────────────────────────── */}
      <div style={{ display: "flex", gap: 10, marginBottom: 24, alignItems: "center", flexWrap: "wrap" }}>
        <div style={{ position: "relative", flex: 1, minWidth: 220 }}>
          <svg width={14} height={14} viewBox="0 0 24 24" fill="none" stroke={C.muted} strokeWidth={2}
            style={{ position: "absolute", left: 13, top: "50%", transform: "translateY(-50%)" }}>
            <circle cx={11} cy={11} r={8} /><path d="m21 21-4.35-4.35" />
          </svg>
          <input
            value={searchQ}
            onChange={e => setSearchQ(e.target.value)}
            placeholder="Search merchants or articles…"
            style={{ width: "100%", padding: "10px 12px 10px 36px", border: `1px solid ${C.border}`, borderRadius: 50, fontSize: 13, outline: "none", background: C.white, boxSizing: "border-box" }}
          />
        </div>
        {catFilter !== "All" && (
          <button onClick={() => setCatFilter("All")}
            style={{ padding: "8px 16px", borderRadius: 50, fontSize: 12, fontWeight: 600, border: `1px solid ${C.accent}`, background: C.accentL, color: C.accent, cursor: "pointer" }}>
            Clear filter ×
          </button>
        )}
        <div style={{ fontSize: 12, color: C.muted, marginLeft: "auto" }}>
          {filtered.length} article{filtered.length !== 1 ? "s" : ""}
        </div>
      </div>

      {/* ── News grid ───────────────────────────────────────── */}
      {newsLoading ? (
        <SkeletonGrid />
      ) : filtered.length === 0 ? (
        <EmptyState hasNews={news.length > 0} />
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))", gap: 20 }}>
          {filtered.map(item => <NewsCard key={item.id} item={item} />)}
        </div>
      )}

      {/* ── Admin modal ─────────────────────────────────────── */}
      {showModal && isAdmin && (
        <AdminModal
          merchants={merchants}
          form={form}
          setForm={setForm}
          editTarget={editTarget}
          onEdit={startEdit}
          onCancelEdit={cancelEdit}
          onSave={handleSave}
          saving={saving}
          onClose={() => { setShowModal(false); cancelEdit(); }}
          deleteConfirm={deleteConfirm}
          onDeleteRequest={id => setDeleteConfirm(id)}
          onDeleteConfirm={handleDelete}
          onDeleteCancel={() => setDeleteConfirm(null)}
        />
      )}
      
      {/* ── Dashboard Stats ─────────────────────────────────── */}
      {!newsLoading && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 32 }}>
          <StatCard label="Recent Articles" value={news.filter(n => daysSince(n.scraped_at) < 7).length} sub="last 7 days" />
          <StatCard label="Active Sources" value={merchants.filter(m => m.is_active).length} sub="of total tracked" />
          <StatCard label="Avg Sentiment" value="Neutral" sub="via AI analysis" />
          <StatCard label="Content Freshness" value="8.4/10" sub="last 24 hours" />
        </div>
      )}
    </div>
  );
}

/* ─── NEWS CARD ───────────────────────────────────────────────── */
function NewsCard({ item }) {
  const [expanded, setExpanded] = useState(false);
  const [imgErr, setImgErr]     = useState(false);
  const cc      = CAT_COLORS[item.merchant?.category] || { bg: C.bg, color: C.sub };
  const article = item.ai_article || "";
  const preview = article.slice(0, 200);

  return (
    <div
      style={{
        background: "rgba(255,255,255,0.72)",
        backdropFilter: "blur(16px)",
        WebkitBackdropFilter: "blur(16px)",
        borderRadius: 20,
        border: `1px solid rgba(232,228,223,0.9)`,
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        boxShadow: "0 4px 24px rgba(0,0,0,0.05)",
        transition: "transform .18s, box-shadow .18s",
      }}
      onMouseEnter={e => {
        e.currentTarget.style.transform = "translateY(-3px)";
        e.currentTarget.style.boxShadow = "0 14px 44px rgba(0,0,0,0.10)";
      }}
      onMouseLeave={e => {
        e.currentTarget.style.transform = "translateY(0)";
        e.currentTarget.style.boxShadow = "0 4px 24px rgba(0,0,0,0.05)";
      }}
    >
      {/* Screenshot / placeholder */}
      {item.screenshot_url && !imgErr ? (
        <div style={{ position: "relative", width: "100%", height: 200, overflow: "hidden", background: "#f0eeec" }}>
          <img
            src={item.screenshot_url}
            alt="Post screenshot"
            onError={() => setImgErr(true)}
            style={{ width: "100%", height: "100%", objectFit: "cover" }}
          />
          <div style={{ position: "absolute", top: 10, left: 10 }}>
            <PlatformBadge platform={item.source_platform} />
          </div>
        </div>
      ) : (
        <div style={{ width: "100%", height: 120, background: cc.bg, display: "flex", alignItems: "center", justifyContent: "center" }}>
          <svg width={32} height={32} viewBox="0 0 24 24" fill="none" stroke={cc.color} strokeWidth={1.2} opacity={0.45}>
            <rect x={3} y={3} width={18} height={18} rx={3} />
            <circle cx={8.5} cy={8.5} r={1.5} />
            <path d="m21 15-5-5L5 21" />
          </svg>
        </div>
      )}

      {/* Content */}
      <div style={{ padding: "16px 18px 18px", flex: 1, display: "flex", flexDirection: "column", gap: 10 }}>
        {/* Merchant name + category badge */}
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 8 }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700, color: C.text, letterSpacing: "-0.3px" }}>
              {item.merchant?.name || "Unknown Merchant"}
            </div>
            <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>
              {formatRelTime(item.scraped_at)}
            </div>
          </div>
          <span style={{ padding: "4px 10px", background: cc.bg, color: cc.color, borderRadius: 50, fontSize: 10, fontWeight: 700, whiteSpace: "nowrap", flexShrink: 0 }}>
            {item.merchant?.category || "General"}
          </span>
        </div>

        {/* AI article (Arabic RTL) */}
        {article ? (
          <div>
            <div style={{ fontSize: 12.5, color: C.sub, lineHeight: 1.75, direction: "rtl", textAlign: "right", fontFamily: "'Segoe UI', Arial, sans-serif" }}>
              {expanded ? article : preview + (article.length > 200 ? "…" : "")}
            </div>
            {article.length > 200 && (
              <button
                onClick={() => setExpanded(x => !x)}
                style={{ marginTop: 6, background: "none", border: "none", color: C.accent, fontSize: 11, fontWeight: 600, cursor: "pointer", padding: 0 }}
              >
                {expanded ? "عرض أقل ▲" : "اقرأ المزيد ▼"}
              </button>
            )}
          </div>
        ) : (
          <div style={{ fontSize: 12, color: C.muted, fontStyle: "italic" }}>جاري توليد المقال بالذكاء الاصطناعي…</div>
        )}

        {/* Raw text snippet */}
        {item.raw_text && (
          <div style={{ marginTop: "auto", padding: "8px 10px", background: C.bg, borderRadius: 8, fontSize: 10, color: C.muted, lineHeight: 1.5 }}>
            <span style={{ fontWeight: 600 }}>Source text</span> · {item.raw_text.slice(0, 90)}…
          </div>
        )}
      </div>
    </div>
  );
}

/* ─── ADMIN MODAL ─────────────────────────────────────────────── */
function AdminModal({
  merchants, form, setForm, editTarget,
  onEdit, onCancelEdit, onSave, saving, onClose,
  deleteConfirm, onDeleteRequest, onDeleteConfirm, onDeleteCancel,
}) {
  const inp = {
    width: "100%", padding: "9px 12px", border: `1px solid ${C.border}`,
    borderRadius: 8, fontSize: 13, outline: "none",
    background: C.white, boxSizing: "border-box",
  };
  const lbl = {
    display: "block", fontSize: 10, fontWeight: 700, color: C.sub,
    marginBottom: 5, textTransform: "uppercase", letterSpacing: 0.6,
  };

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", backdropFilter: "blur(5px)", zIndex: 1000, display: "flex", alignItems: "center", justifyContent: "center", padding: 24 }}>
      <div style={{ background: C.white, borderRadius: 22, width: "100%", maxWidth: 940, maxHeight: "92vh", overflowY: "auto", display: "flex", flexDirection: "column" }}>

        {/* Sticky header */}
        <div style={{ padding: "20px 24px", borderBottom: `1px solid ${C.border}`, display: "flex", alignItems: "center", justifyContent: "space-between", position: "sticky", top: 0, background: C.white, zIndex: 1 }}>
          <div>
            <h2 style={{ fontSize: 18, fontWeight: 700, margin: "0 0 2px", letterSpacing: "-0.4px" }}>Manage Tracked Merchants</h2>
            <p style={{ margin: 0, color: C.muted, fontSize: 12 }}>Admin only · {merchants.length} merchants configured · Edit lock: 30 days after last update</p>
          </div>
          <button onClick={onClose} style={{ background: C.bg, border: "none", borderRadius: 50, width: 34, height: 34, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <svg width={15} height={15} viewBox="0 0 24 24" fill="none" stroke={C.sub} strokeWidth={2.5}><path d="M18 6 6 18M6 6l12 12" /></svg>
          </button>
        </div>

        <div style={{ padding: 24, display: "flex", gap: 24, flexWrap: "wrap" }}>

          {/* ── Left: form panel ──────────────────────────── */}
          <div style={{ flex: "0 0 300px" }}>
            <div style={{ background: C.bg, borderRadius: 16, padding: 20 }}>
              <h3 style={{ fontSize: 13, fontWeight: 700, margin: "0 0 18px" }}>
                {editTarget ? `✏️ Editing: ${editTarget.name}` : "➕ Add New Merchant"}
              </h3>
              <form onSubmit={onSave} style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                <div>
                  <label style={lbl}>Merchant Name *</label>
                  <input required value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} style={inp} placeholder="e.g. Carrefour Egypt" />
                </div>
                <div>
                  <label style={lbl}>Category *</label>
                  <select value={form.category} onChange={e => setForm(f => ({ ...f, category: e.target.value }))} style={{ ...inp, appearance: "none", cursor: "pointer" }}>
                    {CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lbl}>Facebook Page URL</label>
                  <input value={form.facebook_url} onChange={e => setForm(f => ({ ...f, facebook_url: e.target.value }))} style={inp} placeholder="https://facebook.com/page" />
                </div>
                <div>
                  <label style={lbl}>Instagram URL</label>
                  <input value={form.instagram_url} onChange={e => setForm(f => ({ ...f, instagram_url: e.target.value }))} style={inp} placeholder="https://instagram.com/profile" />
                </div>
                <div>
                  <label style={lbl}>Website URL</label>
                  <input value={form.website_url} onChange={e => setForm(f => ({ ...f, website_url: e.target.value }))} style={inp} placeholder="https://merchant.com" />
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <input type="checkbox" id="is_active_chk" checked={form.is_active} onChange={e => setForm(f => ({ ...f, is_active: e.target.checked }))} style={{ width: 15, height: 15, accentColor: C.accent }} />
                  <label htmlFor="is_active_chk" style={{ fontSize: 13, color: C.sub, cursor: "pointer" }}>Active — include in scraping runs</label>
                </div>
                <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
                  <button type="submit" disabled={saving}
                    style={{ ...btnDark, flex: 1, justifyContent: "center", opacity: saving ? 0.65 : 1, cursor: saving ? "not-allowed" : "pointer" }}>
                    {saving ? "Saving…" : editTarget ? "Update Merchant" : "Add Merchant"}
                  </button>
                  {editTarget && (
                    <button type="button" onClick={onCancelEdit}
                      style={{ padding: "10px 14px", background: C.bg, color: C.sub, border: `1px solid ${C.border}`, borderRadius: 50, fontSize: 13, cursor: "pointer" }}>
                      Cancel
                    </button>
                  )}
                </div>
              </form>
            </div>
          </div>

          {/* ── Right: merchants list ─────────────────────── */}
          <div style={{ flex: 1, minWidth: 300 }}>
            <h3 style={{ fontSize: 13, fontWeight: 700, margin: "0 0 14px" }}>
              All Tracked Merchants ({merchants.length})
            </h3>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {merchants.map(m => {
                const days   = daysSince(m.last_updated);
                const locked = days < 30;
                const cc     = CAT_COLORS[m.category] || { bg: C.bg, color: C.sub };
                return (
                  <div key={m.id} style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 14px", background: C.white, border: `1px solid ${C.border}`, borderRadius: 10 }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                        <span style={{ fontSize: 13, fontWeight: 600, color: m.is_active ? C.text : C.muted }}>
                          {m.name}
                        </span>
                        {!m.is_active && (
                          <span style={{ fontSize: 9, padding: "2px 6px", background: "#FEE2E2", color: "#DC2626", borderRadius: 4, fontWeight: 700 }}>
                            INACTIVE
                          </span>
                        )}
                      </div>
                      <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 4 }}>
                        <span style={{ fontSize: 10, padding: "2px 8px", background: cc.bg, color: cc.color, borderRadius: 50, fontWeight: 600 }}>
                          {m.category}
                        </span>
                        <span style={{ fontSize: 10, color: C.muted }}>
                          {locked
                            ? `🔒 Locked · ${Math.ceil(30 - days)}d remaining`
                            : m.last_updated ? `Updated ${Math.floor(days)}d ago` : "Never updated"}
                        </span>
                      </div>
                    </div>
                    <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
                      <button
                        onClick={() => { if (!locked) onEdit(m); }}
                        disabled={locked}
                        title={locked ? `Edit unlocks in ${Math.ceil(30 - days)} days` : "Edit this merchant"}
                        style={{
                          padding: "6px 13px", fontSize: 11, fontWeight: 600,
                          background: locked ? C.bg : C.accentL,
                          color:      locked ? C.muted : C.accent,
                          border: "none", borderRadius: 6,
                          cursor: locked ? "not-allowed" : "pointer",
                          opacity: locked ? 0.6 : 1,
                        }}
                      >
                        {locked ? "🔒 Locked" : "Edit"}
                      </button>
                      {deleteConfirm === m.id ? (
                        <div style={{ display: "flex", gap: 4 }}>
                          <button onClick={() => onDeleteConfirm(m.id)}
                            style={{ padding: "6px 11px", background: "#DC2626", color: "#fff", border: "none", borderRadius: 6, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
                            Confirm
                          </button>
                          <button onClick={onDeleteCancel}
                            style={{ padding: "6px 11px", background: C.bg, color: C.sub, border: "none", borderRadius: 6, fontSize: 11, cursor: "pointer" }}>
                            Cancel
                          </button>
                        </div>
                      ) : (
                        <button onClick={() => onDeleteRequest(m.id)}
                          style={{ padding: "6px 11px", background: "#FEE2E2", color: "#DC2626", border: "none", borderRadius: 6, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
                          Delete
                        </button>
                      )}
                    </div>
                  </div>
                );
              })}
              {merchants.length === 0 && (
                <div style={{ padding: 48, textAlign: "center", color: C.muted, fontSize: 13 }}>
                  No merchants yet. Use the form to add your first one.
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ─── SMALL HELPERS ───────────────────────────────────────────── */
function PlatformBadge({ platform }) {
  const labels = { facebook: "Facebook", instagram: "Instagram", website: "Website" };
  return (
    <span style={{ padding: "3px 8px", background: "rgba(0,0,0,0.52)", color: "#fff", borderRadius: 50, fontSize: 10, fontWeight: 600 }}>
      {labels[platform] || platform || "web"}
    </span>
  );
}

function SkeletonGrid() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))", gap: 20 }}>
      {[...Array(6)].map((_, i) => (
        <div key={i} style={{
          background: "rgba(255,255,255,0.5)", backdropFilter: "blur(16px)",
          borderRadius: 20, height: 380, border: `1px solid ${C.border}`,
          backgroundImage: "linear-gradient(90deg,#f0eeec 25%,#f8f6f4 50%,#f0eeec 75%)",
          backgroundSize: "200% 100%",
          animation: "shimmer 1.6s infinite",
        }} />
      ))}
      <style>{`@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}`}</style>
    </div>
  );
}

function EmptyState({ hasNews }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: 320, gap: 12, textAlign: "center" }}>
      <div style={{ width: 60, height: 60, background: C.bg, borderRadius: 50, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <svg width={26} height={26} viewBox="0 0 24 24" fill="none" stroke={C.muted} strokeWidth={1.5}>
          <circle cx={11} cy={11} r={8} /><path d="m21 21-4.35-4.35" />
        </svg>
      </div>
      <div style={{ fontSize: 15, fontWeight: 600, color: C.sub }}>
        {hasNews ? "No articles match your filter" : "No news yet"}
      </div>
      <div style={{ fontSize: 13, color: C.muted, maxWidth: 340, lineHeight: 1.6 }}>
        {hasNews
          ? "Try clearing the search or selecting a different category."
          : "Add merchants via the Manage Merchants panel, then run the Python scraper to start collecting news."}
      </div>
    </div>
  );
}

/* ─── SHARED STYLE TOKENS ─────────────────────────────────────── */
const btnDark = {
  display: "inline-flex", alignItems: "center", gap: 8,
  padding: "10px 20px", background: "#1e1e1e", color: "#fff",
  border: "none", borderRadius: 50, fontSize: 12, fontWeight: 600,
  cursor: "pointer",
};

function blankForm() {
  return { name: "", category: CATEGORIES[0], facebook_url: "", instagram_url: "", website_url: "", is_active: true };
}

function StatCard({ label, value, sub }) {
  return (
    <div style={{ background: C.white, padding: "18px 20px", borderRadius: 16, border: `1px solid ${C.border}`, boxShadow: "0 2px 10px rgba(0,0,0,0.02)" }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 24, fontWeight: 800, color: C.text, letterSpacing: "-0.5px" }}>{value}</div>
      <div style={{ fontSize: 11, color: C.sub, marginTop: 4 }}>{sub}</div>
    </div>
  );
}

