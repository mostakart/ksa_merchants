import { useState, useCallback, useEffect } from "react";

// ─── Constants ────────────────────────────────────────────────────────────────

const OLLAMA_URL = "http://localhost:11434/api/generate";

const MODELS = [
    { id: "qwen2.5:7b", label: "Qwen 2.5 (7B)" },
    { id: "gemma3:4b", label: "Gemma 4 (4B)" },
];

// ─── Prompt Engineering ───────────────────────────────────────────────────────

const buildSystemPrompt = () => `
You are an elite Business Development AI for a SaaS company operating in Saudi Arabia (KSA).
Your task: analyze merchant customer reviews and return a structured JSON merchant profile.

STRICT RULES:
1. Respond ONLY with a single valid JSON object — no markdown, no explanation, no code fences.
2. All Arabic text must be Modern Standard Arabic (فصحى) unless a field specifies dialect.
3. Every field is REQUIRED. Never omit or null any key.
4. Scores must be integers between 1 and 10 inclusive.

JSON SCHEMA (return exactly these keys):
{
  "sentiment_score": <integer 1-10, overall customer sentiment>,
  "top_praise": "<single most praised aspect, in Arabic>",
  "top_complaint": "<single most critical complaint, in Arabic>",
  "service_highlights": ["<top dish/activity/item mentioned positively, Arabic>", ...],
  "bd_priority_score": <integer 1-10, how urgently our BD team should target this merchant>,
  "arabic_sales_pitch": "<full persuasive B2B sales script in Arabic, 3-5 sentences, addressing the merchant's pain points, highlighting our platform's ROI, and ending with a clear call-to-action>"
}

SCORING GUIDE for bd_priority_score:
- 8-10: High complaint volume + strong brand = high growth potential with our platform
- 5-7:  Moderate engagement, worth a follow-up call
- 1-4:  Stable but low urgency
`.trim();

const buildUserPrompt = (merchantName, reviews) => `
Merchant Name: ${merchantName}
Customer Reviews:
${reviews}

Analyze the above reviews and return the JSON profile now.
`.trim();

// ─── Ollama API Layer ─────────────────────────────────────────────────────────

async function fetchOllamaProfile({ model, merchantName, reviews, onChunk }) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 120_000); // 2-min timeout

    try {
        const response = await fetch(OLLAMA_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            signal: controller.signal,
            body: JSON.stringify({
                model,
                system: buildSystemPrompt(),
                prompt: buildUserPrompt(merchantName, reviews),
                format: "json",   // Forces Ollama to constrain output to valid JSON
                stream: true,
                options: {
                    temperature: 0.2,   // Low temp = deterministic, structured output
                    top_p: 0.9,
                    num_predict: 1024,
                },
            }),
        });

        if (!response.ok) {
            const err = await response.text();
            throw new Error(`Ollama HTTP ${response.status}: ${err}`);
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let accumulated = "";

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            const lines = decoder.decode(value, { stream: true }).split("\n");
            for (const line of lines) {
                if (!line.trim()) continue;
                try {
                    const chunk = JSON.parse(line);
                    if (chunk.response) {
                        accumulated += chunk.response;
                        onChunk?.(accumulated);
                    }
                    if (chunk.done) break;
                } catch {
                    // Partial JSON line — skip
                }
            }
        }

        // Parse final accumulated JSON
        return JSON.parse(accumulated);
    } catch (err) {
        if (err.name === "AbortError") {
            throw new Error("Request timed out after 2 minutes. Is the model loaded?");
        }
        if (err.message.includes("fetch")) {
            throw new Error(
                "Cannot reach Ollama at localhost:11434. Run `ollama serve` in your terminal."
            );
        }
        throw err;
    } finally {
        clearTimeout(timeoutId);
    }
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function ModelToggle({ models, activeModel, onChange, disabled }) {
    return (
        <div style={styles.toggleGroup}>
            {models.map((m) => (
                <button
                    key={m.id}
                    onClick={() => onChange(m.id)}
                    disabled={disabled}
                    style={{
                        ...styles.toggleBtn,
                        ...(activeModel === m.id ? styles.toggleBtnActive : {}),
                    }}
                >
                    {m.label}
                </button>
            ))}
        </div>
    );
}

function ProfileCard({ result, model, label }) {
    if (!result) return null;

    const scoreColor = (n) =>
        n >= 7 ? "#16a34a" : n >= 4 ? "#d97706" : "#dc2626";

    return (
        <div style={styles.card}>
            <h3 style={styles.cardTitle}>{label}</h3>
            <code style={styles.modelBadge}>{model}</code>

            <div style={styles.scoreRow}>
                <ScoreBadge label="Sentiment" value={result.sentiment_score} color={scoreColor(result.sentiment_score)} />
                <ScoreBadge label="BD Priority" value={result.bd_priority_score} color={scoreColor(result.bd_priority_score)} />
            </div>

            <Field label="Top Praise 👍" value={result.top_praise} arabic />
            <Field label="Top Complaint ⚠️" value={result.top_complaint} arabic />

            <div style={styles.field}>
                <span style={styles.fieldLabel}>Service/Menu Highlights 🌟</span>
                <div style={styles.pills}>
                    {result.service_highlights?.map((item, i) => (
                        <span key={i} style={styles.pill}>{item}</span>
                    ))}
                </div>
            </div>

            <div style={styles.pitchBox}>
                <span style={styles.fieldLabel}>Arabic Sales Pitch 📞</span>
                <p style={styles.pitchText}>{result.arabic_sales_pitch}</p>
            </div>
        </div>
    );
}

function ScoreBadge({ label, value, color }) {
    return (
        <div style={styles.scoreBadge}>
            <span style={{ ...styles.scoreNum, color }}>{value}/10</span>
            <span style={styles.scoreLabel}>{label}</span>
        </div>
    );
}

function Field({ label, value, arabic }) {
    return (
        <div style={styles.field}>
            <span style={styles.fieldLabel}>{label}</span>
            <span style={arabic ? styles.arabicText : {}}>{value}</span>
        </div>
    );
}

function CompareView({ results, loading, streamText }) {
    return (
        <div style={styles.compareGrid}>
            {MODELS.map((m) => (
                <div key={m.id} style={styles.compareCol}>
                    {loading[m.id] ? (
                        <div style={styles.loadingBox}>
                            <div style={styles.spinner} />
                            <pre style={styles.streamPreview}>{streamText[m.id] || "Generating…"}</pre>
                        </div>
                    ) : (
                        <ProfileCard result={results[m.id]} model={m.id} label={m.label} />
                    )}
                </div>
            ))}
        </div>
    );
}

// ─── Main Component ───────────────────────────────────────────────────────────

export default function MerchantProfiler({ initialMerchant, embedded }) {
    const [activeModel, setActiveModel] = useState(MODELS[0].id);
    const [compareMode, setCompareMode] = useState(false);
    const [merchantName, setMerchantName] = useState(initialMerchant ? initialMerchant.Merchant : "");
    const [reviews, setReviews] = useState(initialMerchant ? (initialMerchant.Reviews3 || "") : "");
    const [results, setResults] = useState({});
    const [loading, setLoading] = useState({});
    const [streamText, setStreamText] = useState({});
    const [error, setError] = useState(null);

    useEffect(() => {
        if (initialMerchant) {
            setMerchantName(initialMerchant.Merchant || "");
            setReviews(initialMerchant.Reviews3 || "");
            setResults({});
            setStreamText({});
            setError(null);
        }
    }, [initialMerchant]);

    const runSingle = useCallback(async (model) => {
        setLoading((p) => ({ ...p, [model]: true }));
        setStreamText((p) => ({ ...p, [model]: "" }));
        setError(null);

        try {
            const profile = await fetchOllamaProfile({
                model,
                merchantName,
                reviews,
                onChunk: (text) =>
                    setStreamText((p) => ({ ...p, [model]: text })),
            });
            setResults((p) => ({ ...p, [model]: profile }));
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading((p) => ({ ...p, [model]: false }));
        }
    }, [merchantName, reviews]);

    const handleAnalyze = useCallback(async () => {
        if (!merchantName.trim() || !reviews.trim()) {
            setError("Please enter both a merchant name and reviews.");
            return;
        }

        if (compareMode) {
            // Fire both models in parallel
            await Promise.allSettled(MODELS.map((m) => runSingle(m.id)));
        } else {
            await runSingle(activeModel);
        }
    }, [compareMode, activeModel, runSingle, merchantName, reviews]);

    const isAnyLoading = Object.values(loading).some(Boolean);

    return (
        <div style={embedded ? { width: "100%" } : styles.root}>
            {!embedded && (
                <>
                    <h2 style={styles.title}>AI Merchant Profiler</h2>
                    <p style={styles.subtitle}>Powered by local Ollama — no cloud APIs</p>
                </>
            )}

            {/* Model Controls */}
            <div style={styles.controls}>
                <ModelToggle
                    models={MODELS}
                    activeModel={activeModel}
                    onChange={setActiveModel}
                    disabled={compareMode || isAnyLoading}
                />
                <label style={styles.compareToggle}>
                    <input
                        type="checkbox"
                        checked={compareMode}
                        onChange={(e) => setCompareMode(e.target.checked)}
                        disabled={isAnyLoading}
                    />
                    &nbsp; Side-by-side compare
                </label>
            </div>

            {/* Inputs */}
            <div style={styles.inputGroup}>
                <input
                    style={styles.input}
                    placeholder="Merchant name (e.g. مطعم الأصيل)"
                    value={merchantName}
                    onChange={(e) => setMerchantName(e.target.value)}
                />
                <textarea
                    style={styles.textarea}
                    placeholder="Paste customer reviews here (Arabic or English)…"
                    rows={6}
                    value={reviews}
                    onChange={(e) => setReviews(e.target.value)}
                />
            </div>

            <button
                style={{ ...styles.analyzeBtn, opacity: isAnyLoading ? 0.6 : 1 }}
                onClick={handleAnalyze}
                disabled={isAnyLoading}
            >
                {isAnyLoading ? "Analyzing…" : compareMode ? "Compare Both Models" : "Analyze Merchant"}
            </button>

            {/* Error Banner */}
            {error && (
                <div style={styles.errorBanner}>
                    <strong>Error:</strong> {error}
                </div>
            )}

            {/* Results */}
            {compareMode ? (
                <CompareView results={results} loading={loading} streamText={streamText} />
            ) : (
                <>
                    {loading[activeModel] && (
                        <div style={styles.loadingBox}>
                            <div style={styles.spinner} />
                            <pre style={styles.streamPreview}>{streamText[activeModel] || "Waiting for model…"}</pre>
                        </div>
                    )}
                    <ProfileCard
                        result={results[activeModel]}
                        model={activeModel}
                        label={MODELS.find((m) => m.id === activeModel)?.label}
                    />
                </>
            )}
        </div>
    );
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const styles = {
    root: { maxWidth: 1200, margin: "0 auto", padding: 24, fontFamily: "system-ui, sans-serif" },
    title: { fontSize: 24, fontWeight: 700, margin: 0 },
    subtitle: { color: "#6b7280", marginBottom: 20, fontSize: 13 },
    controls: { display: "flex", alignItems: "center", gap: 16, marginBottom: 16, flexWrap: "wrap" },
    toggleGroup: { display: "flex", border: "1px solid #d1d5db", borderRadius: 8, overflow: "hidden" },
    toggleBtn: { padding: "8px 16px", border: "none", background: "#f9fafb", cursor: "pointer", fontSize: 13, color: "#374151" },
    toggleBtnActive: { background: "#1d4ed8", color: "#fff", fontWeight: 600 },
    compareToggle: { fontSize: 13, color: "#374151", cursor: "pointer", display: "flex", alignItems: "center" },
    inputGroup: { display: "flex", flexDirection: "column", gap: 10, marginBottom: 16 },
    input: { padding: "10px 14px", border: "1px solid #d1d5db", borderRadius: 8, fontSize: 14 },
    textarea: { padding: "10px 14px", border: "1px solid #d1d5db", borderRadius: 8, fontSize: 14, resize: "vertical" },
    analyzeBtn: { padding: "12px 28px", background: "#1d4ed8", color: "#fff", border: "none", borderRadius: 8, fontSize: 15, fontWeight: 600, cursor: "pointer" },
    errorBanner: { marginTop: 16, padding: "12px 16px", background: "#fef2f2", border: "1px solid #fca5a5", borderRadius: 8, color: "#b91c1c", fontSize: 13 },
    compareGrid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginTop: 24 },
    compareCol: { minWidth: 0 },
    card: { border: "1px solid #e5e7eb", borderRadius: 12, padding: 20, marginTop: 24, background: "#fff" },
    cardTitle: { margin: "0 0 4px", fontSize: 16, fontWeight: 700 },
    modelBadge: { fontSize: 11, background: "#f3f4f6", padding: "2px 8px", borderRadius: 4, color: "#6b7280" },
    scoreRow: { display: "flex", gap: 16, margin: "16px 0" },
    scoreBadge: { display: "flex", flexDirection: "column", alignItems: "center", background: "#f9fafb", borderRadius: 8, padding: "10px 20px" },
    scoreNum: { fontSize: 22, fontWeight: 700 },
    scoreLabel: { fontSize: 11, color: "#6b7280", marginTop: 2 },
    field: { marginBottom: 12 },
    fieldLabel: { display: "block", fontSize: 11, fontWeight: 600, color: "#6b7280", textTransform: "uppercase", marginBottom: 4, letterSpacing: "0.05em" },
    arabicText: { direction: "rtl", display: "block", fontFamily: "Tahoma, Arial, sans-serif", fontSize: 15 },
    pills: { display: "flex", flexWrap: "wrap", gap: 6, direction: "rtl" },
    pill: { background: "#dbeafe", color: "#1d4ed8", padding: "4px 10px", borderRadius: 20, fontSize: 13 },
    pitchBox: { background: "#f0fdf4", border: "1px solid #bbf7d0", borderRadius: 8, padding: 14, marginTop: 8 },
    pitchText: { direction: "rtl", fontFamily: "Tahoma, Arial, sans-serif", fontSize: 14, lineHeight: 1.8, margin: 0, color: "#166534" },
    loadingBox: { marginTop: 24, padding: 20, border: "1px dashed #d1d5db", borderRadius: 12, textAlign: "center" },
    streamPreview: { textAlign: "left", fontSize: 11, color: "#6b7280", maxHeight: 120, overflow: "hidden", marginTop: 12, direction: "ltr" },
    spinner: { width: 28, height: 28, border: "3px solid #e5e7eb", borderTopColor: "#1d4ed8", borderRadius: "50%", animation: "spin 0.8s linear infinite", margin: "0 auto" },
};