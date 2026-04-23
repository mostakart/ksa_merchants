-- ═══════════════════════════════════════════════════════
-- CRM API Setup: Final Tables & Authorization
-- ═══════════════════════════════════════════════════════

-- 1. Create crm_api_keys table
CREATE TABLE IF NOT EXISTS public.crm_api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_name TEXT NOT NULL,
    key_hash TEXT UNIQUE NOT NULL,
    owner_email TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

-- 2. Update/Create zoho_tickets table
-- Note: Simplified structure based on tickets_rows 02.txt
CREATE TABLE IF NOT EXISTS public.zoho_tickets (
    id BIGINT PRIMARY KEY, -- Use BIGINT for numeric IDs from CSV
    ticket_number TEXT UNIQUE NOT NULL,
    ticket_id TEXT UNIQUE, -- Keep for backward compatibility
    channel TEXT,
    status TEXT,
    department_id TEXT,
    assignee TEXT,
    customer_email TEXT,
    customer_phone TEXT,
    tags TEXT,
    message_direction TEXT,
    sender_name TEXT,
    message TEXT,
    has_attachments BOOLEAN,
    ticket_time TIMESTAMPTZ,
    merchant_name TEXT, -- From original schema if needed
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Create expanded ticket_analysis table
CREATE TABLE IF NOT EXISTS public.ticket_analysis (
    id BIGINT PRIMARY KEY, -- Use BIGINT to match numeric IDs in CSV
    ticket_number TEXT REFERENCES public.zoho_tickets(ticket_number) ON DELETE CASCADE,
    
    -- Exact headers from CSV for direct import
    p_issue_type TEXT,
    p_merchant_name TEXT,
    p_merchant_issue_type TEXT,
    p_payment_blocker TEXT,    -- Changed from BOOLEAN to TEXT to handle "None"
    p_refund_requested TEXT,   -- Changed from BOOLEAN to TEXT to handle "None"
    p_ux_friction_point TEXT,
    p_missing_feature TEXT,
    p_root_cause_owner TEXT,
    p_smart_tags TEXT,
    mer_branch_name TEXT,
    m_promo_code_used TEXT,
    fin_ticket_monetary_value FLOAT,
    c_misleading_wording_exact TEXT,
    f_fraud_suspicion TEXT,    -- Changed from BOOLEAN to TEXT
    cs_escalation_department TEXT,
    s_initial_sentiment TEXT,
    s_final_sentiment TEXT,
    s_sentiment_shift TEXT,
    s_churn_intent TEXT,       -- Changed from BOOLEAN to TEXT
    s_customer_effort_score INT,
    s_profanity_detected TEXT,  -- Changed from BOOLEAN to TEXT
    s_gratitude_detected TEXT,  -- Changed from BOOLEAN to TEXT
    s_sentiment_summary TEXT,
    a_empathy_score INT,
    a_policy_compliance TEXT,   -- Changed from BOOLEAN to TEXT
    a_grammar_professionalism INT,
    a_knowledge_accuracy INT,
    a_is_template_heavy TEXT,   -- Changed from BOOLEAN to TEXT
    a_one_touch_resolution TEXT, -- Changed from BOOLEAN to TEXT
    a_escalated TEXT,           -- Changed from BOOLEAN to TEXT
    a_overall_score FLOAT,
    a_evaluation_notes TEXT,
    ai_status TEXT,
    ai_model_used TEXT,
    analyzed_at TIMESTAMPTZ DEFAULT NOW(),
    embedding VECTOR(1536), 
    failed_reason TEXT,
    
    UNIQUE(ticket_number)
);

-- Index for performance
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_number ON public.zoho_tickets(ticket_number);
CREATE INDEX IF NOT EXISTS idx_ticket_analysis_ticket_number ON public.ticket_analysis(ticket_number);
CREATE INDEX IF NOT EXISTS idx_ticket_analysis_issue_type ON public.ticket_analysis(p_issue_type);
CREATE INDEX IF NOT EXISTS idx_ticket_analysis_sentiment ON public.ticket_analysis(s_final_sentiment);

-- 4. Enable RLS
ALTER TABLE public.zoho_tickets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ticket_analysis ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.crm_api_keys ENABLE ROW LEVEL SECURITY;

-- 5. RLS Policies
DROP POLICY IF EXISTS "Dashboard: Read Tickets" ON public.zoho_tickets;
CREATE POLICY "Dashboard: Read Tickets" ON public.zoho_tickets
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "Dashboard: Read Analysis" ON public.ticket_analysis;
CREATE POLICY "Dashboard: Read Analysis" ON public.ticket_analysis
    FOR SELECT TO authenticated USING (true);

-- 6. Helper for API Key Validation
CREATE OR REPLACE FUNCTION check_crm_api_key(p_key TEXT) 
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM public.crm_api_keys 
        WHERE key_hash = p_key AND is_active = TRUE
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
