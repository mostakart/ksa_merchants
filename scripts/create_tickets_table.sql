-- ═══════════════════════════════════════════════════════
-- Waffarha CRM Portal — Zoho Desk Tickets Table
-- Run this in Supabase → SQL Editor before uploading data
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS zoho_tickets (
  id                         SERIAL PRIMARY KEY,
  ticket_id                  TEXT UNIQUE,
  subject                    TEXT,
  status                     TEXT,
  channel                    TEXT,
  priority                   TEXT,
  reason                     TEXT,
  sub_reason                 TEXT,
  ticket_owner               TEXT,
  created_time               TIMESTAMPTZ,
  closed_time                TIMESTAMPTZ,
  happiness_rating           TEXT,
  resolution_time_ms         BIGINT,
  first_response_time_ms     BIGINT,
  total_response_time_ms     BIGINT,
  num_threads                INTEGER,
  num_responses              INTEGER,
  num_reassign               INTEGER,
  num_reopen                 INTEGER,
  is_overdue                 BOOLEAN,
  is_escalated               BOOLEAN,
  escalation_validity        TEXT,
  sla_violation_type         TEXT,
  sla_name                   TEXT,
  team_id                    TEXT,
  tags                       TEXT,
  total_time_spent           INTEGER,
  merchant_name              TEXT,
  branch_name                TEXT,
  country                    TEXT,
  language                   TEXT,
  user_id                    TEXT,
  order_id                   TEXT,
  transaction_id             TEXT,
  created_at                 TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast filtering on common columns
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_channel      ON zoho_tickets (channel);
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_reason       ON zoho_tickets (reason);
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_status       ON zoho_tickets (status);
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_owner        ON zoho_tickets (ticket_owner);
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_created_time ON zoho_tickets (created_time);
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_happiness    ON zoho_tickets (happiness_rating);
CREATE INDEX IF NOT EXISTS idx_zoho_tickets_sla          ON zoho_tickets (sla_violation_type);

-- RLS: allow authenticated reads, service-role writes
ALTER TABLE zoho_tickets ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can read tickets"
  ON zoho_tickets FOR SELECT
  TO authenticated
  USING (true);

-- Verify
SELECT COUNT(*) FROM zoho_tickets;
