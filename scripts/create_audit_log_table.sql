-- Waffarha Nexus: Audit Logging Table
-- Tracks all user activity for compliance, analytics, and debugging

CREATE TABLE IF NOT EXISTS nexus_audit_log (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
  user_email TEXT NOT NULL,
  session_id TEXT,
  event_type TEXT NOT NULL,
  resource_type TEXT,
  resource_id TEXT,
  resource_name TEXT,
  old_value TEXT,
  new_value TEXT,
  metadata JSONB DEFAULT '{}',
  ip_address INET,
  user_agent TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Indexes for fast queries
CREATE INDEX idx_nexus_audit_user_id ON nexus_audit_log(user_id);
CREATE INDEX idx_nexus_audit_user_email ON nexus_audit_log(user_email);
CREATE INDEX idx_nexus_audit_event_type ON nexus_audit_log(event_type);
CREATE INDEX idx_nexus_audit_created_at ON nexus_audit_log(created_at DESC);
CREATE INDEX idx_nexus_audit_resource ON nexus_audit_log(resource_type, resource_id);

-- Enable RLS
ALTER TABLE nexus_audit_log ENABLE ROW LEVEL SECURITY;

-- Policies
CREATE POLICY "Users can insert audit events" ON nexus_audit_log
  FOR INSERT TO authenticated WITH CHECK (true);

CREATE POLICY "Users can read own audit logs" ON nexus_audit_log
  FOR SELECT TO authenticated USING (auth.uid() = user_id);

CREATE POLICY "Admins can read all audit logs" ON nexus_audit_log
  FOR SELECT TO authenticated USING (auth.jwt() ->> 'role' = 'admin');

-- Grant permissions
GRANT INSERT ON nexus_audit_log TO authenticated;
GRANT SELECT ON nexus_audit_log TO authenticated;
