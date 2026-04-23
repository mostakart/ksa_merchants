# CRM Support Intelligence: Deep Integration Plan

Based on the latest requirements, we will integrate the full spectrum of AI-detected parameters from the `ticket_analysis` table into the existing dashboard structure. This will create a unified view where support data and merchant performance are cross-referenced.

## 1. Comprehensive Data Mapping
We will map and visualize all key parameters from the `ticket_analysis` table, grouped into logical categories:

| Category | Parameters to Integrate |
| :--- | :--- |
| **Product & UX** | `p_issue_type`, `p_payment_blocker`, `p_ux_friction_point`, `p_missing_feature`, `p_root_cause_owner` |
| **Customer Sentiment** | `s_initial_sentiment`, `s_final_sentiment`, `s_sentiment_shift`, `s_churn_intent`, `s_customer_effort_score` |
| **Agent Quality** | `a_empathy_score`, `a_policy_compliance`, `a_knowledge_accuracy`, `a_overall_score`, `a_evaluation_notes` |
| **Business Impact** | `fin_ticket_monetary_value`, `p_refund_requested`, `f_fraud_suspicion` |

---

## 2. "Attached" Merchant Intelligence
We will link ticket analysis directly to individual merchants across the dashboard.

### A. Merchant Profiler Integration
- **Support History Section**: When viewing a merchant, a new section will display their recent tickets with AI sentiment.
- **Top Issues**: List the most frequent `p_merchant_issue_type` for that specific merchant.
- **Risk Score**: Incorporate `s_churn_intent` and `p_refund_requested` into the merchant's overall BD priority.

### B. Global Analytics
- **Financial Dashboard**: Sum of `fin_ticket_monetary_value` grouped by `p_merchant_name` or `p_issue_type`.
- **Root Cause Map**: A high-level chart showing who is responsible for most frictions (Merchant, Ops, or Tech).

---

## 3. Advanced Filtering System
Upgrade the **Ticket Explorer** with a "Power Filter" bar to slice data by all new parameters:
- Filter for **Fraud Suspicion** (`f_fraud_suspicion`).
- Filter for **Payment Blockers** (`p_payment_blocker`).
- Filter for **High Churn Risk** (`s_churn_intent`).

---

## Proposed Changes

### [KSA Dashboard Component]

#### [MODIFY] [App.js](file:///Users/mostafakhaled/Downloads/ksa_pipeline%202/ksa-dashboard/src/App.js)
- **Data Fetch**: Update `loadTickets` to use a left join: `zoho_tickets?select=*,analysis:ticket_analysis(*)`.
- **Normalization**: Update `normTicket` to include all 30+ analysis fields.
- **Support Overview**: Add new "Quality Metrics" grid (Empathy, Accuracy, Compliance).
- **Global Search**: Allow searching tickets by `p_smart_tags`.

#### [MODIFY] [MerchantProfiler.jsx](file:///Users/mostafakhaled/Downloads/ksa_pipeline%202/ksa-dashboard/src/MerchantProfiler.jsx)
- **Support Module**: Add a new sub-component to show merchant-specific ticket trends and AI sentiment history.

## User Review Required

> [!IMPORTANT]
> **Merchant Name Matching**: The analysis table uses `p_merchant_name`. We will use our existing `canonicalMap` (fuzzy matching logic) to ensure tickets are correctly linked even if there are slight spelling variations in the CRM.

> [!WARNING]
> **Data Types**: Several columns (like `p_payment_blocker`) were converted to `TEXT` in the SQL setup to handle "None" values. We will treat these as "Yes/No/Unknown" in the UI.

## Verification Plan

### Manual Verification
1.  **Join Test**: Check `tickets[0].analysis` in the console to ensure all fields are arriving correctly.
2.  **Cross-Reference Test**: Open a merchant in the Profiler and verify their specific tickets appear in the new "Support" section.
3.  **Financial Test**: Verify the "Total Monetary Value" chart matches the sum of `fin_ticket_monetary_value` in Supabase.
