import pandas as pd

try:
    analysis_df = pd.read_csv('ticket_analysis_rows 04.csv')
    tickets_df = pd.read_csv('tickets_rows.csv')
    
    # Merge on ticket_number
    merged = pd.merge(analysis_df, tickets_df, on='ticket_number', how='inner')
    
    agent_df = merged[merged['assignee'] == 'Yehia Adel']
    print(f"Tickets for Yehia Adel: {len(agent_df)}")
    
    if 'a_escalated' in agent_df.columns:
        escalated = agent_df['a_escalated'].astype(str).str.lower().eq('true').sum()
        print(f"ESCALATED: {escalated}")
        
    if 'p_issue_type' in agent_df.columns:
        top_reason = agent_df['p_issue_type'].value_counts().index[0] if not agent_df['p_issue_type'].empty else "N/A"
        print(f"Top reason (p_issue_type): {top_reason}")
        
except Exception as e:
    print(f"Error: {e}")

