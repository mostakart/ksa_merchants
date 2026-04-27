import pandas as pd

try:
    df = pd.read_csv('../ticket_analysis_rows 04.csv')
    print("Unique s_final_sentiment:", df['s_final_sentiment'].unique())
    print("Unique s_initial_sentiment:", df['s_initial_sentiment'].unique())
    print("Unique s_customer_effort_score:", df['s_customer_effort_score'].unique())
    print("Unique a_one_touch_resolution:", df['a_one_touch_resolution'].unique())
    
    # Check if there are transfer messages in tickets_rows.csv
    tickets_df = pd.read_csv('../tickets_rows.csv')
    transfer_count = tickets_df['message'].str.contains('accepted the transfer request', na=False, case=False).sum()
    print("Transfer messages count in tickets_rows.csv:", transfer_count)
    
except Exception as e:
    print(f"Error: {e}")

