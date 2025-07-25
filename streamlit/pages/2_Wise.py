# streamlit/pages/2_Wise.py
import streamlit as st
import pandas as pd
import altair as alt
import sys
import os

# Add parent directory to path to import S3Helper
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from aws.connect_to_s3 import S3Helper

st.set_page_config(
    page_title="Wise Dashboard",
    page_icon="💳",
    layout="wide",
)

@st.cache_data(ttl=600)
def load_latest_staging_data(_s3_helper, base_path, file_prefix):
    """Loads the most recent staging file."""
    try:
        files = _s3_helper.list_files(prefix=f"{base_path}/staging/{file_prefix}")
        if not files:
            st.warning(f"No staging data found with prefix '{file_prefix}'. Please run the Wise pipeline.")
            return None
        
        latest_file = sorted(files)[-1]
        st.info(f"Loading data from: `{latest_file}`")
        df = _s3_helper.read_csv_from_s3(latest_file)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return None

def create_net_change_chart(df):
    """Creates a bar chart of daily net change."""
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('net_change:Q', title='Net Change (£)'),
        color=alt.condition(
            alt.datum.net_change > 0,
            alt.value("mediumseagreen"),
            alt.value("indianred")
        ),
        tooltip=[
            alt.Tooltip('date:T', title='Date'),
            alt.Tooltip('net_change:Q', title='Net Change', format=',.2f'),
            alt.Tooltip('closing_balance:Q', title='Closing Balance', format=',.2f')
        ]
    ).interactive().properties(
        title="Daily Net Change",
        width=400,
        height=300
    )
    return chart

def create_balance_chart(df):
    """Creates a line chart of daily closing balance."""
    chart = alt.Chart(df).mark_line(point=True, color='steelblue').encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('closing_balance:Q', title='Balance (£)', scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip('date:T', title='Date'),
            alt.Tooltip('closing_balance:Q', title='Closing Balance', format=',.2f'),
            alt.Tooltip('net_change:Q', title='Net Change', format=',.2f')
        ]
    ).interactive().properties(
        title="Daily Closing Balance",
        width=400,
        height=300
    )
    return chart

def create_calendar_heatmap(df):
    """Creates a calendar heatmap for cash flow analysis."""
    df_heatmap = df.copy()
    df_heatmap['day'] = df_heatmap['date'].dt.day
    df_heatmap['month'] = df_heatmap['date'].dt.strftime('%Y-%m')
    df_heatmap['weekday'] = df_heatmap['date'].dt.day_name()
    
    chart = alt.Chart(df_heatmap).mark_rect().encode(
        x=alt.X('day:O', title='Day of Month'),
        y=alt.Y('month:O', title='Month'),
        color=alt.Color(
            'net_change:Q',
            title='Net Change (£)',
            scale=alt.Scale(scheme='redblue', domain=[-df_heatmap['net_change'].abs().max(), df_heatmap['net_change'].abs().max()])
        ),
        tooltip=[
            alt.Tooltip('date:T', title='Date'),
            alt.Tooltip('net_change:Q', title='Net Change', format=',.2f'),
            alt.Tooltip('closing_balance:Q', title='Closing Balance', format=',.2f')
        ]
    ).properties(
        title="Cash Flow Calendar Heatmap",
        width=600,
        height=200
    )
    return chart

def main():
    """Main Streamlit application for the Wise Dashboard."""
    st.title("💳 Wise Banking Dashboard")
    st.markdown("An overview of your Wise account balance and transaction patterns.")

    try:
        from configuration.secrets import ENVIRONMENT
    except ImportError:
        st.error("Secrets file not found. Please ensure `configuration/secrets.py` is set up.")
        st.stop()
    
    s3_helper = S3Helper()
    base_path = f"{ENVIRONMENT}/bank-statements/wise-gbp"
    
    # Load daily data
    daily_df = load_latest_staging_data(s3_helper, base_path, "wise_balance_daily_")
    
    if daily_df is None:
        st.warning("No data could be loaded. Please run the Wise pipeline.")
        st.stop()

    # Key Metrics
    st.header("Key Metrics")
    latest_balance = daily_df['closing_balance'].iloc[-1]
    total_deposits = daily_df['deposits'].sum()
    total_withdrawals = daily_df['withdrawals'].sum()
    total_fees = daily_df['fees'].sum()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current Balance", f"£{latest_balance:,.2f}")
    col2.metric("Total Deposits", f"£{total_deposits:,.2f}")
    col3.metric("Total Withdrawals", f"£{total_withdrawals:,.2f}")
    col4.metric("Total Fees", f"£{total_fees:,.2f}")

    # Charts
    st.header("Balance Analysis")
    col1, col2 = st.columns(2)
    
    with col1:
        st.altair_chart(create_net_change_chart(daily_df), use_container_width=True)
    
    with col2:
        st.altair_chart(create_balance_chart(daily_df), use_container_width=True)

    # Cash Flow Analysis
    st.header("Cash Flow Analysis")
    st.altair_chart(create_calendar_heatmap(daily_df), use_container_width=True)

    # Data Tables
    st.header("Data Tables")
    
    with st.expander("View Daily Balance Data"):
        st.dataframe(daily_df)

if __name__ == "__main__":
    main() 