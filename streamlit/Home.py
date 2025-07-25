import streamlit as st

st.set_page_config(
    page_title="FinTracker V2",
    page_icon="💼",
    layout="wide",
)

def main():
    """Main landing page for FinTracker V2."""
    st.title("💼 FinTracker V2")
    st.markdown("Welcome to your personal finance tracking dashboard.")
    
    st.markdown("---")
    
    # Overview
    st.header("Overview")
    st.markdown("""
    FinTracker V2 is your comprehensive personal finance tracking system that processes and visualizes data from multiple sources:
    
    - **Banking Data:** Track your Wise account balance and transaction patterns
    - **Pension Data:** Monitor your pension performance and investment growth
    """)
    
    # Navigation Guide
    st.header("Navigation")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💳 Wise Dashboard")
        st.markdown("""
        View your Wise banking data including:
        - Current account balance
        - Daily transaction patterns
        - Cash flow analysis with calendar heatmap
        - Balance trends over time
        """)
        st.info("Navigate to the **Wise** page in the sidebar to view your banking dashboard.")
    
    with col2:
        st.subheader("💰 Pensions Dashboard")
        st.markdown("""
        Monitor your pension performance including:
        - Investment growth tracking
        - Gain/loss analysis
        - Performance comparison (Wahed vs Standard Life)
        - Cash invested vs pension value trends
        """)
        st.info("Navigate to the **Pensions** page in the sidebar to view your pension dashboard.")
    
    st.markdown("---")
    
    # Data Pipeline Status
    st.header("Data Pipeline Information")
    st.markdown("""
    ### How to Update Your Data
    
    To refresh the data displayed in the dashboards, run the respective pipeline scripts:
    
    **For Wise Data:**
    ```bash
    python wise/run_pipeline.py
    ```
    
    **For Pensions Data:**
    ```bash
    python pensions/run_pipeline.py
    ```
    
    These scripts will:
    1. Extract the latest data from your sources
    2. Clean and process the data
    3. Generate dashboard-ready staging tables
    4. Store everything in your S3 bucket
    """)
    
    # System Information
    with st.expander("System Information"):
        try:
            from configuration.secrets import ENVIRONMENT
            st.success(f"Environment: `{ENVIRONMENT}`")
        except ImportError:
            st.error("Configuration not found. Please ensure `configuration/secrets.py` is set up.")
        
        st.markdown("""
        **Data Sources:**
        - **Wise:** CSV statements uploaded to S3
        - **Pensions:** Google Sheets via API
        
        **Storage:** AWS S3 with organized folder structure for raw, cleansed, and staging data
        """)

if __name__ == "__main__":
    main()
