import streamlit as st
import pandas as pd
import altair as alt
import sys
import os

# Add parent directory to path to import S3Helper
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aws.connect_to_s3 import S3Helper

st.set_page_config(
    page_title="FinTracker Dashboard",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=600)
def load_latest_staging_data(_s3_helper, base_path, file_prefix):
    """Loads the most recent staging file from S3."""
    try:
        files = _s3_helper.list_files(prefix=f"{base_path}/staging/{file_prefix}")
        if not files:
            st.error(
                f"No data found for '{file_prefix}'. Please run the staging script."
            )
            return None

        latest_file = sorted(files)[-1]
        st.info(f"Loading data from: `{latest_file}`")
        df = _s3_helper.read_csv_from_s3(latest_file)
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        st.error(f"Failed to load data from S3: {e}")
        return None


def main():
    """Main Streamlit application."""
    st.title("Wise Account Dashboard")
    st.markdown("A simple visual overview of your Wise account balance.")

    try:
        from configuration.secrets import ENVIRONMENT
    except ImportError:
        st.error(
            "Secrets file not found. Please ensure `configuration/secrets.py` is set up."
        )
        st.stop()

    s3_helper = S3Helper()
    base_path = f"{ENVIRONMENT}/bank-statements/wise-gbp"

    # --- Load Data ---
    daily_df = load_latest_staging_data(s3_helper, base_path, "wise_balance_daily_")

    if daily_df is None:
        st.info(
            "No data to display. Please run the staging script to generate daily balance data."
        )
        st.stop()

    # --- Key Metrics ---
    st.header("Key Metrics")
    latest_balance = daily_df["closing_balance"].iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Balance", f"£{latest_balance:,.2f}")

    # --- Daily Net Change Chart ---
    st.header("Daily Net Change")
    net_change_chart = (
        alt.Chart(daily_df)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("net_change:Q", title="Net Change (£)"),
            color=alt.condition(
                alt.datum.net_change > 0,
                alt.value("mediumseagreen"),  # Positive change
                alt.value("indianred"),  # Negative change
            ),
            tooltip=["date:T", "net_change:Q", "deposits:Q", "withdrawals:Q", "fees:Q"],
        )
        .interactive()
        .properties(title="Daily Net Change in Balance")
    )
    st.altair_chart(net_change_chart, use_container_width=True)

    # --- Balance Trends ---
    st.header("Daily Balance Trend")
    daily_chart = (
        alt.Chart(daily_df)
        .mark_line(point=alt.OverlayMarkDef(color="cyan"))
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(
                "closing_balance:Q",
                title="Closing Balance (£)",
                scale=alt.Scale(zero=False),
            ),
            tooltip=["date:T", "closing_balance:Q"],
        )
        .interactive()
        .properties(title="Daily Closing Balance Over Time")
    )
    st.altair_chart(daily_chart, use_container_width=True)

    # --- Daily Cash Flow Calendar Heatmap ---
    st.header("Daily Cash Flow Calendar Heatmap")

    if not daily_df.empty:
        # Create a full date range to ensure every day is represented
        date_range = pd.date_range(
            start=daily_df["date"].min(), end=daily_df["date"].max(), freq="D"
        )
        all_days = pd.DataFrame({"date": date_range})
        heatmap_df = pd.merge(all_days, daily_df, on="date", how="left").fillna(0)

        # Add date parts for the heatmap layout
        heatmap_df["year"] = heatmap_df["date"].dt.year
        heatmap_df["day_of_week"] = heatmap_df["date"].dt.strftime(
            "%a"
        )  # Mon, Tue, etc.
        heatmap_df["week_of_year"] = heatmap_df["date"].dt.isocalendar().week

        # Create the heatmap
        calendar_heatmap = (
            alt.Chart(heatmap_df)
            .mark_rect()
            .encode(
                x=alt.X(
                    "week_of_year:O", title="Week of Year", axis=alt.Axis(labelAngle=0)
                ),
                y=alt.Y(
                    "day_of_week:O",
                    title="Day of Week",
                    sort=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
                ),
                color=alt.Color(
                    "net_change:Q",
                    scale=alt.Scale(scheme="redblue", domainMid=0),
                    legend=alt.Legend(title="Net Change (£)"),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("net_change:Q", title="Net Change", format=".2f"),
                    alt.Tooltip("deposits:Q", title="Deposits", format=".2f"),
                    alt.Tooltip("withdrawals:Q", title="Withdrawals", format=".2f"),
                    alt.Tooltip("fees:Q", title="Fees", format=".2f"),
                ],
            )
            .properties(title="Daily Cash Flow Activity")
            .facet(facet=alt.Facet("year:O", title=None), columns=1)
        )
        st.altair_chart(calendar_heatmap, use_container_width=True)
    else:
        st.info("No daily data available to display heatmap.")

    # --- Data Table ---
    st.header("Daily Balance Data")
    st.dataframe(daily_df.sort_values("date", ascending=False))


if __name__ == "__main__":
    main()
