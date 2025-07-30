# streamlit/pages/1_Pensions.py
import streamlit as st
import pandas as pd
import altair as alt
import sys
import os

# Add parent directory to path to import S3Helper
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from aws.connect_to_s3 import S3Helper

st.set_page_config(
    page_title="Pensions Dashboard",
    page_icon="💰",
    layout="wide",
)


@st.cache_data(ttl=600)
def load_latest_pension_data(_s3_helper, base_path, platform_name):
    """Loads the most recent pension timeseries file for a given platform."""
    try:
        platform_name_snake_case = platform_name.lower().replace(" ", "_")
        file_prefix = f"{base_path}/staging/timeseries_{platform_name_snake_case}_"

        files = _s3_helper.list_files(prefix=file_prefix)
        if not files:
            st.warning(
                f"No staging data found for '{platform_name}'. Please run the pensions pipeline."
            )
            return None

        latest_file = sorted(files)[-1]
        st.info(f"Loading {platform_name} data from: `{latest_file}`")
        df = _s3_helper.read_csv_from_s3(latest_file)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Failed to load data for {platform_name}: {e}")
        return None


def create_performance_chart(df, platform_name):
    """Creates a chart comparing pension value vs. cash invested."""

    base = alt.Chart(df).encode(x=alt.X("timestamp:T", title="Date"))

    pension_value_line = base.mark_line(color="skyblue", point=True).encode(
        y=alt.Y(
            "imputed_pension_value:Q", title="Value (£)", scale=alt.Scale(zero=False)
        ),
        tooltip=[
            alt.Tooltip("timestamp:T", title="Date"),
            alt.Tooltip("pension_value:Q", title="Pension Value", format=",.2f"),
            alt.Tooltip("cash_invested:Q", title="Cash Invested", format=",.2f"),
        ],
    )

    cash_invested_line = base.mark_line(
        color="orange", point=True, interpolate="step-after"
    ).encode(
        y=alt.Y("cash_invested:Q", title="Value (£)"),
        tooltip=[
            alt.Tooltip("timestamp:T", title="Date"),
            alt.Tooltip("cash_invested:Q", title="Cash Invested", format=",.2f"),
            alt.Tooltip("pension_value:Q", title="Pension Value", format=",.2f"),
        ],
    )

    return (
        (pension_value_line + cash_invested_line)
        .interactive()
        .properties(title=f"{platform_name}: Pension Value vs. Cash Invested")
    )


def create_gain_loss_chart(df, platform_name):
    """Creates a bar chart showing absolute gain/loss over time."""

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("timestamp:T", title="Date"),
            y=alt.Y("gain_loss_absolute:Q", title="Gain / Loss (£)"),
            color=alt.condition(
                alt.datum.gain_loss_absolute > 0,
                alt.value("mediumseagreen"),
                alt.value("indianred"),
            ),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Date"),
                alt.Tooltip("gain_loss_absolute:Q", title="Gain / Loss", format=",.2f"),
                alt.Tooltip(
                    "gain_loss_percentage:Q", title="Gain / Loss %", format=".2f"
                ),
            ],
        )
        .interactive()
        .properties(title=f"{platform_name}: Absolute Gain / Loss Over Time")
    )
    return chart


def main():
    """Main Streamlit application for the Pensions Dashboard."""
    st.title("💰 Pensions Performance Dashboard")
    st.markdown(
        "An overview of your pension performance based on cashflows and snapshots."
    )

    try:
        from configuration.secrets import ENVIRONMENT
    except ImportError:
        st.error(
            "Secrets file not found. Please ensure `configuration/secrets.py` is set up."
        )
        st.stop()

    s3_helper = S3Helper()
    base_path = f"{ENVIRONMENT}/pensions"

    # --- Load Data for Both Platforms ---
    wahed_df = load_latest_pension_data(s3_helper, base_path, "Wahed")
    sl_df = load_latest_pension_data(s3_helper, base_path, "Standard Life")

    if wahed_df is None and sl_df is None:
        st.warning("No pension data could be loaded. Please run the pipeline.")
        st.stop()

    # --- Create Tabs for Each Pension ---
    tab1, tab2 = st.tabs(["Wahed", "Standard Life"])

    with tab1:
        st.header("Wahed SIPP Performance")
        if wahed_df is not None:
            # Key Metrics
            latest_wahed = wahed_df.iloc[-1]
            col1, col2, col3 = st.columns(3)
            col1.metric(
                "Latest Pension Value", f"£{latest_wahed['pension_value']:,.2f}"
            )
            col2.metric("Total Cash Invested", f"£{latest_wahed['cash_invested']:,.2f}")
            col3.metric(
                "Overall Gain / Loss",
                f"£{latest_wahed['gain_loss_absolute']:,.2f}",
                f"{latest_wahed['gain_loss_percentage']:.2f}%",
            )

            # Charts
            st.altair_chart(
                create_performance_chart(wahed_df, "Wahed"), use_container_width=True
            )
            st.altair_chart(
                create_gain_loss_chart(wahed_df, "Wahed"), use_container_width=True
            )

            # Data Table
            with st.expander("View Raw Staging Data"):
                st.dataframe(wahed_df)
        else:
            st.info("No data available for Wahed.")

    with tab2:
        st.header("Standard Life Pension Performance")
        if sl_df is not None:
            # Key Metrics
            latest_sl = sl_df.iloc[-1]
            col1, col2, col3 = st.columns(3)
            col1.metric("Latest Pension Value", f"£{latest_sl['pension_value']:,.2f}")
            col2.metric("Total Cash Invested", f"£{latest_sl['cash_invested']:,.2f}")
            col3.metric(
                "Overall Gain / Loss",
                f"£{latest_sl['gain_loss_absolute']:,.2f}",
                f"{latest_sl['gain_loss_percentage']:.2f}%",
            )

            # Charts
            st.altair_chart(
                create_performance_chart(sl_df, "Standard Life"),
                use_container_width=True,
            )
            st.altair_chart(
                create_gain_loss_chart(sl_df, "Standard Life"), use_container_width=True
            )

            # Data Table
            with st.expander("View Raw Staging Data"):
                st.dataframe(sl_df)
        else:
            st.info("No data available for Standard Life.")


if __name__ == "__main__":
    main()
