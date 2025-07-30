import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import json
import sys
import os

# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from aws.connect_to_s3 import S3Helper
from configuration.secrets import ENVIRONMENT

# Initialize S3 helper
s3 = S3Helper()


def load_data(file_path):
    """Load data from S3 and handle empty dataframes"""
    try:
        data = s3.read_csv_from_s3(file_path)
        if data is None or data.empty or data.isna().all().all():
            return None
        return data
    except Exception as e:
        st.error(f"Failed to load data from {file_path}: {e}")
        return None


def format_currency(value):
    """Format currency values"""
    if pd.isna(value) or value == 0:
        return "£0.00"
    if abs(value) >= 1000:
        return f"£{value:,.2f}"
    else:
        return f"£{value:.2f}"


def format_percentage(value):
    """Format percentage values"""
    if pd.isna(value) or value == 0:
        return "0.00%"
    return f"{value:.2f}%"


def create_performance_timeseries(performance_data):
    """Create performance timeseries charts"""
    if performance_data is None or performance_data.empty:
        return None

    # Convert date column
    performance_data["date"] = pd.to_datetime(performance_data["date"])

    # Create subplots
    fig = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=(
            "Portfolio Value Over Time",
            "Daily Returns",
            "Cumulative Returns",
            "Volatility (30-day)",
            "Sharpe Ratio",
            "Maximum Drawdown",
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
        ],
    )

    # Portfolio Value
    fig.add_trace(
        go.Scatter(
            x=performance_data["date"],
            y=performance_data["total_portfolio_value"],
            mode="lines",
            name="Portfolio Value",
            line=dict(color="#1f77b4"),
        ),
        row=1,
        col=1,
    )

    # Daily Returns
    fig.add_trace(
        go.Scatter(
            x=performance_data["date"],
            y=performance_data["daily_return"] * 100,
            mode="lines",
            name="Daily Return (%)",
            line=dict(color="#ff7f0e"),
        ),
        row=1,
        col=2,
    )

    # Cumulative Returns
    fig.add_trace(
        go.Scatter(
            x=performance_data["date"],
            y=performance_data["cumulative_return"] * 100,
            mode="lines",
            name="Cumulative Return (%)",
            line=dict(color="#2ca02c"),
        ),
        row=2,
        col=1,
    )

    # Volatility
    fig.add_trace(
        go.Scatter(
            x=performance_data["date"],
            y=performance_data["volatility_30d"] * 100,
            mode="lines",
            name="Volatility (%)",
            line=dict(color="#d62728"),
        ),
        row=2,
        col=2,
    )

    # Sharpe Ratio
    fig.add_trace(
        go.Scatter(
            x=performance_data["date"],
            y=performance_data["sharpe_ratio"],
            mode="lines",
            name="Sharpe Ratio",
            line=dict(color="#9467bd"),
        ),
        row=3,
        col=1,
    )

    # Maximum Drawdown
    fig.add_trace(
        go.Scatter(
            x=performance_data["date"],
            y=performance_data["max_drawdown"] * 100,
            mode="lines",
            name="Max Drawdown (%)",
            line=dict(color="#8c564b"),
        ),
        row=3,
        col=2,
    )

    fig.update_layout(
        height=900, showlegend=False, title_text="Performance Metrics Over Time"
    )
    return fig


def create_risk_timeseries(risk_data):
    """Create risk metrics timeseries charts"""
    if risk_data is None or risk_data.empty:
        return None

    # Convert date column
    risk_data["date"] = pd.to_datetime(risk_data["date"])

    # Create subplots
    fig = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=(
            "VaR (95%, 30-day)",
            "CVaR (95%, 30-day)",
            "Downside Deviation",
            "Sortino Ratio",
            "Calmar Ratio",
            "Information Ratio",
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
        ],
    )

    # VaR
    fig.add_trace(
        go.Scatter(
            x=risk_data["date"],
            y=risk_data["var_95_30d"] * 100,
            mode="lines",
            name="VaR (%)",
            line=dict(color="#e377c2"),
        ),
        row=1,
        col=1,
    )

    # CVaR
    fig.add_trace(
        go.Scatter(
            x=risk_data["date"],
            y=risk_data["cvar_95_30d"] * 100,
            mode="lines",
            name="CVaR (%)",
            line=dict(color="#7f7f7f"),
        ),
        row=1,
        col=2,
    )

    # Downside Deviation
    fig.add_trace(
        go.Scatter(
            x=risk_data["date"],
            y=risk_data["downside_deviation"] * 100,
            mode="lines",
            name="Downside Deviation (%)",
            line=dict(color="#bcbd22"),
        ),
        row=2,
        col=1,
    )

    # Sortino Ratio
    fig.add_trace(
        go.Scatter(
            x=risk_data["date"],
            y=risk_data["sortino_ratio"],
            mode="lines",
            name="Sortino Ratio",
            line=dict(color="#17becf"),
        ),
        row=2,
        col=2,
    )

    # Calmar Ratio
    fig.add_trace(
        go.Scatter(
            x=risk_data["date"],
            y=risk_data["calmar_ratio"],
            mode="lines",
            name="Calmar Ratio",
            line=dict(color="#ff9896"),
        ),
        row=3,
        col=1,
    )

    # Information Ratio
    fig.add_trace(
        go.Scatter(
            x=risk_data["date"],
            y=risk_data["information_ratio"],
            mode="lines",
            name="Information Ratio",
            line=dict(color="#98df8a"),
        ),
        row=3,
        col=2,
    )

    fig.update_layout(height=900, showlegend=False, title_text="Risk Metrics Over Time")
    return fig


def create_allocation_visualizations(allocation_data):
    """Create allocation analysis visualizations"""
    if allocation_data is None or allocation_data.empty:
        return None

    # Pie chart for allocation
    fig_pie = px.pie(
        allocation_data,
        values="current_value_gbp",
        names="asset_symbol",
        title="Portfolio Allocation by Asset",
        color_discrete_sequence=px.colors.qualitative.Set3,
    )

    # Bar chart for allocation percentages
    fig_bar = px.bar(
        allocation_data,
        x="asset_symbol",
        y="percentage_allocation",
        title="Portfolio Allocation Percentages",
        color="asset_symbol",
        color_discrete_sequence=px.colors.qualitative.Set3,
    )

    # Diversification metrics
    fig_diversification = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Concentration Risk", "Diversification Score"),
        specs=[[{"type": "bar"}, {"type": "indicator"}]],
    )

    fig_diversification.add_trace(
        go.Bar(
            x=allocation_data["asset_symbol"],
            y=allocation_data["concentration_risk"],
            name="Concentration Risk",
            marker_color="#ff7f0e",
        ),
        row=1,
        col=1,
    )

    avg_diversification = allocation_data["diversification_score"].mean()
    fig_diversification.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=avg_diversification * 100,
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": "Portfolio Diversification"},
            gauge={
                "axis": {"range": [None, 100]},
                "bar": {"color": "darkblue"},
                "steps": [
                    {"range": [0, 50], "color": "lightgray"},
                    {"range": [50, 80], "color": "yellow"},
                    {"range": [80, 100], "color": "green"},
                ],
            },
        ),
        row=1,
        col=2,
    )

    fig_diversification.update_layout(height=400, title_text="Diversification Analysis")

    return fig_pie, fig_bar, fig_diversification


def create_market_insights_visualizations(market_insights_data):
    """Create market insights visualizations"""
    if market_insights_data is None or market_insights_data.empty:
        return None

    # Price vs Moving Averages
    fig_ma = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Price vs 30-day MA",
            "Price vs 90-day MA",
            "Momentum (30-day)",
            "Momentum (90-day)",
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
        ],
    )

    for _, asset in market_insights_data.iterrows():
        symbol = asset["asset_symbol"]

        # Price vs 30-day MA
        fig_ma.add_trace(
            go.Scatter(
                x=[symbol],
                y=[asset["current_price"]],
                mode="markers",
                name=f"{symbol} Current Price",
                marker=dict(size=10, color="blue"),
            ),
            row=1,
            col=1,
        )
        fig_ma.add_trace(
            go.Scatter(
                x=[symbol],
                y=[asset["price_ma_30d"]],
                mode="markers",
                name=f"{symbol} 30-day MA",
                marker=dict(size=10, color="red"),
            ),
            row=1,
            col=1,
        )

        # Price vs 90-day MA
        fig_ma.add_trace(
            go.Scatter(
                x=[symbol],
                y=[asset["current_price"]],
                mode="markers",
                name=f"{symbol} Current Price",
                marker=dict(size=10, color="blue"),
                showlegend=False,
            ),
            row=1,
            col=2,
        )
        fig_ma.add_trace(
            go.Scatter(
                x=[symbol],
                y=[asset["price_ma_90d"]],
                mode="markers",
                name=f"{symbol} 90-day MA",
                marker=dict(size=10, color="green"),
                showlegend=False,
            ),
            row=1,
            col=2,
        )

        # Momentum 30-day
        fig_ma.add_trace(
            go.Bar(
                x=[symbol],
                y=[asset["momentum_30d"]],
                name=f"{symbol} 30-day Momentum",
                marker_color="orange",
            ),
            row=2,
            col=1,
        )

        # Momentum 90-day
        fig_ma.add_trace(
            go.Bar(
                x=[symbol],
                y=[asset["momentum_90d"]],
                name=f"{symbol} 90-day Momentum",
                marker_color="purple",
            ),
            row=2,
            col=2,
        )

    fig_ma.update_layout(height=600, title_text="Market Insights Analysis")

    # RSI and Volatility
    fig_technical = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("RSI by Asset", "Volatility by Asset"),
        specs=[[{"type": "bar"}, {"type": "bar"}]],
    )

    fig_technical.add_trace(
        go.Bar(
            x=market_insights_data["asset_symbol"],
            y=market_insights_data["rsi"],
            name="RSI",
            marker_color="#1f77b4",
        ),
        row=1,
        col=1,
    )

    fig_technical.add_trace(
        go.Bar(
            x=market_insights_data["asset_symbol"],
            y=market_insights_data["volatility_30d"] * 100,
            name="Volatility (%)",
            marker_color="#ff7f0e",
        ),
        row=1,
        col=2,
    )

    fig_technical.update_layout(height=400, title_text="Technical Indicators")

    return fig_ma, fig_technical


def create_correlation_heatmap(correlation_data):
    """Create correlation matrix heatmap"""
    if correlation_data is None or correlation_data.empty:
        return None

    try:
        # Parse correlation matrix
        correlation_matrix_str = correlation_data["correlation_matrix"].iloc[0]
        correlation_matrix = json.loads(correlation_matrix_str)

        # Convert to DataFrame
        assets = list(correlation_matrix.keys())
        corr_df = pd.DataFrame(correlation_matrix)

        # Create heatmap
        fig = px.imshow(
            corr_df,
            text_auto=True,
            aspect="auto",
            title="Asset Correlation Matrix",
            color_continuous_scale="RdBu_r",
        )

        fig.update_layout(height=500)
        return fig

    except Exception as e:
        st.error(f"Error creating correlation heatmap: {e}")
        return None


# Main dashboard
st.title("📊 Coinbase Portfolio Analytics Dashboard")

# Load all data
with st.spinner("Loading data..."):
    # Current data
    current_positions = load_data(
        f"{ENVIRONMENT}/coinbase/processed/current/positions.csv"
    )
    market_overview = load_data(
        f"{ENVIRONMENT}/coinbase/processed/current/market_overview.csv"
    )
    portfolio_summary = load_data(
        f"{ENVIRONMENT}/coinbase/processed/current/portfolio_summary.csv"
    )

    # Historical data
    portfolio_snapshots = load_data(
        f"{ENVIRONMENT}/coinbase/processed/historical/portfolio_snapshots.csv"
    )
    pnl_tracking = load_data(
        f"{ENVIRONMENT}/coinbase/processed/historical/pnl_tracking.csv"
    )

    # Analytics data
    performance_metrics = load_data(
        f"{ENVIRONMENT}/coinbase/analytics/performance_metrics.csv"
    )
    risk_metrics = load_data(f"{ENVIRONMENT}/coinbase/analytics/risk_metrics.csv")
    allocation_analysis = load_data(
        f"{ENVIRONMENT}/coinbase/analytics/allocation_analysis.csv"
    )
    market_insights = load_data(f"{ENVIRONMENT}/coinbase/analytics/market_insights.csv")
    correlation_metrics = load_data(
        f"{ENVIRONMENT}/coinbase/analytics/correlation_metrics.csv"
    )

# Portfolio Overview Section
st.header("📈 Portfolio Overview")

if portfolio_summary is not None and not portfolio_summary.empty:
    summary = portfolio_summary.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Portfolio Value",
            format_currency(summary["total_portfolio_value"]),
            delta=format_currency(summary["total_unrealized_pnl"]),
        )

    with col2:
        st.metric("Number of Assets", int(summary["num_assets"]), delta=None)

    with col3:
        st.metric(
            "Unrealized P&L",
            format_currency(summary["total_unrealized_pnl"]),
            delta=format_percentage(summary["total_unrealized_pnl_pct"]),
        )

    with col4:
        if performance_metrics is not None and not performance_metrics.empty:
            latest_return = performance_metrics["daily_return"].iloc[-1] * 100
            st.metric(
                "Latest Daily Return", format_percentage(latest_return), delta=None
            )

# Current Positions Table
if current_positions is not None and not current_positions.empty:
    st.subheader("Current Positions")

    # Format the display data
    display_positions = current_positions.copy()
    display_positions["current_value_gbp"] = display_positions[
        "current_value_gbp"
    ].apply(format_currency)
    display_positions["unrealized_pnl"] = display_positions["unrealized_pnl"].apply(
        format_currency
    )
    display_positions["unrealized_pnl_percentage"] = display_positions[
        "unrealized_pnl_percentage"
    ].apply(format_percentage)

    st.dataframe(
        display_positions[
            [
                "asset_symbol",
                "quantity",
                "current_value_gbp",
                "unrealized_pnl",
                "unrealized_pnl_percentage",
            ]
        ],
        use_container_width=True,
    )

# Main Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    [
        "📈 Performance Analytics",
        "⚠️ Risk Analytics",
        "💰 Allocation Analysis",
        "📊 Market Insights",
        "🔗 Correlation Analysis",
        "📋 Data Tables",
    ]
)

# Performance Analytics Tab
with tab1:
    st.header("Performance Analytics")

    if performance_metrics is not None and not performance_metrics.empty:
        # Timeseries charts
        performance_fig = create_performance_timeseries(performance_metrics)
        if performance_fig:
            st.plotly_chart(performance_fig, use_container_width=True)

        # Latest metrics
        st.subheader("Latest Performance Metrics")
        latest_metrics = performance_metrics.iloc[-1]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Total Portfolio Value",
                format_currency(latest_metrics["total_portfolio_value"]),
            )
            st.metric(
                "Daily Return", format_percentage(latest_metrics["daily_return"] * 100)
            )
            st.metric(
                "Cumulative Return",
                format_percentage(latest_metrics["cumulative_return"] * 100),
            )

        with col2:
            st.metric(
                "30-day Volatility",
                format_percentage(latest_metrics["volatility_30d"] * 100),
            )
            st.metric("Sharpe Ratio", f"{latest_metrics['sharpe_ratio']:.3f}")
            st.metric(
                "Maximum Drawdown",
                format_percentage(latest_metrics["max_drawdown"] * 100),
            )

        with col3:
            st.metric("Beta", f"{latest_metrics['beta']:.3f}")
            st.metric("Alpha", f"{latest_metrics['alpha']:.3f}")

        # Performance Metrics Explanation
        with st.expander("📚 Performance Metrics Explanation"):
            st.markdown(
                """
            **Performance Metrics Explained:**
            
            **Portfolio Value**: Total current value of all assets in GBP
            **Daily Return**: Percentage change in portfolio value from previous day
            **Cumulative Return**: Total percentage return since the start of tracking
            **Volatility (30-day)**: Standard deviation of daily returns over 30 days, measures price variability
            **Sharpe Ratio**: Risk-adjusted return measure (excess return per unit of risk), higher is better
            **Maximum Drawdown**: Largest peak-to-trough decline in portfolio value
            **Beta**: Portfolio sensitivity to market movements (1.0 = market average)
            **Alpha**: Excess return relative to market benchmark
            """
            )

# Risk Analytics Tab
with tab2:
    st.header("Risk Analytics")

    if risk_metrics is not None and not risk_metrics.empty:
        # Timeseries charts
        risk_fig = create_risk_timeseries(risk_metrics)
        if risk_fig:
            st.plotly_chart(risk_fig, use_container_width=True)

        # Latest risk metrics
        st.subheader("Latest Risk Metrics")
        latest_risk = risk_metrics.iloc[-1]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "VaR (95%, 30-day)", format_percentage(latest_risk["var_95_30d"] * 100)
            )
            st.metric(
                "CVaR (95%, 30-day)",
                format_percentage(latest_risk["cvar_95_30d"] * 100),
            )
            st.metric(
                "Downside Deviation",
                format_percentage(latest_risk["downside_deviation"] * 100),
            )

        with col2:
            st.metric("Sortino Ratio", f"{latest_risk['sortino_ratio']:.3f}")
            st.metric("Calmar Ratio", f"{latest_risk['calmar_ratio']:.3f}")
            st.metric("Information Ratio", f"{latest_risk['information_ratio']:.3f}")

        with col3:
            st.metric(
                "30-day Volatility",
                format_percentage(latest_risk["volatility_30d"] * 100),
            )
            st.metric(
                "Maximum Drawdown", format_percentage(latest_risk["max_drawdown"] * 100)
            )

        # Risk Metrics Explanation
        with st.expander("📚 Risk Metrics Explanation"):
            st.markdown(
                """
            **Risk Metrics Explained:**
            
            **VaR (Value at Risk)**: Maximum expected loss over 30 days with 95% confidence
            **CVaR (Conditional VaR)**: Average loss beyond VaR threshold (tail risk)
            **Downside Deviation**: Standard deviation of negative returns only
            **Sortino Ratio**: Risk-adjusted return using downside deviation instead of volatility
            **Calmar Ratio**: Annual return divided by maximum drawdown
            **Information Ratio**: Excess return per unit of active risk
            **Volatility**: Standard deviation of returns, measures price variability
            **Maximum Drawdown**: Largest peak-to-trough decline in portfolio value
            """
            )

# Allocation Analysis Tab
with tab3:
    st.header("Allocation Analysis")

    if allocation_analysis is not None and not allocation_analysis.empty:
        # Create visualizations
        fig_pie, fig_bar, fig_diversification = create_allocation_visualizations(
            allocation_analysis
        )

        if fig_pie:
            st.plotly_chart(fig_pie, use_container_width=True)

        if fig_bar:
            st.plotly_chart(fig_bar, use_container_width=True)

        if fig_diversification:
            st.plotly_chart(fig_diversification, use_container_width=True)

        # Allocation metrics
        st.subheader("Allocation Metrics")

        col1, col2, col3 = st.columns(3)

        with col1:
            avg_concentration = allocation_analysis["concentration_risk"].mean()
            st.metric("Average Concentration Risk", f"{avg_concentration:.6f}")

        with col2:
            avg_diversification = allocation_analysis["diversification_score"].mean()
            st.metric("Portfolio Diversification Score", f"{avg_diversification:.3f}")

        with col3:
            effective_assets = allocation_analysis["effective_assets"].iloc[0]
            st.metric("Effective Number of Assets", f"{effective_assets:.1f}")

        # Allocation Metrics Explanation
        with st.expander("📚 Allocation Metrics Explanation"):
            st.markdown(
                """
            **Allocation Metrics Explained:**
            
            **Percentage Allocation**: Share of each asset in total portfolio value
            **Concentration Risk**: Risk measure based on asset concentration (lower is better)
            **Diversification Score**: Measure of portfolio diversification (0-1, higher is better)
            **Effective Assets**: Number of equally-weighted assets that would provide same diversification
            **Unrealized P&L**: Current profit/loss on each position
            **Unrealized P&L %**: Percentage gain/loss on each position
            **Cost Basis**: Total amount invested in each asset
            """
            )

# Market Insights Tab
with tab4:
    st.header("Market Insights")

    if market_insights is not None and not market_insights.empty:
        # Create visualizations
        fig_ma, fig_technical = create_market_insights_visualizations(market_insights)

        if fig_ma:
            st.plotly_chart(fig_ma, use_container_width=True)

        if fig_technical:
            st.plotly_chart(fig_technical, use_container_width=True)

        # Market insights table
        st.subheader("Technical Indicators")

        display_insights = market_insights.copy()
        display_insights["current_price"] = display_insights["current_price"].apply(
            format_currency
        )
        display_insights["price_ma_30d"] = display_insights["price_ma_30d"].apply(
            format_currency
        )
        display_insights["price_ma_90d"] = display_insights["price_ma_90d"].apply(
            format_currency
        )
        display_insights["volatility_30d"] = display_insights["volatility_30d"].apply(
            lambda x: f"{x*100:.2f}%"
        )
        display_insights["momentum_30d"] = display_insights["momentum_30d"].apply(
            lambda x: f"{x*100:.2f}%"
        )
        display_insights["momentum_90d"] = display_insights["momentum_90d"].apply(
            lambda x: f"{x*100:.2f}%"
        )
        display_insights["rsi"] = display_insights["rsi"].apply(lambda x: f"{x:.2f}")

        st.dataframe(
            display_insights[
                [
                    "asset_symbol",
                    "current_price",
                    "price_ma_30d",
                    "price_ma_90d",
                    "volatility_30d",
                    "momentum_30d",
                    "momentum_90d",
                    "rsi",
                    "trend",
                ]
            ],
            use_container_width=True,
        )

        # Market Insights Explanation
        with st.expander("📚 Market Insights Explanation"):
            st.markdown(
                """
            **Market Insights Explained:**
            
            **Current Price**: Latest market price of the asset
            **30-day Moving Average**: Average price over last 30 days
            **90-day Moving Average**: Average price over last 90 days
            **Volatility (30-day)**: Price variability over 30 days
            **Momentum (30-day)**: Price change over last 30 days
            **Momentum (90-day)**: Price change over last 90 days
            **RSI (Relative Strength Index)**: Momentum oscillator (0-100, >70 overbought, <30 oversold)
            **Trend**: Overall price direction (bullish/bearish/neutral)
            **Volatility Level**: Categorization of volatility (low/medium/high)
            """
            )

# Correlation Analysis Tab
with tab5:
    st.header("Correlation Analysis")

    if correlation_metrics is not None and not correlation_metrics.empty:
        # Correlation heatmap
        correlation_fig = create_correlation_heatmap(correlation_metrics)
        if correlation_fig:
            st.plotly_chart(correlation_fig, use_container_width=True)

        # Correlation metrics
        st.subheader("Correlation Metrics")

        latest_corr = correlation_metrics.iloc[0]

        col1, col2 = st.columns(2)

        with col1:
            st.metric("Average Correlation", f"{latest_corr['avg_correlation']:.3f}")

        with col2:
            st.metric(
                "Diversification Score", f"{latest_corr['diversification_score']:.3f}"
            )

        # Correlation Explanation
        with st.expander("📚 Correlation Analysis Explanation"):
            st.markdown(
                """
            **Correlation Analysis Explained:**
            
            **Correlation Matrix**: Shows how assets move relative to each other (-1 to +1)
            **Average Correlation**: Mean correlation across all asset pairs
            **Diversification Score**: Measure of portfolio diversification (0-1, higher is better)
            
            **Correlation Interpretation:**
            - **+1.0**: Perfect positive correlation (assets move together)
            - **0.0**: No correlation (assets move independently)
            - **-1.0**: Perfect negative correlation (assets move opposite)
            
            **Diversification Benefits:**
            - Lower average correlation = better diversification
            - Higher diversification score = reduced portfolio risk
            """
            )

# Data Tables Tab
with tab6:
    st.header("Raw Data Tables")

    # Performance Metrics Table
    with st.expander("Performance Metrics"):
        if performance_metrics is not None and not performance_metrics.empty:
            st.dataframe(performance_metrics, use_container_width=True)

    # Risk Metrics Table
    with st.expander("Risk Metrics"):
        if risk_metrics is not None and not risk_metrics.empty:
            st.dataframe(risk_metrics, use_container_width=True)

    # Allocation Analysis Table
    with st.expander("Allocation Analysis"):
        if allocation_analysis is not None and not allocation_analysis.empty:
            st.dataframe(allocation_analysis, use_container_width=True)

    # Market Insights Table
    with st.expander("Market Insights"):
        if market_insights is not None and not market_insights.empty:
            st.dataframe(market_insights, use_container_width=True)

    # Correlation Metrics Table
    with st.expander("Correlation Metrics"):
        if correlation_metrics is not None and not correlation_metrics.empty:
            st.dataframe(correlation_metrics, use_container_width=True)

    # Portfolio Snapshots Table
    with st.expander("Portfolio Snapshots"):
        if portfolio_snapshots is not None and not portfolio_snapshots.empty:
            st.dataframe(portfolio_snapshots, use_container_width=True)

    # P&L Tracking Table
    with st.expander("P&L Tracking"):
        if pnl_tracking is not None and not pnl_tracking.empty:
            st.dataframe(pnl_tracking, use_container_width=True)

# Footer
st.markdown("---")
st.markdown(
    "*Dashboard last updated: " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S") + "*"
)
