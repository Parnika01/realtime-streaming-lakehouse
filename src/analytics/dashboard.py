import time
from pathlib import Path
import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go

from src.config import config

# Page configuration
st.set_page_config(
    page_title="Real-Time Streaming Lakehouse | Data Engineering",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern dark-mode aesthetics
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e222d 0%, #15181f 100%);
        border: 1px solid #2d3343;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.25);
    }
    .metric-title {
        color: #8b949e;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-value {
        color: #58a6ff;
        font-size: 2.1rem;
        font-weight: 700;
        margin-top: 4px;
    }
    .metric-alert {
        color: #ff7b72;
    }
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        background-color: #238636;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_duckdb():
    return duckdb.connect(":memory:")

db = get_duckdb()

def load_lakehouse_data():
    """Queries partitioned Silver Parquet lakehouse via DuckDB."""
    silver_glob = str(config.SILVER_PATH / "**" / "*.parquet").replace("\\", "/")
    parquet_files = list(config.SILVER_PATH.glob("**/*.parquet"))
    
    if not parquet_files:
        return pd.DataFrame()

    try:
        query = f"""
            SELECT 
                transaction_id,
                event_timestamp,
                user_id,
                card_number_masked,
                amount,
                currency,
                merchant_name,
                merchant_category,
                city,
                country,
                latitude,
                longitude,
                risk_score,
                is_high_risk_alert,
                fraud_type
            FROM read_parquet('{silver_glob}', hive_partitioning=true)
            ORDER BY event_timestamp DESC
        """
        return db.execute(query).df()
    except Exception as e:
        st.error(f"Error reading Lakehouse: {e}")
        return pd.DataFrame()

# Header
st.title("⚡ Real-Time Financial Fraud & Streaming Lakehouse")
st.caption("End-to-End Streaming Architecture: Ingestion (Kafka/Redpanda) ➔ Processing (PySpark & Windowing) ➔ Storage (Partitioned Parquet / Iceberg) ➔ Real-Time Querying (DuckDB)")

# Sidebar Controls
st.sidebar.header("🕹️ Pipeline Control Panel")
auto_refresh = st.sidebar.checkbox("Auto-Refresh Stream (every 3s)", value=True)
sample_limit = st.sidebar.slider("Historical Records to Display", min_value=50, max_value=2000, value=250)

# Check broker & storage status
kafka_status = "🟢 ONLINE" if config.is_kafka_available() else "🟡 LOCAL BUFFER MODE"
st.sidebar.markdown(f"**Streaming Ingestion:** `{kafka_status}`")
st.sidebar.markdown(f"**Silver Lakehouse:** `{config.SILVER_PATH.name}`")
st.sidebar.markdown(f"**Gold Marts:** `{config.GOLD_PATH.name}`")

st.sidebar.divider()
st.sidebar.markdown("""
### 🏗️ Architecture Stack
- **Message Broker:** Redpanda (Kafka API)
- **Streaming Engine:** PySpark & Micro-batching
- **Storage:** Partitioned Parquet Lakehouse
- **In-Memory Analytics:** DuckDB
- **Data Quality:** Rule-based Anomaly Scoring
""")

# Load Data
df = load_lakehouse_data()

if df.empty:
    st.info("💡 **Awaiting stream data...** Run the event generator to start streaming transactions into the lakehouse.")
    st.code("python -m src.producer.transaction_generator", language="bash")
    if auto_refresh:
        time.sleep(3)
        st.rerun()
    st.stop()

# Cap records for performance
df_display = df.head(sample_limit)

# ========================================================
# 1. Executive Real-Time KPI Cards
# ========================================================
total_tx = len(df)
total_volume = df["amount"].sum()
alert_count = df["is_high_risk_alert"].sum()
fraud_rate = (alert_count / total_tx * 100) if total_tx > 0 else 0.0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total Ingested Events</div>
        <div class="metric-value">{total_tx:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total Processed Volume</div>
        <div class="metric-value">${total_volume:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">High-Risk Fraud Alerts</div>
        <div class="metric-value metric-alert">{alert_count:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Anomaly Detection Rate</div>
        <div class="metric-value">{fraud_rate:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

st.write("")

# ========================================================
# 2. Interactive Geo Map & Category Distribution
# ========================================================
map_col, chart_col = st.columns([3, 2])

with map_col:
    st.subheader("📍 Live Global Transaction Stream")
    # Color-code by risk
    map_df = df_display.dropna(subset=["latitude", "longitude"]).copy()
    map_df["Risk Level"] = map_df["is_high_risk_alert"].apply(lambda x: "High Risk (Alert)" if x else "Standard")
    
    fig_map = px.scatter_geo(
        map_df,
        lat="latitude",
        lon="longitude",
        color="Risk Level",
        color_discrete_map={"Standard": "#58a6ff", "High Risk (Alert)": "#ff7b72"},
        hover_name="merchant_name",
        hover_data={"amount": ":$.2f", "city": True, "country": True, "fraud_type": True},
        projection="natural earth",
        template="plotly_dark"
    )
    fig_map.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        paper_bgcolor="#161b22",
        geo=dict(bgcolor="#161b22", showland=True, landcolor="#21262d", countrycolor="#30363d")
    )
    st.plotly_chart(fig_map, use_container_width=True)

with chart_col:
    st.subheader("📊 Volume by Merchant Category")
    cat_df = df.groupby("merchant_category")["amount"].sum().reset_index().sort_values("amount", ascending=False)
    fig_cat = px.bar(
        cat_df,
        x="merchant_category",
        y="amount",
        labels={"merchant_category": "Category", "amount": "Volume ($ USD)"},
        color="amount",
        color_continuous_scale="Viridis",
        template="plotly_dark"
    )
    fig_cat.update_layout(paper_bgcolor="#161b22", plot_bgcolor="#161b22", showlegend=False)
    st.plotly_chart(fig_cat, use_container_width=True)

# ========================================================
# 3. Interactive Lakehouse SQL Query Engine (DuckDB)
# ========================================================
st.subheader("🔍 Interactive Lakehouse SQL Query Engine (Powered by DuckDB)")
st.caption("Execute zero-copy analytical SQL queries directly over partitioned Parquet files.")

silver_glob = str(config.SILVER_PATH / "**" / "*.parquet").replace("\\", "/")
default_sql = f"""SELECT 
    merchant_name,
    merchant_category,
    COUNT(*) as total_transactions,
    ROUND(SUM(amount), 2) as total_volume_usd,
    COUNT(CASE WHEN is_high_risk_alert THEN 1 END) as flagged_anomalies
FROM read_parquet('{silver_glob}', hive_partitioning=true)
GROUP BY merchant_name, merchant_category
ORDER BY total_volume_usd DESC
LIMIT 10;"""

user_query = st.text_area("SQL Query Editor", value=default_sql, height=130)

if st.button("🚀 Run SQL Query", type="primary"):
    try:
        query_result = db.execute(user_query).df()
        st.dataframe(query_result, use_container_width=True)
    except Exception as e:
        st.error(f"SQL Execution Error: {e}")

# ========================================================
# 4. Live Curated Event Feed (Silver Layer)
# ========================================================
st.subheader("📋 Curated Streaming Feed (Silver Table)")
st.dataframe(
    df_display[[
        "event_timestamp", "user_id", "merchant_name", "merchant_category",
        "amount", "city", "country", "risk_score", "is_high_risk_alert", "fraud_type"
    ]],
    use_container_width=True,
    height=300
)

# Auto-refresh loop
if auto_refresh:
    time.sleep(3)
    st.rerun()
