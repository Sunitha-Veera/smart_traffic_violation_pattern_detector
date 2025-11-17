# streamlit_app.py
import streamlit as st
import pandas as pd
import os
import plotly.express as px

st.set_page_config(layout="wide", page_title="Traffic Violation Analytics")

@st.cache_data
def load_parquet(path):
    return pd.read_parquet(path)

# Paths
AGG_DIR = "aggregated_results"
HOTSPOT_DIR = "hotspot_results"
CLEANED_PARQUET = "cleaned/traffic_cleaned.parquet"

st.title("Smart Traffic Violation — Analytics Dashboard (Milestone 4)")

# --- Load base cleaned data ---
if os.path.exists(CLEANED_PARQUET):
    df = load_parquet(CLEANED_PARQUET)
    # Use cleaned timestamp
    if "Timestamp_clean" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Timestamp_clean"])
    else:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
else:
    st.error(f"Cleaned parquet not found at {CLEANED_PARQUET}")
    st.stop()

# --- Sidebar filters ---
st.sidebar.header("Filters")
min_date = df["Timestamp"].min().date()
max_date = df["Timestamp"].max().date()

date_range = st.sidebar.date_input("Date range", value=(min_date, max_date))
violation_types = st.sidebar.multiselect(
    "Violation Type", options=sorted(df["ViolationType"].dropna().unique()),
    default=sorted(df["ViolationType"].dropna().unique())
)
severities = sorted(df["Severity"].dropna().unique())
severity_sel = st.sidebar.multiselect("Severity", options=severities, default=severities)

# Apply filters
start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
mask = (df["Timestamp"] >= start_date) & (df["Timestamp"] <= end_date)
if violation_types:
    mask &= df["ViolationType"].isin(violation_types)
if severity_sel:
    mask &= df["Severity"].isin(severity_sel)
df_filtered = df.loc[mask].copy()

st.markdown(f"**Showing {len(df_filtered):,} records** after filters")

# --- Time-based charts ---
st.header("Time-based Trends")
col1, col2 = st.columns((2,1))

with col1:
    df_filtered["Hour"] = df_filtered["Timestamp"].dt.hour
    per_hour = df_filtered.groupby("Hour").size().reset_index(name="Count")
    fig_hour = px.bar(per_hour, x="Hour", y="Count", title="Violations per Hour")
    st.plotly_chart(fig_hour, use_container_width=True)

    df_filtered["DayOfWeek"] = df_filtered["Timestamp"].dt.day_name()
    per_weekday = df_filtered.groupby("DayOfWeek").size().reindex(
        ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    ).reset_index(name="Count")
    fig_day = px.bar(per_weekday, x="DayOfWeek", y="Count", title="Violations by Weekday")
    st.plotly_chart(fig_day, use_container_width=True)

with col2:
    type_counts = df_filtered["ViolationType"].value_counts().reset_index()
    type_counts.columns = ["ViolationType","Count"]
    fig_type = px.pie(type_counts, names="ViolationType", values="Count", title="Violation Type Distribution")
    st.plotly_chart(fig_type, use_container_width=True)

# --- Cross-tab heatmap ---
st.header("Violation Type × Hour Heatmap")
cross_path = os.path.join(AGG_DIR, "cross_tab_type_hour")
if os.path.exists(cross_path):
    cross_df = load_parquet(cross_path)
    try:
        heat = cross_df.set_index("ViolationType").astype(int)
        fig_heat = px.imshow(heat, labels=dict(x="Hour", y="Violation Type", color="Count"),
                             x=list(heat.columns), y=list(heat.index))
        st.plotly_chart(fig_heat, use_container_width=True)
    except Exception:
        st.dataframe(cross_df.head())
else:
    st.info("Cross-tab results not found. Run pattern_analysis.py first.")

# --- Top N locations ---
st.header("Top Risk Locations")
loc_path = os.path.join(AGG_DIR, "violations_per_location")
if os.path.exists(loc_path):
    loc_df = load_parquet(loc_path)
    loc_df_sorted = loc_df.sort_values("Total_Violations", ascending=False).reset_index(drop=True)
    top_n = st.slider("Top N locations", min_value=1, max_value=min(50, len(loc_df_sorted)), value=10)
    st.dataframe(loc_df_sorted.head(top_n))
else:
    st.info("Violations by location not available.")

# --- Hotspots & map ---
st.header("Hotspots & Spatial View")
coords_exist = ("Latitude" in df_filtered.columns) and ("Longitude" in df_filtered.columns)
if coords_exist:
    st.subheader("Scatter Map of Violations")
    map_df = df_filtered[["Latitude","Longitude","ViolationType","Severity","Timestamp"]].dropna()
    st.map(map_df.rename(columns={"Latitude":"lat","Longitude":"lon"}))

    kmeans_path = os.path.join(HOTSPOT_DIR, "kmeans_clusters.parquet")
    if os.path.exists(kmeans_path):
        clusters = load_parquet(kmeans_path)
        st.write("Clusters (sample):")
        st.dataframe(clusters.head())
    else:
        st.info("KMeans cluster output not found.")
else:
    st.info("No coordinates found. Add Latitude/Longitude to cleaned dataset to enable spatial charts.")

# --- Export filtered dataset ---
st.header("Export Filtered Data")
def to_csv_bytes(df_):
    return df_.to_csv(index=False).encode('utf-8')

def to_json_bytes(df_):
    return df_.to_json(orient="records", force_ascii=False).encode('utf-8')

csv_bytes = to_csv_bytes(df_filtered)
json_bytes = to_json_bytes(df_filtered)

col1, col2 = st.columns(2)
with col1:
    st.download_button("Download CSV", data=csv_bytes, file_name="filtered_violations.csv", mime="text/csv")
with col2:
    st.download_button("Download JSON", data=json_bytes, file_name="filtered_violations.json", mime="application/json")

# --- Sidebar summary ---
st.sidebar.markdown("### Dataset Summary")
st.sidebar.write(f"Total records (raw): {len(df):,}")
st.sidebar.write(f"Total records (filtered): {len(df_filtered):,}")

st.markdown("---")
st.caption("Built with Streamlit — press `q` in terminal to stop the server.")
