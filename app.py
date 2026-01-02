import streamlit as st
import polars as pl
import os
from datetime import datetime

st.set_page_config(page_title="Deduplicator Pro", page_icon="🇨🇱", layout="wide")

st.title("🇨🇱 CSV Cleaner & Parquet Converter")
st.markdown("### Optimized for large files (up to 3GB)")

col_set1, col_set2 = st.columns(2)
with col_set1:
    sep = st.selectbox("CSV Separator", [";", ","], help="Chilean files often use semicolon (;)")
with col_set2:
    st.info("Note: System handles Chilean characters, 'NA' values, and Date parsing automatically.")

uploaded_file = st.file_uploader("Upload your CSV", type=["csv"])

if uploaded_file:
    with open("temp_input.csv", "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    try:
        # 1. PEAK DATA for filters
        # We read a small sample to get column names and types
        sample_df = pl.read_csv(
            "temp_input.csv", separator=sep, encoding="latin-1", 
            n_rows=1000, ignore_errors=True, null_values=["NA", "N/A", "null", ""]
        )
        all_columns = sample_df.columns
        
        st.markdown("---")
        col_f1, col_f2 = st.columns(2)
        
        with col_f1:
            st.subheader("🎯 Column Selection")
            if 'selected_cols' not in st.session_state:
                st.session_state.selected_cols = all_columns

            b1, b2 = st.columns(2)
            if b1.button("✅ Select All"): st.session_state.selected_cols = all_columns
            if b2.button("❌ Deselect All"): st.session_state.selected_cols = []

            selected_columns = st.multiselect(
                "Keep these columns:", options=all_columns, default=st.session_state.selected_cols
            )

        with col_f2:
            st.subheader("📅 Date Filter (Optional)")
            date_col = st.selectbox("Select a Date Column to filter by:", ["None"] + all_columns)
            
            filter_dates = None
            if date_col != "None":
                # Detect min/max dates from sample or let user pick
                filter_dates = st.date_input("Select Date Range:", [datetime(2025, 1, 1), datetime(2026, 12, 31)])

        if st.button("🚀 Process, Filter & Clean Data", use_container_width=True):
            with st.status("Processing huge file...", expanded=True) as status:
                st.write("🔍 Loading and Cleaning...")
                
                # Reading with specific fixes for your 'i64' and 'NA' errors
                df = pl.read_csv(
                    "temp_input.csv", 
                    separator=sep, 
                    encoding="latin-1", 
                    columns=selected_columns if selected_columns else None,
                    ignore_errors=True,
                    null_values=["NA", "N/A", "null", "", " "], 
                    infer_schema_length=10000 
                )
                
                # Apply Date Filter if selected
                if date_col != "None" and len(filter_dates) == 2:
                    st.write(f"📅 Filtering for dates between {filter_dates[0]} and {filter_dates[1]}...")
                    # Convert column to date type safely
                    df = df.with_columns(pl.col(date_col).str.to_date(format="%m/%d/%Y", strict=False))
                    df = df.filter(pl.col(date_col).is_between(filter_dates[0], filter_dates[1]))

                initial_rows = df.height
                df_unique = df.unique()
                
                output_parquet = "cleaned_data.parquet"
                df_unique.write_parquet(output_parquet)
                
                status.update(label=f"✅ Done! Removed {initial_rows - df_unique.height:,} duplicates.", state="complete")
                st.balloons()

                st.markdown("### 👀 Data Preview")
                st.dataframe(df_unique.head(10).to_pandas(), use_container_width=True)

                with open(output_parquet, "rb") as f:
                    st.download_button("Download Cleaned Parquet", f, "final_data.parquet", use_container_width=True)
                    
    except Exception as e:
        st.error(f"Error: {e}")
    finally:
        if os.path.exists("temp_input.csv"): os.remove("temp_input.csv")
