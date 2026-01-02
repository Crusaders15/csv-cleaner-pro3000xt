import streamlit as st
import polars as pl
import os

st.set_page_config(page_title="Deduplicator Pro", page_icon="🇨🇱")

st.title("🇨🇱 CSV Cleaner & Parquet Converter")
st.markdown("### Optimized for large files (up to 3GB)")

# Column 1 & 2 for settings
col_set1, col_set2 = st.columns(2)
with col_set1:
    sep = st.selectbox("CSV Separator", [";", ","], help="Chilean files often use semicolon (;)")
with col_set2:
    st.info("Note: System handles Chilean characters and 'NA' values automatically.")

uploaded_file = st.file_uploader("Upload your CSV", type=["csv"])

if uploaded_file:
    # Save the file temporarily to read headers
    with open("temp_input.csv", "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    try:
        # Get headers efficiently without loading the whole file
        all_columns = pl.read_csv(
            "temp_input.csv", 
            separator=sep, 
            encoding="latin-1", 
            n_rows=0,
            truncate_ragged_lines=True
        ).columns
        
        st.markdown("---")
        st.subheader("🎯 Column Selection")

        # Session state for column selection buttons
        if 'selected_cols' not in st.session_state:
            st.session_state.selected_cols = all_columns

        btn_col1, btn_col2, _ = st.columns([1, 1, 3])
        if btn_col1.button("✅ Select All"):
            st.session_state.selected_cols = all_columns
        if btn_col2.button("❌ Deselect All"):
            st.session_state.selected_cols = []

        # The actual multiselect widget
        selected_columns = st.multiselect(
            "Search and select columns to keep:", 
            options=all_columns, 
            default=st.session_state.selected_cols,
            key="col_selector"
        )

        if not selected_columns:
            st.warning("Select at least one column to proceed.")
            st.stop()

        if st.button("🚀 Process and Clean Data", use_container_width=True):
            with st.status("Processing your file...", expanded=True) as status:
                st.write("🔍 Filtering columns and removing duplicates...")
                
                # FIX: Added null_values and ignore_errors to solve the "NA" parsing error
                df = pl.read_csv(
                    "temp_input.csv", 
                    separator=sep, 
                    encoding="latin-1", 
                    columns=selected_columns,
                    ignore_errors=True,
                    null_values=["NA", "N/A", "null", "", " "], 
                    infer_schema_length=10000 
                )
                
                initial_rows = df.height
                df_unique = df.unique()
                final_rows = df_unique.height
                
                output_parquet = "cleaned_data.parquet"
                df_unique.write_parquet(output_parquet)
                
                status.update(label=f"✅ Success! Removed {initial_rows - final_rows:,} duplicates.", state="complete", expanded=False)
                st.balloons()

                # Preview the first 10 rows
                st.markdown("---")
                st.markdown("### 👀 Data Preview (Cleaned)")
                st.dataframe(df_unique.head(10).to_pandas(), use_container_width=True)

                with open(output_parquet, "rb") as f:
                    st.download_button(
                        label="Download Cleaned Parquet",
                        data=f,
                        file_name=f"{uploaded_file.name.replace('.csv', '')}_cleaned.parquet",
                        mime="application/octet-stream",
                        use_container_width=True
                    )
                    
    except Exception as e:
        st.error(f"Error: {e}")
    
    finally:
        if os.path.exists("temp_input.csv"):
            os.remove("temp_input.csv")
