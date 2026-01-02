import streamlit as st
import polars as pl
import os

st.set_page_config(page_title="Deduplicator Pro", page_icon="🇨🇱")

st.title("🇨🇱 CSV Cleaner & Parquet Converter")
st.markdown("### Optimized for large files (up to 3GB)")

# Options for Chilean CSV formats
col1, col2 = st.columns(2)
with col1:
    sep = st.selectbox("CSV Separator", [";", ","], help="Chilean files often use semicolon (;)")
with col2:
    st.info("Note: System auto-handles Chilean characters (ñ, á, etc.) during processing.")

uploaded_file = st.file_uploader("Upload your CSV", type=["csv"])

if uploaded_file:
    # 1. PEAK AT HEADERS: We save the file first to read the header efficiently
    with open("temp_input.csv", "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    try:
        # Get column names without loading the whole file
        all_columns = pl.read_csv("temp_input.csv", separator=sep, encoding="latin-1", n_rows=0).columns
        
        st.markdown("---")
        st.subheader("🎯 Column Selection")
        selected_columns = st.multiselect(
            "Which columns do you want to keep?", 
            options=all_columns, 
            default=all_columns,
            help="Removing unnecessary columns will make your Parquet file much smaller!"
        )

        if not selected_columns:
            st.warning("Please select at least one column to proceed.")
            st.stop()

        if st.button("🚀 Process and Clean Data", use_container_width=True):
            with st.status("Processing your file...", expanded=True) as status:
                st.write("🔍 Loading selected columns and deduplicating...")
                
                # 2. FULL PROCESS: Read ONLY the selected columns to save RAM
                df = pl.read_csv(
                    "temp_input.csv", 
                    separator=sep, 
                    encoding="latin-1", 
                    columns=selected_columns, # <--- This is the magic part
                    ignore_errors=True,
                    infer_schema_length=10000
                )
                
                initial_rows = df.height
                df_unique = df.unique()
                final_rows = df_unique.height
                
                output_parquet = "cleaned_data.parquet"
                df_unique.write_parquet(output_parquet)
                
                status.update(label=f"✅ Success! Removed {initial_rows - final_rows:,} duplicates.", state="complete", expanded=False)
                st.balloons()

                # Preview Section
                st.markdown("---")
                st.markdown("### 👀 Data Preview (Cleaned)")
                st.dataframe(df_unique.head(10).to_pandas(), use_container_width=True)
                st.markdown("---")

                with open(output_parquet, "rb") as f:
                    st.download_button(
                        label="Download Cleaned Parquet",
                        data=f,
                        file_name=f"{uploaded_file.name.replace('.csv', '')}_cleaned.parquet",
                        mime="application/octet-stream",
                        use_container_width=True
                    )
                    
    except Exception as e:
        st.error(f"Error processing file: {e}")
    
    finally:
        # Cleanup temp file if it exists
        if os.path.exists("temp_input.csv"):
            os.remove("temp_input.csv")
