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
    # Removed latin-1 from the direct scan to avoid the sink_parquet error
    st.info("Note: System will auto-handle Chilean characters (ñ, á, etc.) during processing.")

uploaded_file = st.file_uploader("Upload your CSV", type=["csv"])

if uploaded_file:
    with st.status("Processing your file...", expanded=True) as status:
        st.write("💾 Saving file to temporary storage...")
        with open("temp_input.csv", "wb") as f:
            f.write(uploaded_file.getbuffer())

        st.write("🔍 Deduplicating and converting (Safe Mode)...")
        try:
            # FIX: We use read_csv with low_memory=False for better encoding handling, 
            # but we immediately drop duplicates to keep RAM usage down.
            # This is slightly more RAM-intensive than 'sink', but fixes the encoding error.
            df = pl.read_csv(
                "temp_input.csv", 
                separator=sep, 
                encoding="latin-1", # This handles the Chilean characters correctly
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
            st.write("Try switching the separator if the error persists.")
        
        finally:
            if os.path.exists("temp_input.csv"):
                os.remove("temp_input.csv")
