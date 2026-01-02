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
    enc = st.selectbox("Encoding", ["utf-8", "latin-1"], index=1, help="Use 'latin-1' if you see weird characters with accents/ñ")

uploaded_file = st.file_uploader("Upload your CSV", type=["csv"])

if uploaded_file:
    # Save to disk to avoid keeping the whole byte-stream in RAM
    with st.status("Processing your file...", expanded=True) as status:
        st.write("💾 Saving file to temporary storage...")
        with open("temp_input.csv", "wb") as f:
            f.write(uploaded_file.getbuffer())

        st.write("🔍 Deduplicating and converting (Lazy Mode)...")
        try:
            # Lazy Scan: Does not load file into RAM yet
            lazy_plan = pl.scan_csv("temp_input.csv", separator=sep, encoding=enc, ignore_errors=True)
            
            # Deduplicate (Full row match)
            unique_plan = lazy_plan.unique()

            # Sink to Parquet: Streams data from CSV -> unique -> Parquet on disk
            output_parquet = "cleaned_data.parquet"
            unique_plan.sink_parquet(output_parquet)
            
            status.update(label="✅ Success!", state="complete", expanded=False)
            st.balloons()

            # Provide the Parquet file for download
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
            # Cleanup
            if os.path.exists("temp_input.csv"):
                os.remove("temp_input.csv")
