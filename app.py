import streamlit as st
import polars as pl
import os

st.set_page_config(page_title="Deduplicator Pro3000xt", layout="wide")

st.title("🇨🇱 CSV Cleaner & Parquet Converter")
st.write("Upload a huge CSV to remove duplicates and convert to Parquet.")

uploaded_file = st.file_uploader("Drop your CSV here", type=["csv"])

if uploaded_file:
    # 1. Save uploaded file temporarily to disk to save RAM
    with open("temp_input.csv", "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.info("Step 1: Converting to Parquet & Deduplicating...")

    try:
        # We use scan_csv (Lazy loading) to handle files larger than RAM
        # Chilean CSVs often use ';' as a separator and 'latin-1' encoding
        lazy_df = pl.scan_csv("temp_input.csv", separator=";", encoding="latin-1", ignore_errors=True)
        
        # Perform deduplication (unique) while still in 'lazy' mode
        # This only calculates the plan, doesn't execute yet
        cleaned_df = lazy_df.unique()

        # 2. Execute and Save to Parquet
        output_parquet = "cleaned_data.parquet"
        cleaned_df.sink_parquet(output_parquet) # 'sink' is memory efficient for big data
        
        st.success("✅ Process Complete!")

        # 3. Download Buttons
        with open(output_parquet, "rb") as f:
            st.download_button(
                label="Download Cleaned Parquet (Small & Fast)",
                data=f,
                file_name="cleaned_data.parquet",
                mime="application/octet-stream"
            )
            
    except Exception as e:
        st.error(f"Error: {e}. Try changing the separator to a comma if it failed.")
    
    finally:
        # Cleanup temp files
        if os.path.exists("temp_input.csv"):
            os.remove("temp_input.csv")
