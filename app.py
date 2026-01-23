import uuid
import streamlit as st
import asyncio
import datetime
import pandas as pd
import json
import os
from pathlib import Path
import streamlit_mermaid as stmd
import zipfile
import io
import shutil

# Import step functions from the refactored source
from source import (
    PipelineState,
    step1_initialize_pipeline,
    step2_add_downloader,
    step3_configure_rate_limiting,
    step4_download_filings,
    step5_parse_documents,
    step6_deduplicate_documents,
    step7_chunk_text,
    step8_build_pipeline,
    step9_generate_report,
    normalize_parsed_tables,
    FILING_TYPES
)

# Page Config
st.set_page_config(page_title="QuLab: SEC EDGAR Pipeline", layout="wide")
st.sidebar.image("https://www.quantuniversity.com/assets/img/logo5.jpg")
st.sidebar.divider()
st.title("QuLab: SEC EDGAR Pipeline - Step by Step")
st.divider()

# Mermaid diagram template

SESSION_DIR = f"SESSION_FILES/{uuid.uuid4()}"


def get_mermaid_diagram(current_step):
    """Generate mermaid diagram with current step highlighted"""
    steps = {
        1: "Setup pipeline",
        2: "Add downloader",
        3: "Add rate limiting",
        4: "Download filings",
        5: "Parse documents",
        6: "Deduplicate",
        7: "Chunk text",
        8: "Build pipeline",
        9: "Generate report"
    }

    mermaid = "flowchart LR\n\n  %% Define Nodes with attached classes\n"
    for i in range(1, 10):
        class_name = "current" if i == current_step else f"c{i}"
        mermaid += f"  STEP{i}[{steps[i]}]:::{class_name}\n"

    mermaid += "\n  %% Define Connections\n"
    mermaid += "  STEP1 --> STEP2 --> STEP3 --> STEP4 --> STEP5 --> STEP6 --> STEP7 --> STEP8 --> STEP9\n\n"

    mermaid += "  %% Define Styles\n"
    for i in range(1, 10):
        if i == current_step:
            mermaid += f"  classDef current fill:#4CAF50,stroke:#333,stroke-width:3px,color:#fff\n"
        else:
            mermaid += f"  classDef c{i} fill:#ffcc99,stroke:#333,stroke-width:2px,color:#000\n"

    return mermaid


# Initialize session state
if 'pipeline_state' not in st.session_state:
    st.session_state.pipeline_state = None
if 'current_step' not in st.session_state:
    st.session_state.current_step = 1
if 'step_outputs' not in st.session_state:
    st.session_state.step_outputs = {}


step_options = {
    1: "1. Initialize Pipeline",
    2: "2. Add Downloader",
    3: "3. Rate Limiting",
    4: "4. Download Filings",
    5: "5. Parse Documents",
    6: "6. Deduplicate",
    7: "7. Chunk Text",
    8: "8. Build Pipeline",
    9: "9. Generate Report"
}

# Only allow jumping to completed steps or next step
available_steps = list(
    range(1, max(st.session_state.step_outputs.keys() or [0]) + 2))
available_steps = [s for s in available_steps if s <= 9]

if len(available_steps) >= 1:
    st.session_state.current_step = st.sidebar.selectbox(
        "Go to step",
        options=available_steps,
        format_func=lambda x: step_options[x],
        index=available_steps.index(
            st.session_state.current_step) if st.session_state.current_step in available_steps else 0,
        key="jump_to_step"
    )


# Sidebar Navigation
with st.sidebar:

    st.markdown("### 🎯 Key Objectives")
    st.markdown("""
    - **Remember:** List SEC filing types (10-K, 10-Q, 8-K, DEF-14A)
    - **Understand:** Explain why document chunking affects extraction quality
    - **Apply:** Implement SEC download and parsing pipeline
    - **Analyze:** Compare PDF vs HTML extraction strategies
    """)


# Main content area
st.markdown("## Introduction")
st.markdown("""
Your current mission is critical: to establish a robust, reliable, and scalable pipeline for ingesting 
public financial disclosures from the SEC EDGAR database. The platform relies on this data for everything 
from regulatory compliance and market research to sophisticated investment analysis and AI-driven insights.

The challenge lies in handling the volume and variety of SEC filings (10-K, 10-Q, 8-K, DEF 14A), adhering 
to SEC's API guidelines (especially rate limits), and ensuring data quality through deduplication and 
intelligent document segmentation.
""")

st.divider()

# =============================================================================
# STEP 1: Initialize Pipeline
# =============================================================================
if st.session_state.current_step == 1:
    st.markdown("# Step 1: Initialize Pipeline")
    stmd.st_mermaid(get_mermaid_diagram(1))

    st.markdown("""
    ### Setting Up the Foundation
    
    Before we begin processing SEC filings, we need to initialize the pipeline with your company information.
    The SEC requires this information to identify your application in their logs.
    """)

    col1, col2 = st.columns(2)

    with col1:
        company_name = st.text_input(
            "Company Name",
            value="QuantInsight Analytics",
            help="Your company/organization name for SEC User-Agent"
        )

    with col2:
        email_address = st.text_input(
            "Email Address",
            value="your_email@company.com",
            help="Your contact email for SEC User-Agent"
        )

    download_dir = "./sec_filings"

    st.markdown("---")

    if st.button("✅ Initialize Pipeline", key="step1_button", type="primary"):
        with st.spinner("Initializing pipeline..."):
            try:
                state = step1_initialize_pipeline(
                    company_name, email_address, download_dir)
                st.session_state.pipeline_state = state
                st.session_state.step_outputs[1] = {
                    "company_name": company_name,
                    "email_address": email_address,
                }

                st.rerun()

            except Exception as e:
                st.error(f"❌ Error initializing pipeline: {e}")

    if len(st.session_state.step_outputs) >= 1:
        st.success("✓ Pipeline initialized successfully! Proceed to Step 2.")
        st.session_state.current_step = 2


# =============================================================================
# STEP 2: Add Downloader
# =============================================================================
elif st.session_state.current_step == 2:
    st.markdown("# Step 2: Add SEC Downloader")
    stmd.st_mermaid(get_mermaid_diagram(2))

    st.markdown("""
    ### Architecting the Rate-Limited SEC Filing Downloader
    
    The SEC EDGAR system has strict rate limits to prevent abuse and ensure fair access for all users. 
    Our downloader will inherently respect these limits to maintain continuous and compliant data acquisition.
    
    The SEC generally advises approximately **10 requests per second**.
    """)

    # Show previous step info
    with st.expander("📋 Previous Step Info", expanded=False):
        st.json(st.session_state.step_outputs.get(1, {}))

    st.markdown("---")

    if st.button("✅ Add Downloader", key="step2_button", type="primary"):
        with st.spinner("Adding SEC downloader component..."):
            try:
                state = step2_add_downloader(st.session_state.pipeline_state)
                st.session_state.pipeline_state = state
                st.session_state.step_outputs[2] = {
                    "status": "SEC Downloader initialized",
                    "company_name": state.company_name,
                    "email_address": state.email_address
                }
                st.rerun()

            except Exception as e:
                st.error(f"❌ Error adding downloader: {e}")

    if len(st.session_state.step_outputs) >= 2:
        st.session_state.current_step = 3
        st.success("✓ SEC Downloader added successfully! Proceed to Step 3.")
# =============================================================================
# STEP 3: Configure Rate Limiting
# =============================================================================
elif st.session_state.current_step == 3:
    st.markdown("# Step 3: Configure Rate Limiting")
    stmd.st_mermaid(get_mermaid_diagram(3))

    st.markdown("""
    ### Rate Limiting Configuration
    
    To comply with SEC guidelines, we need to limit our request rate. The formula is:
    
    $$T_{delay} = \\frac{1}{R_{max}}$$
    
    where $R_{max}$ is the maximum allowed requests per second.
    """)

    # Show previous step info
    with st.expander("📋 Previous Steps Info", expanded=False):
        st.json(st.session_state.step_outputs)

    request_delay = st.slider(
        "Request Delay (seconds)",
        min_value=0.05,
        max_value=1.0,
        value=0.1,
        step=0.05,
        help="Delay between consecutive requests. 0.1s = 10 requests/second"
    )

    requests_per_second = 1 / request_delay
    st.info(f"📊 This allows **{requests_per_second:.1f} requests per second**")

    st.markdown("---")

    if st.button("✅ Configure Rate Limiting", key="step3_button", type="primary"):
        with st.spinner("Configuring rate limiting..."):
            try:
                state = step3_configure_rate_limiting(
                    st.session_state.pipeline_state, request_delay)
                st.session_state.pipeline_state = state
                st.session_state.step_outputs[3] = {
                    "request_delay": request_delay,
                    "requests_per_second": requests_per_second
                }
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error configuring rate limiting: {e}")

    if len(st.session_state.step_outputs) >= 3:
        st.session_state.current_step = 4
        st.success(
            f"✓ Rate limiting configured: {request_delay}s delay ({requests_per_second:.1f} req/s). Proceed to Step 4.")


# =============================================================================
# STEP 4: Download Filings
# =============================================================================
elif st.session_state.current_step == 4:
    st.markdown("# Step 4: Download SEC Filings")
    stmd.st_mermaid(get_mermaid_diagram(4))

    st.markdown("""
    ### Download Company Filings
    
    Now we'll download SEC filings for specific companies. Enter the CIK numbers (Company Identification Numbers)
    and select which filing types you want to retrieve.
    """)

    # Show previous step info
    with st.expander("📋 Previous Steps Info", expanded=False):
        st.json(st.session_state.step_outputs)

    st.markdown("### Filing Selection")

    col1, col2 = st.columns(2)

    with col1:
        ciks_input = st.text_input(
            "CIK",
            value="0000320193",
            help="Enter CIK number. Example: 0000320193 (Apple)"
        )

        filing_types = st.multiselect(
            "Filing Types",
            options=FILING_TYPES,
            default=["10-K"],
            help="Select the types of SEC filings to download"
        )

    with col2:
        after_date = st.date_input(
            "Download filings after",
            value=datetime.date(2023, 1, 1),
            help="Only download filings published after this date"
        )

        limit = st.number_input(
            "Max filings per type per CIK",
            min_value=1,
            max_value=5,
            value=2,
            help="Limit the number of filings to download"
        )

    # Parse CIKs
    ciks_list = [cik.strip() for cik in ciks_input.split('\n') if cik.strip()]

    st.markdown("---")

    st.info("Note: In this demo version, downloads are limited and truncated for speed.")

    if st.button("✅ Download Filings", key="step4_button", type="primary", disabled=not ciks_list or not filing_types):
        with st.spinner("Downloading filings... This may take a few minutes..."):
            try:
                state = asyncio.run(step4_download_filings(
                    st.session_state.pipeline_state,
                    ciks=ciks_list,
                    filing_types=filing_types,
                    after_date=after_date.strftime("%Y-%m-%d"),
                    limit=limit
                ))
                st.session_state.pipeline_state = state
                st.session_state.step_outputs[4] = {
                    "ciks": ciks_list,
                    "filing_types": filing_types,
                    "after_date": after_date.strftime("%Y-%m-%d"),
                    "limit": limit,
                    "downloaded_count": len(state.downloaded_filings)
                }
                st.rerun()

            except Exception as e:
                st.error(f"❌ Error downloading filings: {e}")

    if len(st.session_state.step_outputs) >= 4:
        st.success(
            f"✓ Downloaded {len(st.session_state.pipeline_state.downloaded_filings)} filings successfully!")

        # Show download summary
        if st.session_state.pipeline_state.downloaded_filings:
            df = pd.DataFrame(
                st.session_state.pipeline_state.downloaded_filings)
            st.dataframe(
                df[['cik', 'filing_type', 'accession_number', 'path']], width='stretch')

        # let the user download the files in a zip

        zip_buffer = io.BytesIO()

        # Use ZIP_DEFLATED to compress the files
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:

            # 1. Define the base directory and resolve it to an absolute path
            # This acts as the "root" of your zip structure
            base_dir = Path("./sec_filings").resolve()

            for filing in st.session_state.pipeline_state.downloaded_filings:
                file_path = Path(filing['path']).resolve()

                try:
                    # 2. Compute the relative path from the base directory
                    # This preserves the structure (e.g., "10-K/0000320193/...")
                    arcname = file_path.relative_to(base_dir)
                except ValueError:
                    # Fallback: if the file is outside the base_dir, just use the filename
                    arcname = file_path.name

                zf.write(file_path, arcname=str(arcname))

        zip_buffer.seek(0)

        st.download_button(
            label="📥 Download ZIP of All Filings",
            data=zip_buffer,
            file_name="sec_filings.zip",
            mime="application/zip"
        )

        st.session_state.current_step = 5
        st.success("✓ Filings downloaded successfully! Proceed to Step 5.")

# =============================================================================
# STEP 5: Parse Documents
# =============================================================================
elif st.session_state.current_step == 5:
    st.markdown("# Step 5: Parse Documents")
    stmd.st_mermaid(get_mermaid_diagram(5))

    st.markdown("""
    ### Crafting a Universal Document Parser
    
    SEC filings come in various formats (HTML, PDF, etc.). We'll extract clean text and structured tables 
    from each document, regardless of format.
    
    Our parser handles:
    - **HTML**: Using BeautifulSoup to extract text and tables
    - **PDF**: Using pdfplumber and PyMuPDF for robust extraction
    """)

    # Show previous step info
    with st.expander("📋 Previous Steps Info", expanded=False):
        st.json(st.session_state.step_outputs)

    state = st.session_state.pipeline_state
    st.info(f"📊 **{len(state.downloaded_filings)} filings** ready to parse")

    st.markdown("---")

    if st.button("✅ Parse Documents", key="step5_button", type="primary"):
        with st.spinner("Parsing documents... Extracting text and tables... This will take a few minutes..."):
            try:
                state = asyncio.run(step5_parse_documents(
                    st.session_state.pipeline_state))
                st.session_state.pipeline_state = state
                st.session_state.step_outputs[5] = {
                    "parsed_count": len(state.parsed_filings),
                    "parsing_errors": state.summary["parsing_errors"]
                }
                st.rerun()

            except Exception as e:
                st.error(f"❌ Error parsing documents: {e}")

    if len(st.session_state.step_outputs) >= 5:

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Sample Parsed PDF Content")
            sample_pdf_parsed = next((
                (f for f in st.session_state.pipeline_state.parsed_filings if f['path'].endswith('.pdf'))), None)
            if sample_pdf_parsed:
                # show original file content

                with st.expander("Original PDF Content"):
                    st.pdf(sample_pdf_parsed['path'])

                with st.expander(f"View PDF parsing output (truncated) ", expanded=False):
                    st.text(sample_pdf_parsed['parsed_text'][:1000] + "...")
                    st.caption(
                        f"Tables found: {len(sample_pdf_parsed.get('parsed_tables', []))}")

                # show 5 tables
                with st.expander("Extracted Tables Preview (Truncated)", expanded=False):
                    if sample_pdf_parsed and sample_pdf_parsed.get('parsed_tables'):
                        cleaned_tables = normalize_parsed_tables(
                            sample_pdf_parsed['parsed_tables'])
                        dfs = [
                            pd.DataFrame(t["rows"], columns=t["headers"]).assign(
                                page=t["page"])
                            for t in cleaned_tables
                        ]
                        count = 0

                        for i, df_table in enumerate(dfs):
                            if count >= 10:
                                break
                            try:
                                st.markdown(
                                    f"**Table {count+1}**")
                                st.dataframe(df_table.drop(
                                    columns=['page']), use_container_width=True)
                                count += 1
                            except Exception as e:
                                pass

                    else:
                        st.markdown("No tables extracted from the sample PDF.")
        with col2:

            st.markdown("### Sample Parsed HTML Content")

            sample_html_parsed = next((
                (f for f in st.session_state.pipeline_state.parsed_filings if f['path'].endswith('.htm') or f['path'].endswith('.html'))), None)
            if sample_html_parsed:
                with st.expander("Original HTML Content"):
                    with open(sample_html_parsed['path'], 'r', encoding='utf-8') as f:
                        html_content = f.read()
                    with st.container(height=500):
                        st.html(html_content)
                with st.expander(f"View HTML parsing output (truncated) ", expanded=False):

                    st.text(sample_html_parsed['parsed_text'][:1000] + "...")
                    st.caption(
                        f"Tables found: {len(sample_html_parsed.get('parsed_tables', []))}")

                # show 5 tables
                with st.expander("Extracted Tables Preview (Truncated)", expanded=False):
                    if sample_html_parsed and sample_html_parsed.get('parsed_tables'):
                        cleaned_tables = normalize_parsed_tables(
                            sample_html_parsed['parsed_tables'])
                        dfs = [
                            pd.DataFrame(t["rows"], columns=t["headers"]).assign(
                                page=t["page"])
                            for t in cleaned_tables
                        ]
                        count = 0
                        for i, df_table in enumerate(dfs):
                            if count >= 10:
                                break
                            try:
                                st.markdown(
                                    f"**Table {count+1}**")
                                st.dataframe(df_table.drop(
                                    columns=['page']), use_container_width=True)
                                count += 1
                            except Exception as e:
                                pass
                    else:
                        st.markdown(
                            "No tables extracted from the sample HTML.")
        st.success(
            f"✓ Parsed {len(st.session_state.pipeline_state.parsed_filings)} documents successfully!")

        if st.session_state.pipeline_state.summary["parsing_errors"] > 0:
            st.warning(
                f"⚠️ {st.session_state.pipeline_state.summary['parsing_errors']} parsing errors occurred")

        st.session_state.current_step = 6
        st.success("✓ Documents parsed successfully! Proceed to Step 6.")
# =============================================================================
# STEP 6: Deduplicate Documents
# =============================================================================
elif st.session_state.current_step == 6:
    st.markdown("# Step 6: Deduplicate Documents")
    stmd.st_mermaid(get_mermaid_diagram(6))

    st.markdown("""
    ### Building the Document Registry
    
    Duplicate data leads to wasted processing and skewed analytics. We use SHA-256 cryptographic hashing 
    to generate a unique "fingerprint" for each document's content.
    
    $$h = H(M)$$
    
    where:
    - $H$ is the SHA-256 hash function
    - $M$ is the document content
    - $h$ is the unique 256-bit hash value
    
    The probability of collision (two different documents with the same hash) is astronomically small.
    """)

    # Show previous step info
    with st.expander("📋 Previous Steps Info", expanded=False):
        st.json(st.session_state.step_outputs)

    state = st.session_state.pipeline_state
    st.info(
        f"📊 **{len(state.parsed_filings)} parsed filings** ready for deduplication")

    registry_file = f"./{SESSION_DIR}/registry.txt"

    os.makedirs(SESSION_DIR, exist_ok=True)
    st.markdown("---")

    if st.button("✅ Deduplicate Documents", key="step6_button", type="primary"):
        with st.spinner("Checking for duplicates..."):
            try:
                state = step6_deduplicate_documents(
                    st.session_state.pipeline_state, registry_file)
                st.session_state.pipeline_state = state
                st.session_state.step_outputs[6] = {
                    "unique_filings": len(state.deduplicated_filings),
                    "duplicates_skipped": state.summary["skipped_duplicates"],
                }
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error during deduplication: {e}")

    if len(st.session_state.step_outputs) >= 6:
        st.session_state.current_step = 7
        st.info(
            f"📊 **{len(st.session_state.pipeline_state.deduplicated_filings)} unique documents** | **{st.session_state.pipeline_state.summary['skipped_duplicates']} duplicates** removed.")

        st.success("✓ Documents deduplicated successfully! Proceed to Step 7.")

# =============================================================================
# STEP 7: Chunk Text
# =============================================================================
elif st.session_state.current_step == 7:
    st.markdown("# Step 7: Chunk Text")
    stmd.st_mermaid(get_mermaid_diagram(7))

    st.markdown("""
    ### Document Chunking Strategy
    
    Modern AI models have token limits. We split documents into smaller, context-rich chunks that:
    - Fit within model contexts (500-1000 tokens)
    - Retain meaningful information
    - Include overlap to preserve context across boundaries
    """)

    # Show previous step info
    with st.expander("📋 Previous Steps Info", expanded=False):
        st.json(st.session_state.step_outputs)

    state = st.session_state.pipeline_state
    st.info(
        f"📊 **{len(state.deduplicated_filings)} unique documents** ready for chunking")

    col1, col2 = st.columns(2)

    with col1:
        chunk_size = st.number_input(
            "Chunk Size",
            min_value=100,
            max_value=2000,
            value=750,
            step=50,
            help="Target size of each chunk in words"
        )

    with col2:
        chunk_overlap = st.number_input(
            "Chunk Overlap",
            min_value=0,
            max_value=int(chunk_size * 0.5),
            value=50,
            step=10,
            help="Number of words to overlap between chunks"
        )

    st.markdown("---")

    if st.button("✅ Chunk Documents", key="step7_button", type="primary"):
        with st.spinner("Chunking documents..."):
            try:
                state = asyncio.run(step7_chunk_text(
                    st.session_state.pipeline_state,
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap
                ))
                st.session_state.pipeline_state = state

                total_chunks = sum(f.get('num_chunks', 0)
                                   for f in state.chunked_filings)
                st.session_state.step_outputs[7] = {
                    "chunked_filings": len(state.chunked_filings),
                    "total_chunks": total_chunks,
                    "chunk_size": chunk_size,
                    "chunk_overlap": chunk_overlap
                }

                st.rerun()

            except Exception as e:
                st.error(f"❌ Error chunking documents: {e}")

    if len(st.session_state.step_outputs) >= 7:
        st.info(
            f"📊 **{len(st.session_state.pipeline_state.chunked_filings)} documents** chunked.")

        # Show chunk distribution
        if st.session_state.pipeline_state.chunked_filings:
            chunk_counts = [f['num_chunks']
                            for f in st.session_state.pipeline_state.chunked_filings]
            df_chunks = pd.DataFrame({
                'CIK': [f['cik'] for f in st.session_state.pipeline_state.chunked_filings],
                'Filing Type': [f['filing_type'] for f in st.session_state.pipeline_state.chunked_filings],
                'Chunks': chunk_counts
            })
            st.dataframe(df_chunks, use_container_width=True)

        st.success(f"✓ Chunking complete! Proceed to Step 8.")
        st.session_state.current_step = 8

# =============================================================================
# STEP 8: Build Pipeline
# =============================================================================
elif st.session_state.current_step == 8:
    st.markdown("# Step 8: Build Pipeline")
    stmd.st_mermaid(get_mermaid_diagram(8))

    st.markdown("""
    ### Pipeline Validation
    
    Before generating the final report, we'll validate that all pipeline components have completed successfully
    and the data is ready for export.
    """)

    # Show all step outputs
    with st.expander("📋 Complete Pipeline Summary", expanded=True):
        for step_num, output in st.session_state.step_outputs.items():
            st.markdown(f"**Step {step_num}**")
            if step_num == 9:
                output["filings"] = '[Redacted]'
            st.json(output)
            st.markdown("---")

    st.markdown("---")

    if st.button("✅ Validate Pipeline", key="step8_button", type="primary"):
        with st.spinner("Validating pipeline..."):
            try:
                state = step8_build_pipeline(st.session_state.pipeline_state)
                st.session_state.pipeline_state = state

                total_chunks = sum(f.get('num_chunks', 0)
                                   for f in state.chunked_filings)
                st.session_state.step_outputs[8] = {
                    "validation_status": "passed",
                    "total_filings_processed": len(state.chunked_filings),
                    "total_chunks_created": total_chunks,
                    "duplicates_skipped": state.summary['skipped_duplicates'],
                    "parsing_errors": state.summary['parsing_errors']
                }
                st.rerun()

            except Exception as e:
                st.error(f"❌ Pipeline validation failed: {e}")

    if len(st.session_state.step_outputs) >= 8:

        # Show validation summary
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Filings Processed", len(
            st.session_state.pipeline_state.chunked_filings))
        total_chunks = sum(f.get('num_chunks', 0)
                           for f in st.session_state.pipeline_state.chunked_filings)
        col2.metric("Total Chunks", total_chunks)
        col3.metric("Duplicates Skipped",
                    st.session_state.pipeline_state.summary['skipped_duplicates'])
        col4.metric(
            "Errors", st.session_state.pipeline_state.summary['parsing_errors'])

        st.session_state.current_step = 9
        st.success("✅ Pipeline validation passed! Proceed to Step 9.")

# =============================================================================
# STEP 9: Generate Report
# =============================================================================
elif st.session_state.current_step == 9:
    st.markdown("# Step 9: Generate Report and Export Files")
    stmd.st_mermaid(get_mermaid_diagram(9))

    st.markdown("""
    ### Final Report Generation
    
    The pipeline is complete! Now we'll generate a comprehensive report and export all processed data
    to downloadable files.
    """)

    # Show validation summary
    with st.expander("📋 Pipeline Validation Summary", expanded=False):
        st.json(st.session_state.step_outputs.get(8, {}))

    output_dir = f"./{SESSION_DIR}"

    st.markdown("---")

    if st.button("✅ Generate Report", key="step9_button", type="primary"):
        with st.spinner("Generating report and exporting files..."):
            try:
                report = step9_generate_report(
                    st.session_state.pipeline_state, output_dir)
                st.session_state.step_outputs[9] = report

                st.success("✅ Report generated successfully!")

                # Display final summary
                st.markdown("## 📊 Final Pipeline Summary")

                col1, col2, col3, col4 = st.columns(4)
                col1.metric(
                    "Unique Filings", report['pipeline_summary']['unique_filings_processed'])
                col2.metric("Total Chunks",
                            report['pipeline_summary']['total_chunks'])
                col3.metric("Duplicates Skipped",
                            report['pipeline_summary']['skipped_duplicates'])
                col4.metric(
                    "Errors", report['pipeline_summary']['parsing_errors'])

                # Show filing details
                st.markdown("### 📄 Processed Filings")
                if report['filings']:
                    df_filings = pd.DataFrame([{
                        'CIK': f['cik'],
                        'Filing Type': f['filing_type'],
                        'Accession #': f['accession_number'],
                        'Chunks': f['num_chunks'],
                        'Tables': f['num_tables']
                    } for f in report['filings']])
                    st.dataframe(df_filings, use_container_width=True)

                # Download buttons
                st.markdown("### 💾 Download Files")

                output_path = Path(output_dir)

                # Download report JSON
                report_file = output_path / "pipeline_report.json"
                if report_file.exists():
                    with open(report_file, 'r') as f:
                        report_json = f.read()
                    st.download_button(
                        label="📥 Download Full Report (JSON)",
                        data=report_json,
                        file_name="pipeline_report.json",
                        mime="application/json"
                    )

                # Download summary text
                summary_file = output_path / "summary.txt"
                if summary_file.exists():
                    with open(summary_file, 'r') as f:
                        summary_text = f.read()
                    st.download_button(
                        label="📥 Download Summary (TXT)",
                        data=summary_text,
                        file_name="summary.txt",
                        mime="text/plain"
                    )

                # Show file tree
                st.markdown("### 📁 Generated Files")
                if output_path.exists():
                    files = list(output_path.glob("*.json")) + \
                        list(output_path.glob("*.txt"))
                    file_tree = "```\n"
                    file_tree += f"📂 {output_dir}/\n"
                    for file in files:
                        size = file.stat().st_size
                        size_str = f"{size / 1024:.1f} KB" if size > 1024 else f"{size} B"
                        file_tree += f"  📄 {file.name} ({size_str})\n"
                    file_tree += "```"
                    st.markdown(file_tree)

                # Download zip file
                st.markdown("### 📦 Download All Files (Zip)")

                # Create the zip archive in memory-safe way
                zip_base_name = f"{output_dir}/session_bundle"

                # make_archive creates 'session_bundle.zip'
                zip_path = shutil.make_archive(
                    zip_base_name, 'zip', output_dir)

                # Read the zip file into bytes
                with open(zip_path, "rb") as f:
                    zip_data = f.read()

                # Delete the generated zip file from disk now that it's in memory
                # (We don't want it inside the folder if the user resets)
                if os.path.exists(zip_path):
                    os.remove(zip_path)

                def cleanup_session_folder():
                    """Callback to delete the folder after download is clicked."""
                    print("Callback is called")
                    if os.path.exists(output_dir):
                        try:
                            shutil.rmtree(output_dir)
                            # Using st.toast to notify user without blocking
                            st.toast(
                                f"🗑️ Cleanup successful: {output_dir} deleted.")
                        except Exception as e:
                            print(f"Error cleaning up: {e}")

                # Display the Download Button
                st.download_button(
                    label="📥 Download Complete Session (.zip)",
                    data=zip_data,
                    file_name="session_output.zip",
                    mime="application/zip",
                    on_click=cleanup_session_folder,
                    help="Download all generated reports and JSON files in a single archive."
                )

                st.markdown("---")
                st.success(
                    "🎉 **Pipeline Complete!** All steps executed successfully.")

            except Exception as e:
                st.error(f"❌ Error generating report: {e}")

# Footer
st.markdown("---")
st.caption("""
## QuantUniversity License

© QuantUniversity 2025  
This application was created for **educational purposes only** and is **not intended for commercial use**.  

- You **may not copy, share, or redistribute** this application **without explicit permission** from QuantUniversity.  
- Content generated may contain **errors**. Please **verify before using**.  

All rights reserved. For permissions or commercial licensing, contact: [info@qusandbox.com](mailto:info@qusandbox.com)
""")
