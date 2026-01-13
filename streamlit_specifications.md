
# Streamlit Application Specification: SEC EDGAR Data Pipeline

## 1. Application Overview

### Purpose of the Application

The SEC EDGAR Data Pipeline Streamlit application is designed as a development blueprint for "PE ORG-AIR System" simulating a Software Developer's workflow for ingesting, processing, and analyzing public financial disclosures from the SEC EDGAR database. It provides an interactive interface to configure, execute, and monitor an end-to-end data pipeline, demonstrating concepts like rate-limited downloading, content-hash-based deduplication, universal document parsing (HTML/PDF), and smart document chunking for AI readiness.

### High-Level Story Flow

The application guides the user through the following stages, mirroring a real-world data engineering task:

1.  **Introduction:** A welcoming page explaining the importance of the pipeline for financial intelligence.
2.  **Configuration & Setup:** The user defines global pipeline parameters, such as user agent details for SEC access, download directories, and chunking strategies. This initializes the core pipeline components.
3.  **Download & Process Filings:** The user specifies Company Identification Numbers (CIKs), desired filing types (e.g., 10-K, 10-Q), and date filters. The application then executes the rate-limited download, parsing, deduplication, and chunking process in the background, providing real-time status updates.
4.  **Processed Filings & Analysis:** Upon completion, the application presents a comprehensive report summarizing the pipeline's execution, including counts of attempted, unique, skipped (duplicates), and failed filings. Users can delve into the details of each filing and, for successful ones, view the extracted raw text and the generated AI-ready chunks.

This flow emphasizes practical application of the concepts by allowing the developer to control the inputs and observe the pipeline's output and efficiency.

---

## 2. Code Requirements

### Import Statement

```python
import streamlit as st
import asyncio
from source import SECDownloader, DocumentRegistry, DocumentParser, DocumentChunker, SECPipeline
import datetime
import pandas as pd
import os
```

### `st.session_state` Design

The following `st.session_state` keys will be used to maintain state across user interactions and page changes:

*   `st.session_state.current_page`: `str` - Controls which section of the application is displayed (e.g., 'Home', 'Configuration', 'Download Filings', 'Processed Filings & Analysis'). Initialized to 'Home'.
*   `st.session_state.company_name`: `str` - User's company name for SEC User-Agent. Initialized to "QuantInsight Analytics".
*   `st.session_state.email_address`: `str` - User's email for SEC User-Agent. Initialized to "your_email@quantinsight.com".
*   `st.session_state.download_dir`: `str` - Local directory for SEC filings. Initialized to "./sec_filings".
*   `st.session_state.registry_file`: `str` - Path to the document registry file. Initialized to "document_registry.txt".
*   `st.session_state.chunk_size`: `int` - Target word count for document chunks. Initialized to 750.
*   `st.session_state.chunk_overlap`: `int` - Word overlap between document chunks. Initialized to 50.
*   `st.session_state.pipeline_initialized`: `bool` - True if `SECPipeline` has been successfully initialized. Initialized to `False`.
*   `st.session_state.sec_pipeline_instance`: `SECPipeline` - The instantiated pipeline object. Initialized to `None`.
*   `st.session_state.ciks_input`: `str` - Raw string input for CIKs. Initialized to "0000320193\n0000789019".
*   `st.session_state.filing_types_selected`: `list` - List of selected filing types. Initialized to `["10-K", "10-Q"]`.
*   `st.session_state.after_date_input`: `datetime.date` - Date filter for filings. Initialized to `datetime.date(2022, 1, 1)`.
*   `st.session_state.limit_input`: `int` - Max filings per type per CIK. Initialized to 2.
*   `st.session_state.processing_in_progress`: `bool` - Flag to indicate pipeline is running. Initialized to `False`.
*   `st.session_state.pipeline_report`: `dict` - Stores the result from `SECPipeline.run_pipeline`. Initialized to `None`.
*   `st.session_state.selected_filing_for_view`: `dict` - The metadata of a filing selected by the user for detailed viewing (raw text, chunks). Initialized to `None`.
*   `st.session_state.view_text`: `str` - Extracted raw text for the `selected_filing_for_view`. Initialized to `None`.
*   `st.session_state.view_chunks`: `list` - Generated chunks for the `selected_filing_for_view`. Initialized to `None`.

### Application Structure and Flow

The application will use a sidebar for navigation and `st.session_state.current_page` to conditionally render content.

**Initialization (`app.py` top-level)**

```python
# Set Streamlit page configuration
st.set_page_config(layout="wide", page_title="SEC EDGAR Data Pipeline")

# Initialize session state variables if not already present
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'Home'
if 'company_name' not in st.session_state:
    st.session_state.company_name = "QuantInsight Analytics"
if 'email_address' not in st.session_state:
    st.session_state.email_address = "your_email@quantinsight.com"
if 'download_dir' not in st.session_state:
    st.session_state.download_dir = "./sec_filings"
if 'registry_file' not in st.session_state:
    st.session_state.registry_file = "document_registry.txt"
if 'chunk_size' not in st.session_state:
    st.session_state.chunk_size = 750
if 'chunk_overlap' not in st.session_state:
    st.session_state.chunk_overlap = 50
if 'pipeline_initialized' not in st.session_state:
    st.session_state.pipeline_initialized = False
if 'sec_pipeline_instance' not in st.session_state:
    st.session_state.sec_pipeline_instance = None
if 'ciks_input' not in st.session_state:
    st.session_state.ciks_input = "0000320193\n0000789019" # Apple, Microsoft
if 'filing_types_selected' not in st.session_state:
    st.session_state.filing_types_selected = ["10-K", "10-Q"]
if 'after_date_input' not in st.session_state:
    st.session_state.after_date_input = datetime.date(2022, 1, 1)
if 'limit_input' not in st.session_state:
    st.session_state.limit_input = 2
if 'processing_in_progress' not in st.session_state:
    st.session_state.processing_in_progress = False
if 'pipeline_report' not in st.session_state:
    st.session_state.pipeline_report = None
if 'selected_filing_for_view' not in st.session_state:
    st.session_state.selected_filing_for_view = None
if 'view_text' not in st.session_state:
    st.session_state.view_text = None
if 'view_chunks' not in st.session_state:
    st.session_state.view_chunks = None

# Sidebar for navigation
with st.sidebar:
    st.title("SEC EDGAR Pipeline")
    st.session_state.current_page = st.radio(
        "Navigation",
        ['Home', 'Configuration & Setup', 'Download & Process Filings', 'Processed Filings & Analysis'],
        index=['Home', 'Configuration & Setup', 'Download & Process Filings', 'Processed Filings & Analysis'].index(st.session_state.current_page)
    )
```

**Page: Home**

```python
if st.session_state.current_page == 'Home':
    st.markdown(f"# SEC EDGAR Data Pipeline: A Developer's Workflow for Financial Data Ingestion")
    st.markdown(f"## Introduction: Building the Foundation for Financial Intelligence")
    st.markdown(f"Welcome to the heart of financial data infrastructure! As a Software Developer at \"QuantInsight Analytics,\" a cutting-edge financial data analytics firm, your current mission is critical: to establish a robust, reliable, and scalable pipeline for ingesting public financial disclosures from the SEC EDGAR database. Our firm relies on this data for everything from regulatory compliance and market research to sophisticated investment analysis and AI-driven insights.")
    st.markdown(f"")
    st.markdown(f"The challenge lies in handling the volume and variety of SEC filings (10-K, 10-Q, 8-K, DEF 14A), adhering to SEC's API guidelines (especially rate limits), and ensuring data quality through deduplication and intelligent document segmentation. This application will walk you through building the core components of this pipeline, demonstrating how each step addresses a real-world requirement for QuantInsight Analytics. You will implement a rate-limited downloader, a content-hash-based registry for deduplication, a versatile parser for HTML and PDF filings, and a smart chunking mechanism for AI readiness.")
    st.markdown(f"")
    st.markdown(f"Your work here lays the groundwork for all downstream applications, ensuring that our analysts and AI models always have access to clean, non-redundant, and properly formatted financial data.")
    st.markdown(f"---")
    st.markdown(f"## Key Objectives")
    st.markdown(f"""
    - **Remember:** List SEC filing types (10-K, 10-Q, 8-K, DEF-14A)
    - **Understand:** Explain why document chunking affects extraction quality
    - **Apply:** Implement SEC download and parsing pipeline
    - **Analyze:** Compare PDF vs HTML extraction strategies
    """)
    st.markdown(f"---")
    st.markdown(f"## Tools Introduced")
    st.markdown(f"""
    | Tool                 | Purpose             | Why This Tool                 |
    | :------------------- | :------------------ | :---------------------------- |
    | `sec-edgar-downloader` | Filing retrieval    | Official SEC API wrapper      |
    | `pdfplumber`         | PDF extraction      | Accurate table extraction     |
    | `pymupdf (fitz)`     | PDF processing      | Fast, memory-efficient        |
    | `httpx`              | HTTP client         | Async support, modern API     |
    """)
    st.markdown(f"---")
    st.markdown(f"## Key Concepts")
    st.markdown(f"""
    - SEC filing types and their AI-relevance
    - Document chunking strategies
    - Content deduplication
    - Rate limiting for SEC API
    """)

    st.markdown(f"---")
    st.markdown(f"Ready to configure your pipeline?")
    if st.button("Go to Configuration"):
        st.session_state.current_page = 'Configuration & Setup'
        st.experimental_rerun()
```

**Page: Configuration & Setup**

```python
if st.session_state.current_page == 'Configuration & Setup':
    st.markdown(f"# 1. Setting Up the SEC EDGAR Data Pipeline")
    st.markdown(f"Before we dive into building the pipeline, let's configure the essential parameters. These settings ensure we comply with SEC guidelines and prepare our environment for efficient data processing.")
    st.markdown(f"")
    st.markdown(f"## Architecting the Rate-Limited SEC Filing Downloader")
    st.markdown(f"As a Software Developer at QuantInsight Analytics, the first critical component of our pipeline is a reliable SEC filing downloader. The SEC EDGAR system has strict rate limits to prevent abuse and ensure fair access for all users. Ignoring these limits can lead to our IP address being temporarily or permanently blocked, completely disrupting our data flow. Our task is to build a downloader that inherently respects these limits, ensuring a continuous and compliant data acquisition process. The SEC generally advises approximately 10 requests per second.")
    st.markdown(r"To be safe and compliant, we will introduce a delay $T_{{delay}}$ between consecutive download requests. If the desired rate limit is $R$ requests per second, then the minimum delay needed between requests is $T_{{delay}} = \frac{{1}}{{R}}$ seconds. For example, if $R = 0.1$ requests/second (i.e., 1 request every 10 seconds to be very conservative, or $R=10$ requests/second, meaning $T_{{delay}}=0.1$ seconds), we ensure we stay within the guidelines.")
    st.markdown(r"$$ T_{{delay}} = \frac{{1}}{{R_{{max}}}} $$")
    st.markdown(r"where $R_{{max}}$ is the maximum allowed requests per second. For SEC EDGAR, we will aim for a conservative delay of 0.1 seconds (10 requests/second).")
    st.markdown(f"---")
    
    st.markdown(f"### Pipeline Configuration Parameters")
    st.session_state.company_name = st.text_input(
        "Your Company Name (for SEC User-Agent)", 
        st.session_state.company_name, 
        help="The SEC requires a user agent string to identify your application. E.g., 'QuantInsight Analytics'"
    )
    st.session_state.email_address = st.text_input(
        "Your Email Address (for SEC User-Agent)", 
        st.session_state.email_address,
        help="Your email address for SEC User-Agent. E.g., 'your_email@quantinsight.com'"
    )
    st.session_state.download_dir = st.text_input(
        "Local Download Directory", 
        st.session_state.download_dir, 
        help="Path where SEC filings will be stored locally. E.g., './sec_filings'"
    )
    st.session_state.registry_file = st.text_input(
        "Document Registry File", 
        st.session_state.registry_file, 
        help="File path to store processed document hashes for deduplication. E.g., 'document_registry.txt'"
    )

    st.markdown(f"### Document Chunking Strategy")
    st.markdown(f"Modern AI models, especially Large Language Models (LLMs), have token limits. Sending an entire, multi-page SEC filing to an LLM is often infeasible and inefficient. Moreover, smaller, context-rich chunks lead to better retrieval and generation quality. For QuantInsight Analytics, our AI applications need document segments that are small enough to fit within model contexts (e.g., 500-1000 tokens) yet large enough to retain meaningful information and context.")
    st.markdown(f"")
    st.markdown(f"This process, known as 'chunking,' involves splitting a long document into smaller, manageable pieces. A simple yet effective strategy is fixed-size chunking, where we split the text based on a word or token count, ensuring some overlap between chunks to preserve context across boundaries.")
    st.markdown(r"If a document has $TotalTokens$ and we want chunks of $ChunkSize$ tokens with an $OverlapSize$ tokens, the number of chunks $N_{{chunks}}$ can be roughly calculated as:")
    st.markdown(r"$$ N_{{chunks}} = \lceil \frac{{TotalTokens - OverlapSize}}{{ChunkSize - OverlapSize}} \rceil $$")
    st.markdown(r"For simplicity, we will implement fixed-size chunking based on word count (as a proxy for tokens) with no explicit overlap for this demonstration, aiming for chunks between 500-1000 words.")

    st.session_state.chunk_size = st.number_input(
        "Chunk Size (words)", 
        min_value=100, 
        max_value=2000, 
        value=st.session_state.chunk_size, 
        step=50,
        help="The target size of each document chunk in words (proxy for tokens)."
    )
    st.session_state.chunk_overlap = st.number_input(
        "Chunk Overlap (words)", 
        min_value=0, 
        max_value=int(st.session_state.chunk_size * 0.5), # Ensure overlap is less than chunk size
        value=st.session_state.chunk_overlap, 
        step=10,
        help="The number of words to overlap between consecutive chunks to maintain context."
    )

    if st.button("Initialize Pipeline Components"):
        try:
            # Create download directory if it doesn't exist
            os.makedirs(st.session_state.download_dir, exist_ok=True)
            
            # Instantiate the SECPipeline with configured parameters
            pipeline_instance = SECPipeline(
                download_dir=st.session_state.download_dir,
                registry_file=st.session_state.registry_file,
                chunk_size=st.session_state.chunk_size,
                chunk_overlap=st.session_state.chunk_overlap,
                # Pass company_name and email_address to the SECDownloader constructor via pipeline
                company_name=st.session_state.company_name, 
                email_address=st.session_state.email_address
            )
            st.session_state.sec_pipeline_instance = pipeline_instance
            st.session_state.pipeline_initialized = True
            st.success("Pipeline components initialized successfully!")
            st.session_state.current_page = 'Download & Process Filings'
            st.experimental_rerun()
        except Exception as e:
            st.error(f"Error initializing pipeline: {e}")
```

**Page: Download & Process Filings**

```python
if st.session_state.current_page == 'Download & Process Filings':
    st.markdown(f"# 2. Download and Process SEC Filings")
    st.markdown(f"Here, you'll specify the companies and filing types to ingest. The pipeline will download, parse, deduplicate, and chunk these documents.")
    st.markdown(f"")

    if not st.session_state.pipeline_initialized or st.session_state.sec_pipeline_instance is None:
        st.warning("Pipeline not initialized. Please go to 'Configuration & Setup' first.")
        if st.button("Go to Configuration & Setup"):
            st.session_state.current_page = 'Configuration & Setup'
            st.experimental_rerun()
    else:
        st.markdown(f"### Filing Selection")
        st.markdown(f"Enter CIKs (Company Identification Numbers), one per line.")
        st.session_state.ciks_input = st.text_area(
            "CIKs (e.g., 0000320193 for Apple Inc.)", 
            st.session_state.ciks_input, 
            height=100,
            help="Enter one CIK per line. The CIKs will be cleaned to remove non-digit characters."
        )
        # Clean CIKs: remove whitespace and non-digit characters, filter empty strings
        ciks_list = [cik.strip() for cik in st.session_state.ciks_input.split('\n') if cik.strip()]
        ciks_list = [re.sub(r'\D', '', cik) for cik in ciks_list if re.sub(r'\D', '', cik)] # Remove non-digits
        
        st.session_state.filing_types_selected = st.multiselect(
            "Select Filing Types", 
            options=SECDownloader.FILING_TYPES, 
            default=st.session_state.filing_types_selected,
            help="Choose the SEC filing types (e.g., 10-K, 10-Q) to download."
        )
        st.session_state.after_date_input = st.date_input(
            "Download filings after date (YYYY-MM-DD)", 
            st.session_state.after_date_input,
            help="Only download filings published after this date."
        )
        st.session_state.limit_input = st.number_input(
            "Max number of filings per type per CIK", 
            min_value=1, 
            max_value=100, 
            value=st.session_state.limit_input, 
            step=1,
            help="Controls the number of most recent filings of each type to download for each CIK."
        )

        st.markdown(f"---")
        st.markdown(f"## Building the Document Registry for Deduplication")
        st.markdown(f"Duplicate data is a common headache in data pipelines, leading to wasted processing, skewed analytics, and increased storage costs. For QuantInsight Analytics, processing the same SEC filing multiple times would be inefficient and could lead to inconsistencies in our financial models. To combat this, we need a robust mechanism to identify and prevent the reprocessing of identical documents.")
        st.markdown(f"")
        st.markdown(f"The solution is a \"Document Registry\" that uses cryptographic hashing to generate a unique \"fingerprint\" for each document's content. We will use the SHA-256 algorithm, which takes an input (the document's text content) and produces a fixed-size, unique hash value. The probability of two different documents producing the same SHA-256 hash is astronomically small, making it ideal for deduplication.")
        st.markdown(r"Mathematically, a cryptographic hash function $H$ maps an arbitrary-size input $M$ (our document content) to a fixed-size output $h$ (the hash value), such that:")
        st.markdown(r"1. It is easy to compute $h = H(M)$.")
        st.markdown(r"2. It is computationally infeasible to invert $H$ (find $M$ from $h$).")
        st.markdown(r"3. It is computationally infeasible to find two different inputs $M_1 \neq M_2$ such that $H(M_1) = H(M_2)$ (collision resistance).")
        st.markdown(r"For SHA-256, the output is a 256-bit (64-character hexadecimal) string. We will store these hashes in our registry to quickly check if a document has already been processed.")
        st.markdown(f"---")
        st.markdown(f"## Crafting a Universal Document Parser (HTML & PDF)")
        st.markdown(f"SEC filings come in various formats, primarily HTML and occasionally PDF, each with its own structural nuances. To make this raw data useful for QuantInsight Analytics' AI models and analysts, we need to extract clean, readable text and structured tables, regardless of the original document format. This task is crucial for downstream natural language processing (NLP) and quantitative analysis.")
        st.markdown(f"")
        st.markdown(f"A robust `DocumentParser` needs to:")
        st.markdown(f"*   **HTML Parsing**: Navigate the often complex and inconsistently structured HTML of SEC filings, focusing on extracting the main textual content and identifying tables. Libraries like `BeautifulSoup` excel at this by providing an intuitive way to traverse the HTML DOM.")
        st.markdown(f"*   **PDF Parsing**: Handle PDF documents, which can be challenging due to their visual layout rather than semantic structure. Tools like `pdfplumber` or `PyMuPDF` (Fitz) are essential for extracting text, tables, and even layout information from PDFs.")
        st.markdown(f"")
        st.markdown(f"The goal is to produce \"clean text output\" – free of HTML tags, scripts, and extraneous formatting – and separate, structured tabular data.")
        st.markdown(f"---")

        if st.button("Run SEC Data Pipeline", disabled=st.session_state.processing_in_progress or not ciks_list):
            if not ciks_list:
                st.error("Please enter at least one CIK.")
            else:
                st.session_state.processing_in_progress = True
                st.session_state.pipeline_report = None
                st.session_state.selected_filing_for_view = None
                st.session_state.view_text = None
                st.session_state.view_chunks = None
                
                with st.spinner("Running pipeline... This may take a while depending on CIKs and limits."):
                    try:
                        report = asyncio.run(st.session_state.sec_pipeline_instance.run_pipeline(
                            ciks=ciks_list,
                            filing_types=st.session_state.filing_types_selected,
                            after_date=st.session_state.after_date_input.strftime("%Y-%m-%d"),
                            limit=st.session_state.limit_input
                        ))
                        st.session_state.pipeline_report = report
                        st.success("Pipeline execution completed!")
                        st.session_state.current_page = 'Processed Filings & Analysis'
                    except Exception as e:
                        st.error(f"Pipeline execution failed: {e}")
                st.session_state.processing_in_progress = False
                st.experimental_rerun()
```

**Page: Processed Filings & Analysis**

```python
if st.session_state.current_page == 'Processed Filings & Analysis':
    st.markdown(f"# 3. Orchestrating the End-to-End SEC Data Pipeline and Reporting")
    st.markdown(f"Now that we have all the individual components – the rate-limited downloader, the deduplication registry, the universal parser, and the smart chunker – it's time to integrate them into a seamless, end-to-end data pipeline. As a Software Developer, your responsibility is to ensure that these modules work together harmoniously, transforming raw SEC filings into ready-to-use data for QuantInsight Analytics. This final step involves orchestrating the entire workflow and generating a clear report of its operations, detailing what was processed, what was skipped, and any issues encountered.")
    st.markdown(f"---")

    if st.session_state.pipeline_report is None:
        st.info("No pipeline report available. Please run the pipeline from the 'Download & Process Filings' page.")
        if st.button("Go to Download & Process Filings"):
            st.session_state.current_page = 'Download & Process Filings'
            st.experimental_rerun()
    else:
        report = st.session_state.pipeline_report
        st.markdown(f"### Pipeline Summary")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Attempted Downloads", report["attempted_downloads"])
        col2.metric("Unique Filings Processed", report["unique_filings_processed"])
        col3.metric("Skipped Duplicates", report["skipped_duplicates"])
        col4.metric("Processing Errors", report["parsing_errors"])

        st.markdown(f"### Detailed Processing Log")
        if report["details"]:
            df_details = pd.DataFrame(report["details"])
            st.dataframe(df_details, use_container_width=True)

            st.markdown(f"### View Extracted Text and Chunks for a Processed Filing")
            processed_filings = [d for d in report["details"] if d["status"] == "processed"]
            if processed_filings:
                # Create a display string for each filing
                display_options = [
                    f"{f['cik']} - {f['filing_type']} - {f['accession_number']}" 
                    for f in processed_filings
                ]
                selected_display = st.selectbox(
                    "Select a successfully processed filing to view its content:",
                    options=[""] + display_options,
                    index=0,
                    key="select_filing_to_view"
                )

                if selected_display:
                    # Find the corresponding filing metadata
                    selected_index = display_options.index(selected_display)
                    st.session_state.selected_filing_for_view = processed_filings[selected_index]
                    
                    if st.button("Extract and Chunk Selected Filing"):
                        with st.spinner(f"Extracting text and chunking for {st.session_state.selected_filing_for_view['accession_number']}..."):
                            try:
                                # Re-use the pipeline's parser and chunker instances
                                pipeline = st.session_state.sec_pipeline_instance
                                file_path = st.session_state.selected_filing_for_view['path']
                                parsed_content = pipeline.parser.parse_filing(file_path)
                                full_text = parsed_content["text"]
                                chunks = pipeline.chunker.chunk_document(full_text)
                                
                                st.session_state.view_text = full_text
                                st.session_state.view_chunks = chunks
                                st.success("Text extracted and chunked!")
                                st.experimental_rerun() # Rerun to display the extracted content
                            except Exception as e:
                                st.error(f"Error viewing filing: {e}")
                
                if st.session_state.view_text:
                    st.markdown(f"#### Raw Extracted Text")
                    st.text_area(
                        "Full Text Content", 
                        st.session_state.view_text, 
                        height=300, 
                        key="full_text_viewer"
                    )
                    st.markdown(f"#### Generated Chunks ({len(st.session_state.view_chunks)} chunks)")
                    for i, chunk in enumerate(st.session_state.view_chunks):
                        with st.expander(f"Chunk {i+1} (approx. {len(chunk.split())} words)"):
                            st.text_area(f"Chunk {i+1}", chunk, height=150, key=f"chunk_viewer_{i}")
            else:
                st.info("No filings were successfully processed to view details.")

        else:
            st.info("No processing details available.")
```
