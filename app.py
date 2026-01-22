import streamlit as st
import asyncio
import datetime
import pandas as pd
import os
import re
import time
import hashlib
from sec_edgar_downloader import Downloader
from bs4 import BeautifulSoup
import pdfplumber
import httpx  # For general async HTTP requests, if needed
import fitz  # PyMuPDF for PDF processing
from source import SECDownloader, DocumentParser, DocumentChunker, SECPipeline
import streamlit_mermaid as stmd

# Page Config and Header
st.set_page_config(page_title="QuLab: SEC EDGAR Pipeline", layout="wide")
st.sidebar.image("https://www.quantuniversity.com/assets/img/logo5.jpg")
st.sidebar.divider()
st.title("QuLab: SEC EDGAR Pipeline")
st.divider()


mermaid_steps = """
flowchart LR

  %% Define Nodes with attached classes
  STEP1[Setup pipeline]:::c1
  STEP2[Add downloader]:::c2
  STEP3[Add rate limiting]:::c3
  STEP4[Download filings]:::c4
  STEP5[Parse documents]:::c5
  STEP6[Deduplicate]:::c6
  STEP7[Chunk text]:::c7
  STEP8[Process loop]:::c8
  STEP9[Generate report]:::c9

  %% Define Connections
  STEP1 --> STEP2 --> STEP3 --> STEP4 --> STEP5 --> STEP6 --> STEP7 --> STEP8 --> STEP9

  %% Define Styles (Colors)
  classDef c{step} fill:#ffcc99,stroke:#333,stroke-width:2px,color:#000
"""


# Initialize session state variables
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'Home'
if 'company_name' not in st.session_state:
    st.session_state.company_name = "SEC Analytics"
if 'email_address' not in st.session_state:
    st.session_state.email_address = "your_email@company_name.com"
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
    st.session_state.ciks_input = "0000320193\n0000789019"  # Apple, Microsoft
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
if 'last_ciks_processed' not in st.session_state:
    st.session_state.last_ciks_processed = []
if 'last_filing_types_processed' not in st.session_state:
    st.session_state.last_filing_types_processed = []

# Sidebar Navigation
with st.sidebar:
    st.title("SEC EDGAR Pipeline")
    nav_options = ['Home', 'Configuration & Setup',
                   'Download & Process Filings', 'Processed Filings & Analysis']

    # Determine index safely
    try:
        nav_index = nav_options.index(st.session_state.current_page)
    except ValueError:
        nav_index = 0
        st.session_state.current_page = nav_options[0]

    selected_nav = st.selectbox(
        "Navigation",
        nav_options,
        index=nav_index,
        key="sidebar_navigation_selectbox"
    )

    if selected_nav != st.session_state.current_page:
        st.session_state.current_page = selected_nav
        st.rerun()

    st.markdown("---")
    st.markdown("### 🎯 Key Objectives")
    st.markdown("""
    - **Remember:** List SEC filing types (10-K, 10-Q, 8-K, DEF-14A)
    - **Understand:** Explain why document chunking affects extraction quality
    - **Apply:** Implement SEC download and parsing pipeline
    - **Analyze:** Compare PDF vs HTML extraction strategies
    """)
    st.markdown("---")
    st.markdown("### 🛠️ Tools Introduced")
    st.markdown("""
    - **sec-edgar-downloader**: Filing retrieval
    - **pdfplumber**: PDF extraction
    - **pymupdf (fitz)**: PDF processing
    - **httpx**: HTTP client
    """)
    st.markdown("---")

# Page: Home
if st.session_state.current_page == 'Home':
    st.markdown(
        f"## Introduction")
    st.markdown(f"Your current mission is critical: to establish a robust, reliable, and scalable pipeline for ingesting public financial disclosures from the SEC EDGAR database. The platform relies on this data for everything from regulatory compliance and market research to sophisticated investment analysis and AI-driven insights.")
    st.markdown(f"")
    st.markdown(f"The challenge lies in handling the volume and variety of SEC filings (10-K, 10-Q, 8-K, DEF 14A), adhering to SEC's API guidelines (especially rate limits), and ensuring data quality through deduplication and intelligent document segmentation. This application will walk you through building the core components of this pipeline, demonstrating how each step addresses a real-world requirement for QuantInsight Analytics. You will implement a rate-limited downloader, a content-hash-based registry for deduplication, a versatile parser for HTML and PDF filings, and a smart chunking mechanism for AI readiness.")
    st.markdown(f"")
    st.markdown(f"Your work here lays the groundwork for all downstream applications, ensuring that our analysts and AI models always have access to clean, non-redundant, and properly formatted financial data.")
    stmd.st_mermaid(mermaid_steps.format(step=1))
    st.markdown(f"---")
    st.markdown(f"## Key Concepts")
    st.markdown(f"""
    - SEC filing types and their AI-relevance
    - Document chunking strategies
    - Content deduplication
    - Rate limiting for SEC API
    """)
    st.markdown(f"---")
    st.markdown(f"### Prerequisites")
    st.markdown(f"""
    - Weeks 1-2 completed
    - Understanding of document processing
    """)


# Page: Configuration & Setup
elif st.session_state.current_page == 'Configuration & Setup':
    st.markdown(f"# 1. Setting Up the SEC EDGAR Data Pipeline")
    st.markdown(f"Before we dive into building the pipeline, let's configure the essential parameters. These settings ensure we comply with SEC guidelines and prepare our environment for efficient data processing.")
    st.markdown(f"")

    st.markdown(f"## Architecting the Rate-Limited SEC Filing Downloader")
    st.markdown(f"The first critical component of our pipeline is a reliable SEC filing downloader. The SEC EDGAR system has strict rate limits to prevent abuse and ensure fair access for all users. Ignoring these limits can lead to our IP address being temporarily or permanently blocked, completely disrupting our data flow. Our task is to build a downloader that inherently respects these limits, ensuring a continuous and compliant data acquisition process. The SEC generally advises approximately 10 requests per second.")
    st.markdown(f"To be safe and compliant, we will introduce a delay $T_{{delay}}$ between consecutive download requests. If the desired rate limit is $R$ requests per second, then the minimum delay needed between requests is $T_{{delay}} = \\frac{{1}}{{R}}$ seconds. For example, if $R = 0.1$ requests/second (i.e., 1 request every 10 seconds to be very conservative, or $R=10$ requests/second, meaning $T_{{delay}}=0.1$ seconds), we ensure we stay within the guidelines.")
    st.markdown(r"$$ T_{delay} = \frac{1}{R_{max}} $$")
    st.markdown(
        f"where $R_{{max}}$ is the maximum allowed requests per second. For SEC EDGAR, we will aim for a conservative delay of 0.1 seconds (10 requests/second).")
    stmd.st_mermaid(mermaid_steps.format(step=2))
    st.markdown(f"#### SECDownloader class")
    with st.expander("📄 View SECDownloader Code", expanded=False):
        st.code('''
class SECDownloader:
    """
    Downloads SEC filings with integrated rate limiting and stores them locally.
    """
    FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]
    # SEC requests that you limit your requests to no more than 10 per second.
    # To be conservative, we'll enforce 1 request every 0.1 seconds (10 req/s).
    REQUEST_DELAY = 0.1  # seconds

    def __init__(self, download_dir: str = "./sec_filings",
                 company_name: str = "QuantInsight Analytics",
                 email_address: str = "your_email@company_name.com"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        # Initialize the sec-edgar-downloader with user agent details
        # The SEC requires a user agent string to identify your application.
        self.downloader = Downloader(
            company_name=company_name,
            email_address=email_address,
            download_folder=str(self.download_dir)
        )
        logger.info("SECDownloader initialized", download_dir=str(self.download_dir),
                    company_name=company_name, email_address=email_address)

    async def download_company_filings(
        self,
        cik: str,
        filing_types: Optional[List[str]] = None,
        after_date: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Downloads specified SEC filings for a given CIK, adhering to rate limits.

        Args:
            cik (str): The CIK (Company Identification Number) of the company.
            filing_types (Optional[List[str]]): List of filing types to download.
                                                Defaults to all common types.
            after_date (Optional[str]): Date string (YYYY-MM-DD) to download filings
                                        after.
            limit (int): Maximum number of filings to download for each type.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each describing a downloaded filing.
        """
        filing_types_to_download = filing_types or self.FILING_TYPES
        results = []

        logger.info("Starting download for company", cik=cik,
                    filing_types=filing_types_to_download)

        for filing_type in async_tqdm(filing_types_to_download, desc=f"Downloading {cik}"):
            try:
                # Enforce rate limit before each download request
                await asyncio.sleep(self.REQUEST_DELAY)

                logger.debug("Attempting to download", cik=cik, filing_type=filing_type,
                             after_date=after_date, limit=limit)

                self.downloader.get(
                    filing_type,
                    cik,
                    after=after_date,
                    limit=limit,
                )

                # After successful download, find the paths to the downloaded files
                company_type_dir = self.download_dir / "sec-edgar-filings" / cik / filing_type
                if company_type_dir.exists():
                    for accession_number_dir in company_type_dir.iterdir():
                        if accession_number_dir.is_dir():
                            # Find the actual filing file (usually an HTML or PDF)
                            html_file = next(
                                accession_number_dir.glob("*.html"), None)
                            pdf_file = next(
                                accession_number_dir.glob("*.pdf"), None)
                            txt_file = next(
                                accession_number_dir.glob("*.txt"), None)
                            file_path = html_file or pdf_file or txt_file
                            if file_path:
                                results.append({
                                    "cik": cik,
                                    "filing_type": filing_type,
                                    "accession_number": accession_number_dir.name,
                                    "path": str(file_path),
                                    "processed_hash": None  # Placeholder for later deduplication
                                })
                            else:
                                logger.warning("No HTML or PDF found in downloaded directory",
                                               path=str(accession_number_dir))

                logger.info("Filings downloaded successfully", cik=cik, filing_type=filing_type,
                            count=len([r for r in results if r['cik'] == cik and r['filing_type'] == filing_type]))

            except Exception as e:
                logger.error("Download failed for filing type",
                             cik=cik, filing_type=filing_type, error=str(e))

        return results
''', language='python')
    st.markdown(f"---")

    st.markdown(f"### Document Chunking Strategy")
    st.markdown(f"Modern AI models, especially Large Language Models (LLMs), have token limits. Sending an entire, multi-page SEC filing to an LLM is often infeasible and inefficient. Moreover, smaller, context-rich chunks lead to better retrieval and generation quality. For SEC Analytics, our AI applications need document segments that are small enough to fit within model contexts (e.g., 500-1000 tokens) yet large enough to retain meaningful information and context.")
    st.markdown(f"This process, known as 'chunking,' involves splitting a long document into smaller, manageable pieces. A simple yet effective strategy is fixed-size chunking, where we split the text based on a word or token count, ensuring some overlap between chunks to preserve context across boundaries.")
    stmd.st_mermaid(mermaid_steps.format(step=7))
    st.markdown(f"#### DocumentChunker class")
    with st.expander("📄 View DocumentChunker Code", expanded=False):
        st.code('''
class DocumentChunker:
    """
    Splits document text into manageable, context-rich chunks for AI models.
    """

    def __init__(self, chunk_size: int = 750, chunk_overlap: int = 50):
        """
        Initializes the DocumentChunker.

        Args:
            chunk_size (int): The target size of each chunk in words.
            chunk_overlap (int): The number of words to overlap between consecutive chunks.
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        logger.info("DocumentChunker initialized",
                    chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    def _split_text_into_words(self, text: str) -> List[str]:
        """Splits text into words, handling common punctuation."""
        # Simple split by whitespace, then filter out empty strings
        words = text.split()
        return words

    def chunk_document(self, document_text: str) -> List[str]:
        """
        Splits a document's text content into smaller, overlapping chunks.

        Args:
            document_text (str): The full text content of the document.

        Returns:
            List[str]: A list of text chunks.
        """
        if not document_text:
            return []

        words = self._split_text_into_words(document_text)
        total_words = len(words)
        chunks = []

        start_idx = 0
        while start_idx < total_words:
            end_idx = min(start_idx + self.chunk_size, total_words)
            chunk = " ".join(words[start_idx:end_idx])
            chunks.append(chunk)

            # Move start_idx for next chunk, considering overlap
            start_idx += (self.chunk_size - self.chunk_overlap)
            # Ensure the last chunk isn't tiny due to aggressive overlap
            if start_idx >= total_words and self.chunk_overlap > 0:
                if (total_words - (start_idx - (self.chunk_size - self.chunk_overlap))) < self.chunk_overlap:
                    break  # Avoid creating a very small overlapping last chunk

        logger.debug(f"Chunked document into {len(chunks)} pieces.")
        return chunks''', language='python')

    st.markdown(f"### Pipeline Configuration Parameters")
    st.markdown(
        f"Configure the pipeline using the parameters below. These map directly to the SECPipeline constructor:")
    st.session_state.company_name = st.text_input(
        "Company Name (for SEC User-Agent)",
        st.session_state.company_name,
        key="company_name_input",
        help="The SEC requires a user agent string to identify your application. E.g., 'SEC Analytics'"
    )
    st.session_state.email_address = st.text_input(
        "Email Address (for SEC User-Agent)",
        st.session_state.email_address,
        key="email_address_input",
        help="Your email address for SEC User-Agent. E.g., 'your_email@company_name.com'"
    )
    st.session_state.chunk_size = st.number_input(
        "Chunk Size (words)",
        min_value=100,
        max_value=2000,
        value=st.session_state.chunk_size,
        step=50,
        key="chunk_size_input",
        help="The target size of each document chunk in words."
    )
    st.session_state.chunk_overlap = st.number_input(
        "Chunk Overlap (words)",
        min_value=0,
        max_value=int(st.session_state.chunk_size * 0.5),
        value=st.session_state.chunk_overlap,
        step=10,
        key="chunk_overlap_input",
        help="The number of words to overlap between consecutive chunks."
    )
    st.markdown(f"---")

    if st.button("Initialize Pipeline Components", key="initialize_pipeline_button"):
        # Reset view states to prevent stale data being displayed after configuration change
        st.session_state.pipeline_report = None
        st.session_state.selected_filing_for_view = None
        st.session_state.view_text = None
        st.session_state.view_chunks = None

        try:
            # Create download directory if it doesn't exist
            os.makedirs(st.session_state.download_dir, exist_ok=True)

            # Instantiate the SECPipeline with configured parameters
            pipeline_instance = SECPipeline(
                download_dir=st.session_state.download_dir,
                registry_file=st.session_state.registry_file,
                chunk_size=st.session_state.chunk_size,
                chunk_overlap=st.session_state.chunk_overlap,
                company_name=st.session_state.company_name,
                email_address=st.session_state.email_address
            )
            st.session_state.sec_pipeline_instance = pipeline_instance
            st.session_state.pipeline_initialized = True
            st.success(
                "Pipeline components initialized successfully! Now move on to 'Download & Process Filings' page.")
            st.session_state.current_page = 'Download & Process Filings'

        except Exception as e:
            st.error(f"Error initializing pipeline: {e}")

# Page: Download & Process Filings
elif st.session_state.current_page == 'Download & Process Filings':
    st.markdown(f"# 2. Download and Process SEC Filings")
    st.markdown(f"Here, you'll specify the companies and filing types to ingest. The pipeline will download, parse, deduplicate, and chunk these documents.")
    st.markdown(f"")

    if not st.session_state.pipeline_initialized or st.session_state.sec_pipeline_instance is None:
        st.warning(
            "Pipeline not initialized. Please go to 'Configuration & Setup' first.")
        if st.button("Go to Configuration & Setup", key="go_to_config_from_download_button"):
            st.session_state.current_page = 'Configuration & Setup'
            st.rerun()
    else:
        stmd.st_mermaid(mermaid_steps.format(step=0))
        st.markdown(f"### Filing Selection")
        st.markdown(
            f"Enter CIKs (Company Identification Numbers), one per line.")
        st.session_state.ciks_input = st.text_area(
            "CIKs (e.g., 0000320193 for Apple Inc.)",
            st.session_state.ciks_input,
            height=100,
            key="ciks_text_area",
            help="Enter one CIK per line. The CIKs will be cleaned to remove non-digit characters."
        )
        # Clean CIKs: remove whitespace and non-digit characters, filter empty strings
        ciks_list = [cik.strip() for cik in st.session_state.ciks_input.split(
            '\n') if cik.strip()]
        ciks_list = [re.sub(r'\D', '', cik) for cik in ciks_list if re.sub(
            r'\D', '', cik)]  # Remove non-digits

        st.session_state.filing_types_selected = st.multiselect(
            "Select Filing Types",
            options=SECDownloader.FILING_TYPES,
            default=st.session_state.filing_types_selected,
            key="filing_types_multiselect",
            help="Choose the SEC filing types (e.g., 10-K, 10-Q) to download."
        )
        st.session_state.after_date_input = st.date_input(
            "Download filings after date (YYYY-MM-DD)",
            st.session_state.after_date_input,
            key="after_date_input_date",
            help="Only download filings published after this date."
        )
        st.session_state.limit_input = st.number_input(
            "Max number of filings per type per CIK",
            min_value=1,
            max_value=100,
            value=st.session_state.limit_input,
            step=1,
            key="limit_input_number",
            help="Controls the number of most recent filings of each type to download for each CIK."
        )

        st.markdown(f"---")
        st.markdown(f"## Building the Document Registry for Deduplication")
        st.markdown(f"Duplicate data is a common headache in data pipelines, leading to wasted processing, skewed analytics, and increased storage costs. For SEC Analytics, processing the same SEC filing multiple times would be inefficient and could lead to inconsistencies in our financial models. To combat this, we need a robust mechanism to identify and prevent the reprocessing of identical documents.")
        st.markdown(f"")
        st.markdown(f"The solution is a \"Document Registry\" that uses cryptographic hashing to generate a unique \"fingerprint\" for each document's content. We will use the SHA-256 algorithm, which takes an input (the document's text content) and produces a fixed-size, unique hash value. The probability of two different documents producing the same SHA-256 hash is astronomically small, making it ideal for deduplication.")

        st.markdown(f"Mathematically, a cryptographic hash function $H$ maps an arbitrary-size input $M$ (our document content) to a fixed-size output $h$ (the hash value), such that:")
        st.markdown(r"$$ h = H(M) $$")
        st.markdown(r"1. It is easy to compute $h = H(M)$.")
        st.markdown(
            r"2. It is computationally infeasible to invert $H$ (find $M$ from $h$).")
        st.markdown(
            r"3. It is computationally infeasible to find two different inputs $M_1 \neq M_2$ such that $H(M_1) = H(M_2)$ (collision resistance).")

        st.markdown(f"For SHA-256, the output is a 256-bit (64-character hexadecimal) string. We will store these hashes in our registry to quickly check if a document has already been processed.")
        stmd.st_mermaid(mermaid_steps.format(step=6))
        st.markdown(f"---")
        st.markdown(f"## Crafting a Universal Document Parser (HTML & PDF)")
        st.markdown(f"SEC filings come in various formats, primarily HTML and occasionally PDF, each with its own structural nuances. To make this raw data useful for SEC Analytics' AI models and analysts, we need to extract clean, readable text and structured tables, regardless of the original document format. This task is crucial for downstream natural language processing (NLP) and quantitative analysis.")
        st.markdown(f"")
        st.markdown(f"A robust `DocumentParser` needs to:")
        st.markdown(f"*   **HTML Parsing**: Navigate the often complex and inconsistently structured HTML of SEC filings, focusing on extracting the main textual content and identifying tables. Libraries like `BeautifulSoup` excel at this by providing an intuitive way to traverse the HTML DOM.")
        st.markdown(f"*   **PDF Parsing**: Handle PDF documents, which can be challenging due to their visual layout rather than semantic structure. Tools like `pdfplumber` or `PyMuPDF` (Fitz) are essential for extracting text, tables, and even layout information from PDFs.")
        st.markdown(f"")
        st.markdown(f"The goal is to produce \"clean text output\" – free of HTML tags, scripts, and extraneous formatting – and separate, structured tabular data.")
        stmd.st_mermaid(mermaid_steps.format(step=5))
        with st.expander("📄 View DocumentParser Code", expanded=False):
            st.code('''
class DocumentParser:
    """
    Parses SEC filings (HTML and PDF) to extract clean text and tabular data.
    """

    def __init__(self):
        logger.info("DocumentParser initialized")

    def _parse_html(self, file_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
        """Extracts text and tables from an HTML file."""
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            soup = BeautifulSoup(f, 'lxml')

        # Remove script and style elements
        for script_or_style in soup(['script', 'style']):
            script_or_style.extract()

        # Get text
        text = soup.get_text(separator='\n')
        # Clean up whitespace and empty lines
        text = re.sub(r'\n\s*\n', '\n', text).strip()

        # Extract tables (simplified for demonstration)
        tables_data = []
        for table_tag in soup.find_all('table'):
            headers = [th.get_text(strip=True)
                       for th in table_tag.find_all('th')]
            rows = []
            for tr_tag in table_tag.find_all('tr'):
                # Exclude header rows if they are also found as tr
                if tr_tag.find('th'):
                    continue
                cells = [td.get_text(strip=True)
                         for td in tr_tag.find_all('td')]
                if cells:
                    # Only add if it's a data row
                    rows.append(cells)

            if headers and rows:
                # Only add if we found both headers and rows
                tables_data.append({"headers": headers, "rows": rows})
            elif rows and not headers:
                # Sometimes tables might not have th tags, but still have rows
                tables_data.append({"rows": rows})

        return text, tables_data

    def _parse_pdf(self, file_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
        """Extracts text and tables from a PDF file using pdfplumber and PyMuPDF."""
        full_text = []
        tables_data = []

        # Use pdfplumber for table extraction, PyMuPDF for robust text extraction
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # Extract text using pdfplumber's extract_text (can be good for layout)
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if page_text:
                    full_text.append(page_text)

                # Extract tables using pdfplumber
                page_tables = page.extract_tables()
                if page_tables:
                    for table in page_tables:
                        # Convert list of lists (table) to dictionary for consistency
                        if table and table[0]:  # Ensure table and header row exist
                            headers = table[0]
                            rows = table[1:]
                            tables_data.append(
                                {"page": page_num + 1, "headers": headers, "rows": rows})
                        elif table:
                            # If no explicit headers, just provide rows
                            tables_data.append(
                                {"page": page_num + 1, "rows": table})

        # Fallback/additional text extraction for potentially missed text by pdfplumber
        # Or if we want more granular control over text extraction using PyMuPDF
        try:
            doc = fitz.open(file_path)
            pymupdf_text = []
            for page in doc:
                pymupdf_text.append(page.get_text("text"))
            doc.close()

            # Combine or prioritize text, here we'll just append if pdfplumber was sparse
            if not full_text:
                # If pdfplumber didn't get much, use pymupdf
                full_text = pymupdf_text
            elif len("".join(full_text)) < len("".join(pymupdf_text)):
                # If pymupdf got more text
                full_text = pymupdf_text
        except Exception as e:
            logger.warning(
                f"PyMuPDF text extraction failed for {file_path}: {e}")

        final_text = "\n".join(full_text).strip()
        # Clean up whitespace
        final_text = re.sub(r'\n\s*\n', '\n', final_text)
        return final_text, tables_data

    def parse_filing(self, file_path: str) -> Dict[str, Any]:
        """
        Parses a given filing file based on its extension (HTML or PDF).

        Args:
            file_path (str): The path to the filing file.

        Returns:
            Dict[str, Any]: A dictionary containing 'text' and 'tables' extracted.
        """
        path = Path(file_path)
        extracted_text = ""
        extracted_tables = []

        if path.suffix == '.html':
            logger.debug(f"Parsing HTML file: {file_path}")
            extracted_text, extracted_tables = self._parse_html(path)
        elif path.suffix == '.pdf':
            logger.debug(f"Parsing PDF file: {file_path}")
            extracted_text, extracted_tables = self._parse_pdf(path)
        else:
            logger.warning(f"Unsupported file type for parsing: {file_path}")
            extracted_text = f"Content for {path.name} (unsupported type)"

        return {"text": extracted_text, "tables": extracted_tables}

''', language='python')
        st.markdown(f"---")

        # Define the function to run the pipeline
        def run_pipeline_sync():
            if not ciks_list:
                st.error("Please enter at least one CIK.")
                return

            st.session_state.processing_in_progress = True
            st.session_state.pipeline_report = None
            st.session_state.selected_filing_for_view = None
            st.session_state.view_text = None
            st.session_state.view_chunks = None
            # Store the current selections for later use
            st.session_state.last_ciks_processed = ciks_list
            st.session_state.last_filing_types_processed = st.session_state.filing_types_selected

        # Trigger pipeline execution
        if st.button(
            "Run SEC Data Pipeline",
            disabled=st.session_state.processing_in_progress or not ciks_list,
            key="run_pipeline_button"
        ):
            run_pipeline_sync()

        # Actually run the async pipeline if processing flag is set
        if st.session_state.processing_in_progress:
            with st.spinner("Running pipeline... This may take a while depending on CIKs and limits."):
                try:
                    report = asyncio.run(st.session_state.sec_pipeline_instance.run_pipeline(
                        ciks=ciks_list,
                        filing_types=st.session_state.filing_types_selected,
                        after_date=st.session_state.after_date_input.strftime(
                            "%Y-%m-%d"),
                        limit=st.session_state.limit_input
                    ))
                    st.session_state.pipeline_report = report
                    # st.write(report)
                    st.success(
                        "Pipeline execution completed! Move on to 'Processed Filings & Analysis' page to view results.")

                    # Display downloaded files structure (filtered by selections)
                    st.markdown("### 📁 Downloaded Files")
                    sec_filings_dir = os.path.join(
                        st.session_state.download_dir, "sec-edgar-filings")
                    if os.path.exists(sec_filings_dir):
                        file_tree = []
                        file_tree.append(f"📂 sec-edgar-filings/")

                        # Only show files for selected CIKs and filing types
                        for cik in st.session_state.last_ciks_processed:
                            cik_dir = os.path.join(sec_filings_dir, cik)
                            if os.path.exists(cik_dir):
                                file_tree.append(f"  📂 {cik}/")

                                for filing_type in st.session_state.last_filing_types_processed:
                                    filing_type_dir = os.path.join(
                                        cik_dir, filing_type)
                                    if os.path.exists(filing_type_dir):
                                        file_tree.append(
                                            f"    📂 {filing_type}/")

                                        # List accession number folders and files
                                        for accession_dir in sorted(os.listdir(filing_type_dir)):
                                            accession_path = os.path.join(
                                                filing_type_dir, accession_dir)
                                            if os.path.isdir(accession_path):
                                                file_tree.append(
                                                    f"      📂 {accession_dir}/")

                                                # List files in accession directory
                                                for file in sorted(os.listdir(accession_path)):
                                                    file_path = os.path.join(
                                                        accession_path, file)
                                                    if os.path.isfile(file_path):
                                                        file_size = os.path.getsize(
                                                            file_path)
                                                        file_size_str = f"{file_size / 1024:.1f} KB" if file_size > 1024 else f"{file_size} B"
                                                        file_tree.append(
                                                            f"        📄 {file} ({file_size_str})")
                            file_tree.append("")

                        with st.expander(f"View Downloaded Files Structure ({len(file_tree)} items)", expanded=True):
                            st.text('\n'.join(file_tree))
                    else:
                        st.info("No files directory found yet.")

                    st.session_state.current_page = 'Processed Filings & Analysis'
                except Exception as e:
                    st.error(f"Pipeline execution failed: {e}")
                finally:
                    st.session_state.processing_in_progress = False


# Page: Processed Filings & Analysis
elif st.session_state.current_page == 'Processed Filings & Analysis':
    st.markdown(
        f"# 3. Orchestrating the End-to-End SEC Data Pipeline and Reporting")
    st.markdown(f"Now that we have all the individual components – the rate-limited downloader, the deduplication registry, the universal parser, and the smart chunker – it's time to integrate them into a seamless, end-to-end data pipeline. Your responsibility is to ensure that these modules work together harmoniously, transforming raw SEC filings into ready-to-use data for SEC Analytics. This final step involves orchestrating the entire workflow and generating a clear report of its operations, detailing what was processed, what was skipped, and any issues encountered.")
    st.markdown(f"---")
    stmd.st_mermaid(mermaid_steps.format(step=8))

    with st.expander("📄 View SECPipeline Code", expanded=False):
        st.code(
            '''
class SECPipeline:
    """
    Orchestrates the end-to-end SEC data ingestion pipeline.
    """

    def __init__(self, download_dir: str = "./sec_filings", registry_file: str = "document_registry.txt", chunk_size: int = 750, chunk_overlap: int = 50):
        self.downloader = SECDownloader(download_dir=download_dir)
        self.registry = DocumentRegistry(registry_file=registry_file)
        self.parser = DocumentParser()
        self.chunker = DocumentChunker(
            chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        self.download_dir = Path(download_dir)
        self.processed_summary = {
            "attempted_downloads": 0,
            "unique_filings_processed": 0,
            "skipped_duplicates": 0,
            "parsing_errors": 0,
            "chunking_errors": 0,
            "details": []
        }
        logger.info("SECPipeline initialized")

    async def run_pipeline(
        self,
        ciks: List[str],
        filing_types: Optional[List[str]] = None,
        after_date: Optional[str] = None,
        limit: int = 1,
    ) -> Dict[str, Any]:
        """
        Runs the full SEC data ingestion pipeline for specified CIKs and filing types.

        Args:
            ciks (List[str]): List of CIKs to process.
            filing_types (Optional[List[str]]): List of filing types to download.
            after_date (Optional[str]): Date string (YYYY-MM-DD) to download filings after.
            limit (int): Maximum number of filings to download for each type per CIK.

        Returns:
            Dict[str, Any]: A summary report of the pipeline execution.
        """
        logger.info("Starting full SEC pipeline run")
        all_download_metadata = []

        # 1. Download Stage
        for cik in async_tqdm(ciks, desc="Processing CIKs"):
            self.processed_summary["attempted_downloads"] += (
                len(filing_types or self.downloader.FILING_TYPES) * limit)
            download_results = await self.downloader.download_company_filings(
                cik=cik,
                filing_types=filing_types,
                after_date=after_date,
                limit=limit
            )
            all_download_metadata.extend(download_results)

        # 2. Processing Stage
        for filing_meta in async_tqdm(all_download_metadata, desc="Parsing & Chunking Filings"):
            file_path = filing_meta["path"]
            cik = filing_meta["cik"]
            filing_type = filing_meta["filing_type"]
            accession_number = filing_meta["accession_number"]

            detail_entry = {
                "cik": cik,
                "filing_type": filing_type,
                "accession_number": accession_number,
                "status": "pending",
                "message": ""
            }

            try:
                # A. Parse Document
                parsed_content = self.parser.parse_filing(file_path)
                full_text = parsed_content["text"]
                tables = parsed_content["tables"]

                if not full_text:
                    raise ValueError(
                        f"No text extracted from filing: {file_path}")

                # B. Deduplicate using Content Hash
                content_hash = self.registry.compute_content_hash(full_text)

                if self.registry.is_processed(content_hash):
                    self.processed_summary["skipped_duplicates"] += 1
                    detail_entry.update(
                        status="skipped", message="Duplicate filing (content hash exists)")
                    logger.info("Skipped duplicate filing", cik=cik, filing_type=filing_type,
                                accession_number=accession_number, content_hash=content_hash)
                    self.processed_summary["details"].append(detail_entry)
                    continue

                # Mark as processed BEFORE chunking to prevent re-processing if chunking fails
                # (Optional decision: could also mark AFTER successful chunking)
                self.registry.mark_as_processed(content_hash)
                filing_meta["processed_hash"] = content_hash

                # C. Chunk Document
                chunks = self.chunker.chunk_document(full_text)

                if not chunks:
                    raise ValueError(
                        f"No chunks generated from filing: {file_path}")

                self.processed_summary["unique_filings_processed"] += 1
                detail_entry.update(
                    status="processed",
                    message=f"Processed successfully into {len(chunks)} chunks.",
                    num_chunks=len(chunks),
                    num_tables=len(tables),
                    content_hash=content_hash
                )
                logger.info("Filing processed", cik=cik, filing_type=filing_type,
                            accession_number=accession_number, num_chunks=len(chunks), num_tables=len(tables))

            except ValueError as ve:
                self.processed_summary["parsing_errors"] += 1
                detail_entry.update(
                    status="failed", message=f"Processing error: {ve}")
                logger.error("Processing failed for filing", cik=cik, filing_type=filing_type,
                             accession_number=accession_number, error=str(ve))

            except Exception as e:
                self.processed_summary["parsing_errors"] += 1
                detail_entry.update(
                    status="failed", message=f"Unexpected error during processing: {e}")
                logger.error("Unexpected error during processing", cik=cik,
                             filing_type=filing_type, accession_number=accession_number, error=str(e))

            self.processed_summary["details"].append(detail_entry)

        logger.info("Full SEC pipeline run completed.")
        return self.processed_summary''', language='python')
    st.markdown(f"---")

    if st.session_state.pipeline_report is None:
        st.info("No pipeline report available. Please run the pipeline from the 'Download & Process Filings' page.")
        if st.button("Go to Download & Process Filings", key="go_to_download_from_analysis_button"):
            st.session_state.current_page = 'Download & Process Filings'
            st.rerun()
    else:
        report = st.session_state.pipeline_report
        st.markdown(f"### Pipeline Summary")
        col2, col3, col4 = st.columns(3)
        col2.metric("Unique Filings Processed",
                    report["unique_filings_processed"])
        col3.metric("Skipped Duplicates", report["skipped_duplicates"])
        col4.metric("Processing Errors", report["parsing_errors"])

        st.markdown(f"### Detailed Processing Log")
        if report["details"]:
            df_details = pd.DataFrame(report["details"])
            df_details.drop(columns=["num_chunks",
                            "num_tables", "message"], inplace=True, errors='ignore')
            # drop duplicates in the dataframe based on cik, filing_type, accession_number, keeping the last occurrence
            df_details = df_details.drop_duplicates(
                subset=["cik", "filing_type"], keep="last")
            st.dataframe(df_details, width='stretch',)

        else:
            st.info("No processing details available.")


# License
st.caption('''
---
## QuantUniversity License

© QuantUniversity 2025  
This notebook was created for **educational purposes only** and is **not intended for commercial use**.  

- You **may not copy, share, or redistribute** this notebook **without explicit permission** from QuantUniversity.  
- You **may not delete or modify this license cell** without authorization.  
- This notebook was generated using **QuCreate**, an AI-powered assistant.  
- Content generated by AI may contain **hallucinated or incorrect information**. Please **verify before using**.  

All rights reserved. For permissions or commercial licensing, contact: [info@qusandbox.com](mailto:info@qusandbox.com)
''')
