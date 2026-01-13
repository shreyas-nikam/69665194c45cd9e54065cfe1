

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
import httpx # For general async HTTP requests, if needed
import fitz # PyMuPDF for PDF processing

# --- Start of content that would typically be in source.py ---

# Minimal implementation of SECDownloader, DocumentParser, DocumentChunker, and SECPipeline
# to make app.py runnable. In a real application, these would be robustly implemented
# and likely reside in a separate 'source.py' module.

class SECDownloader:
    """
    Handles downloading SEC filings with rate limiting.
    """
    FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A", "S-1", "S-3", "S-4", "10-KT", "10-QT"]
    RATE_LIMIT_DELAY = 0.1 # 10 requests per second (SEC generally advises approx. 10 req/s)

    def __init__(self, company_name: str, email_address: str, download_dir: str):
        self.user_agent = f"{company_name} {email_address}"
        self.download_dir = download_dir
        self.downloader = Downloader(download_dir, self.user_agent)
        self._last_request_time = 0

    async def _rate_limit_pause(self):
        """Ensures requests are rate-limited."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            await asyncio.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.monotonic()

    async def download_filing(self, cik: str, filing_type: str, after_date: str, limit: int = 1):
        """
        Downloads SEC filings for a given CIK and filing type, respecting rate limits.
        Returns a list of downloaded file paths and accession numbers.
        """
        downloaded_metadata = []
        try:
            await self._rate_limit_pause()
            
            # The sec-edgar-downloader library's `get` method is synchronous.
            # To avoid blocking the Streamlit event loop, we run it in a separate thread.
            
            # Construct the expected target directory path for the filing type
            # The downloader often creates a folder like '10-K' or '8-K' under CIK.
            filing_type_folder_name = filing_type.replace('/', '_') # Handle types like '8-K/A'
            target_dir = os.path.join(self.download_dir, cik, filing_type_folder_name)
            
            # Ensure the target directory exists before listing files, otherwise os.walk will fail
            os.makedirs(target_dir, exist_ok=True)

            # Get list of files before download attempt
            files_before = set()
            for root, _, fnames in os.walk(target_dir):
                for fname in fnames:
                    files_before.add(os.path.join(root, fname))
            
            # Run the synchronous downloader in a separate thread
            await asyncio.to_thread(self.downloader.get, filing_type, cik=cik, after=after_date, limit=limit)

            # Get list of files after download attempt
            files_after = set()
            # Re-scan the target_dir as new subdirectories/files might have been created
            os.makedirs(target_dir, exist_ok=True) # Ensure it exists in case download failed
            for root, _, fnames in os.walk(target_dir):
                for fname in fnames:
                    files_after.add(os.path.join(root, fname))
            
            new_files = list(files_after - files_before)

            # Parse new files to extract accession numbers (this is heuristic)
            for file_path in new_files:
                # sec-edgar-downloader typically saves files with a structure like
                # CIK/FILING_TYPE/ACCESSION_NUMBER.txt
                # Example: .../0000320193/10-K/0001628280-23-036573.txt
                # The accession number is the last part before .txt, sometimes hyphenated.
                match = re.search(r'(\d{10}-\d{2}-\d{6})\.txt', os.path.basename(file_path))
                if match:
                    accession_number = match.group(1).replace('-', '') 
                    downloaded_metadata.append({
                        "cik": cik,
                        "filing_type": filing_type,
                        "accession_number": accession_number,
                        "path": file_path
                    })
                else:
                    # Fallback for other file types or naming conventions
                    # For example, PDF files might be named differently or actual HTML files.
                    # A more robust solution would be to get the actual filing URL and parse the accession from it.
                    # For this implementation, we will use the filename as a last resort identifier.
                    downloaded_metadata.append({
                        "cik": cik,
                        "filing_type": filing_type,
                        "accession_number": os.path.basename(file_path), # Use filename as accession if not found
                        "path": file_path
                    })

        except Exception as e:
            st.error(f"Error downloading {filing_type} for CIK {cik}: {e}")
        return downloaded_metadata


class DocumentParser:
    """
    Parses SEC filings (HTML or PDF) to extract clean text.
    """
    def parse_filing(self, file_path: str) -> dict:
        """
        Parses a document (HTML or PDF) and extracts its text content.
        Returns a dictionary with 'text' and potentially 'tables'.
        """
        # sec-edgar-downloader often downloads HTML content saved as .txt files.
        # So, if it's .txt, we should check if it's actually HTML.
        
        if file_path.lower().endswith(".pdf"):
            return self._parse_pdf(file_path)
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content_sample = f.read(4096) # Read a small chunk to detect HTML
            
            if "<html" in content_sample.lower() or "<!doctype html>" in content_sample.lower():
                # If it looks like HTML, parse it as HTML
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    full_content = f.read()
                return self._parse_html_string(full_content)
            else:
                # Otherwise, treat as plain text
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    full_content = f.read()
                return {"text": full_content, "tables": []}
        except Exception as e:
            st.warning(f"Could not determine type or read file {file_path}, attempting as plain text: {e}")
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    full_content = f.read()
                return {"text": full_content, "tables": []}
            except Exception as e_fallback:
                raise ValueError(f"Failed to parse file {file_path} as any known type: {e_fallback}")


    def _parse_html_string(self, html_content: str) -> dict:
        """Parses an HTML string to extract text and tables."""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove scripts, styles, and other non-content elements
        for script_or_style in soup(["script", "style", "header", "footer", "nav", "meta", "link", "noscript"]):
            script_or_style.decompose()

        # Get text
        text = soup.get_text(separator='\n')
        
        # Clean up excessive whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text) # Remove multiple blank lines
        text = re.sub(r' +', ' ', text)         # Replace multiple spaces with single space
        text = text.strip()

        # Basic table extraction
        tables = []
        for table_tag in soup.find_all('table'):
            table_data = []
            for row in table_tag.find_all('tr'):
                cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
                table_data.append(cols)
            if table_data:
                # Convert list of lists to pandas DataFrame for better handling later
                try:
                    df = pd.DataFrame(table_data[1:], columns=table_data[0]) if table_data and len(table_data) > 1 else pd.DataFrame(table_data)
                    tables.append(df.to_dict('records')) # Store as list of dicts for JSON compatibility
                except Exception: # Fallback if table structure is bad
                    tables.append(table_data)

        return {"text": text, "tables": tables}

    def _parse_pdf(self, file_path: str) -> dict:
        """Parses a PDF file to extract text and tables using pdfplumber."""
        text = ""
        tables = []
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
                    if page_text:
                        text += page_text + "\n"
                    
                    # Extract tables (pdfplumber has good table extraction)
                    page_tables = page.extract_tables()
                    for pt in page_tables:
                        if pt:
                            try:
                                # Convert pdfplumber table to pandas DataFrame
                                df = pd.DataFrame(pt[1:], columns=pt[0]) if pt and len(pt) > 1 else pd.DataFrame(pt)
                                tables.append(df.to_dict('records'))
                            except Exception: # Fallback
                                tables.append(pt)
            
            # Clean up extracted text
            text = re.sub(r'\n\s*\n', '\n\n', text)
            text = re.sub(r' +', ' ', text)
            text = text.strip()

        except Exception as e:
            st.error(f"Error parsing PDF {file_path}: {e}")
            return {"text": "", "tables": []} # Return empty if error
        
        return {"text": text, "tables": tables}


class DocumentChunker:
    """
    Splits a document's text into smaller, overlapping chunks.
    """
    def __init__(self, chunk_size: int = 750, chunk_overlap: int = 50):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def chunk_document(self, text: str) -> list[str]:
        """
        Splits text into chunks of `chunk_size` words with `chunk_overlap`.
        """
        if not text:
            return []

        words = text.split()
        if not words:
            return []

        chunks = []
        start_idx = 0
        
        # Ensure chunk_overlap is not greater than chunk_size, and at least 0
        self.chunk_overlap = max(0, min(self.chunk_overlap, self.chunk_size - 1))
        
        while start_idx < len(words):
            end_idx = min(start_idx + self.chunk_size, len(words))
            chunk = " ".join(words[start_idx:end_idx])
            chunks.append(chunk)
            
            # For the next chunk, move by (chunk_size - chunk_overlap)
            # If we've reached the end, break.
            if end_idx == len(words):
                break 
            
            start_idx += (self.chunk_size - self.chunk_overlap)
            
            # Prevent start_idx from going beyond the end if chunk_size - chunk_overlap is very small
            if start_idx >= len(words):
                 break
        
        # Post-processing: If the last chunk is significantly smaller than chunk_size
        # and there's more than one chunk, merge it with the previous one.
        # This prevents very tiny trailing chunks that might lack context.
        if chunks and len(chunks) > 1:
            last_chunk_len = len(chunks[-1].split())
            if last_chunk_len < self.chunk_size * 0.5: # If last chunk is less than 50% of target size
                chunks[-2] = " ".join([chunks[-2], chunks[-1]])
                chunks.pop() # Remove the now merged last chunk

        return chunks


class SECPipeline:
    """
    Orchestrates the downloading, parsing, deduplication, and chunking of SEC filings.
    """
    def __init__(self, download_dir: str, registry_file: str, 
                 chunk_size: int, chunk_overlap: int, 
                 company_name: str, email_address: str):
        self.download_dir = download_dir
        # Ensure registry file is within the download_dir or a logical subdirectory
        self.registry_file = os.path.join(download_dir, registry_file) 
        self.downloader = SECDownloader(company_name, email_address, download_dir)
        self.parser = DocumentParser()
        self.chunker = DocumentChunker(chunk_size, chunk_overlap)
        self.registry = self._load_registry()

    def _load_registry(self) -> set:
        """Loads previously processed document hashes from the registry file."""
        if not os.path.exists(self.registry_file):
            return set()
        try:
            with open(self.registry_file, 'r', encoding='utf-8') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            st.error(f"Error loading registry file {self.registry_file}: {e}")
            return set()

    def _save_registry(self):
        """Saves the current set of document hashes to the registry file."""
        os.makedirs(os.path.dirname(self.registry_file), exist_ok=True)
        try:
            with open(self.registry_file, 'w', encoding='utf-8') as f:
                for doc_hash in sorted(list(self.registry)):
                    f.write(doc_hash + '\n')
        except Exception as e:
            st.error(f"Error saving registry file {self.registry_file}: {e}")

    async def run_pipeline(self, ciks: list[str], filing_types: list[str], after_date: str, limit: int = 1):
        """
        Runs the full SEC data pipeline for specified CIKs and filing types.
        """
        report = {
            "attempted_downloads": 0,
            "unique_filings_processed": 0,
            "skipped_duplicates": 0,
            "parsing_errors": 0,
            "details": []
        }

        for cik in ciks:
            for f_type in filing_types:
                # Download filings
                downloaded_meta = await self.downloader.download_filing(cik, f_type, after_date, limit)
                
                for meta in downloaded_meta:
                    report["attempted_downloads"] += 1
                    file_path = meta["path"]
                    accession_number = meta["accession_number"]
                    
                    if not os.path.exists(file_path):
                        report["parsing_errors"] += 1
                        report["details"].append({
                            "cik": cik, "filing_type": f_type, "accession_number": accession_number,
                            "status": "error: file not found after download attempt", "path": file_path
                        })
                        continue

                    try:
                        # Parse document
                        parsed_content = self.parser.parse_filing(file_path)
                        text_content = parsed_content["text"]

                        # Check if text_content is empty after parsing (e.g., failed parsing)
                        if not text_content.strip():
                            report["parsing_errors"] += 1
                            report["details"].append({
                                "cik": cik, "filing_type": f_type, "accession_number": accession_number,
                                "status": "error: empty text content after parsing", "path": file_path
                            })
                            continue

                        # Deduplicate using content hash
                        content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

                        if content_hash in self.registry:
                            report["skipped_duplicates"] += 1
                            report["details"].append({
                                "cik": cik, "filing_type": f_type, "accession_number": accession_number,
                                "status": "skipped (duplicate content)", "path": file_path
                            })
                            continue

                        # Add to registry and save
                        self.registry.add(content_hash)
                        self._save_registry()

                        # Chunk document
                        chunks = self.chunker.chunk_document(text_content)

                        report["unique_filings_processed"] += 1
                        report["details"].append({
                            "cik": cik, "filing_type": f_type, "accession_number": accession_number,
                            "status": "processed", "path": file_path, "num_chunks": len(chunks)
                        })

                    except Exception as e:
                        report["parsing_errors"] += 1
                        report["details"].append({
                            "cik": cik, "filing_type": f_type, "accession_number": accession_number,
                            "status": f"error: {type(e).__name__}: {e}", "path": file_path
                        })
        return report

# --- End of content that would typically be in source.py ---


# Page Config and Header
st.set_page_config(page_title="QuLab: SEC EDGAR Pipeline", layout="wide")
st.sidebar.image("https://www.quantuniversity.com/assets/img/logo5.jpg")
st.sidebar.divider()
st.title("QuLab: SEC EDGAR Pipeline")
st.divider()

# Initialize session state variables
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

# Sidebar Navigation
with st.sidebar:
    st.title("SEC EDGAR Pipeline")
    nav_options = ['Home', 'Configuration & Setup', 'Download & Process Filings', 'Processed Filings & Analysis']
    
    # Determine index safely
    try:
        nav_index = nav_options.index(st.session_state.current_page)
    except ValueError:
        nav_index = 0
        st.session_state.current_page = nav_options[0] # Fallback to Home if invalid
        
    selected_nav = st.radio(
        "Navigation",
        nav_options,
        index=nav_index,
        key="sidebar_navigation_radio"
    )
    
    if selected_nav != st.session_state.current_page:
        st.session_state.current_page = selected_nav
        st.rerun()

# Page: Home
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
    if st.button("Go to Configuration", key="home_to_config_button"):
        st.session_state.current_page = 'Configuration & Setup'
        st.rerun()

# Page: Configuration & Setup
elif st.session_state.current_page == 'Configuration & Setup':
    st.markdown(f"# 1. Setting Up the SEC EDGAR Data Pipeline")
    st.markdown(f"Before we dive into building the pipeline, let's configure the essential parameters. These settings ensure we comply with SEC guidelines and prepare our environment for efficient data processing.")
    st.markdown(f"")
    st.markdown(f"## Architecting the Rate-Limited SEC Filing Downloader")
    st.markdown(f"As a Software Developer at QuantInsight Analytics, the first critical component of our pipeline is a reliable SEC filing downloader. The SEC EDGAR system has strict rate limits to prevent abuse and ensure fair access for all users. Ignoring these limits can lead to our IP address being temporarily or permanently blocked, completely disrupting our data flow. Our task is to build a downloader that inherently respects these limits, ensuring a continuous and compliant data acquisition process. The SEC generally advises approximately 10 requests per second.")
    st.markdown(f"To be safe and compliant, we will introduce a delay $T_{{delay}}$ between consecutive download requests. If the desired rate limit is $R$ requests per second, then the minimum delay needed between requests is $T_{{delay}} = \\frac{{1}}{{R}}$ seconds. For example, if $R = 0.1$ requests/second (i.e., 1 request every 10 seconds to be very conservative, or $R=10$ requests/second, meaning $T_{{delay}}=0.1$ seconds), we ensure we stay within the guidelines.")
    
    st.markdown(r"$$ T_{delay} = \frac{1}{R_{max}} $$")
    
    st.markdown(f"where $R_{{max}}$ is the maximum allowed requests per second. For SEC EDGAR, we will aim for a conservative delay of 0.1 seconds (10 requests/second).")
    st.markdown(f"---")
    
    st.markdown(f"### Pipeline Configuration Parameters")
    st.session_state.company_name = st.text_input(
        "Your Company Name (for SEC User-Agent)", 
        st.session_state.company_name, 
        key="company_name_input",
        help="The SEC requires a user agent string to identify your application. E.g., 'QuantInsight Analytics'"
    )
    st.session_state.email_address = st.text_input(
        "Your Email Address (for SEC User-Agent)", 
        st.session_state.email_address,
        key="email_address_input",
        help="Your email address for SEC User-Agent. E.g., 'your_email@quantinsight.com'"
    )
    st.session_state.download_dir = st.text_input(
        "Local Download Directory", 
        st.session_state.download_dir, 
        key="download_dir_input",
        help="Path where SEC filings will be stored locally. E.g., './sec_filings'"
    )
    st.session_state.registry_file = st.text_input(
        "Document Registry File Name", 
        st.session_state.registry_file, 
        key="registry_file_input",
        help="Filename to store processed document hashes for deduplication. It will be stored inside the download directory. E.g., 'document_registry.txt'"
    )

    st.markdown(f"### Document Chunking Strategy")
    st.markdown(f"Modern AI models, especially Large Language Models (LLMs), have token limits. Sending an entire, multi-page SEC filing to an LLM is often infeasible and inefficient. Moreover, smaller, context-rich chunks lead to better retrieval and generation quality. For QuantInsight Analytics, our AI applications need document segments that are small enough to fit within model contexts (e.g., 500-1000 tokens) yet large enough to retain meaningful information and context.")
    st.markdown(f"")
    st.markdown(f"This process, known as 'chunking,' involves splitting a long document into smaller, manageable pieces. A simple yet effective strategy is fixed-size chunking, where we split the text based on a word or token count, ensuring some overlap between chunks to preserve context across boundaries.")
    
    st.markdown(f"If a document has $TotalTokens$ and we want chunks of $ChunkSize$ tokens with an $OverlapSize$ tokens, the number of chunks $N_{{chunks}}$ can be roughly calculated as:")
    
    st.markdown(r"$$ N_{chunks} = \lceil \frac{TotalTokens - OverlapSize}{ChunkSize - OverlapSize} \rceil $$")
    
    st.markdown(f"For simplicity, we will implement fixed-size chunking based on word count (as a proxy for tokens) with no explicit overlap for this demonstration, aiming for chunks between 500-1000 words.")

    st.session_state.chunk_size = st.number_input(
        "Chunk Size (words)", 
        min_value=100, 
        max_value=2000, 
        value=st.session_state.chunk_size, 
        step=50,
        key="chunk_size_input",
        help="The target size of each document chunk in words (proxy for tokens)."
    )
    st.session_state.chunk_overlap = st.number_input(
        "Chunk Overlap (words)", 
        min_value=0, 
        max_value=int(st.session_state.chunk_size * 0.5), # Ensure overlap is less than chunk size
        value=st.session_state.chunk_overlap, 
        step=10,
        key="chunk_overlap_input",
        help="The number of words to overlap between consecutive chunks to maintain context."
    )

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
            st.success("Pipeline components initialized successfully!")
            st.session_state.current_page = 'Download & Process Filings'
            st.rerun()
        except Exception as e:
            st.error(f"Error initializing pipeline: {e}")

# Page: Download & Process Filings
elif st.session_state.current_page == 'Download & Process Filings':
    st.markdown(f"# 2. Download and Process SEC Filings")
    st.markdown(f"Here, you'll specify the companies and filing types to ingest. The pipeline will download, parse, deduplicate, and chunk these documents.")
    st.markdown(f"")

    if not st.session_state.pipeline_initialized or st.session_state.sec_pipeline_instance is None:
        st.warning("Pipeline not initialized. Please go to 'Configuration & Setup' first.")
        if st.button("Go to Configuration & Setup", key="go_to_config_from_download_button"):
            st.session_state.current_page = 'Configuration & Setup'
            st.rerun()
    else:
        st.markdown(f"### Filing Selection")
        st.markdown(f"Enter CIKs (Company Identification Numbers), one per line.")
        st.session_state.ciks_input = st.text_area(
            "CIKs (e.g., 0000320193 for Apple Inc.)", 
            st.session_state.ciks_input, 
            height=100,
            key="ciks_text_area",
            help="Enter one CIK per line. The CIKs will be cleaned to remove non-digit characters."
        )
        # Clean CIKs: remove whitespace and non-digit characters, filter empty strings
        ciks_list = [cik.strip() for cik in st.session_state.ciks_input.split('\n') if cik.strip()]
        ciks_list = [re.sub(r'\D', '', cik) for cik in ciks_list if re.sub(r'\D', '', cik)] # Remove non-digits
        
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
        st.markdown(f"Duplicate data is a common headache in data pipelines, leading to wasted processing, skewed analytics, and increased storage costs. For QuantInsight Analytics, processing the same SEC filing multiple times would be inefficient and could lead to inconsistencies in our financial models. To combat this, we need a robust mechanism to identify and prevent the reprocessing of identical documents.")
        st.markdown(f"")
        st.markdown(f"The solution is a \"Document Registry\" that uses cryptographic hashing to generate a unique \"fingerprint\" for each document's content. We will use the SHA-256 algorithm, which takes an input (the document's text content) and produces a fixed-size, unique hash value. The probability of two different documents producing the same SHA-256 hash is astronomically small, making it ideal for deduplication.")
        
        st.markdown(f"Mathematically, a cryptographic hash function $H$ maps an arbitrary-size input $M$ (our document content) to a fixed-size output $h$ (the hash value), such that:")
        st.markdown(r"$$ h = H(M) $$")
        st.markdown(r"1. It is easy to compute $h = H(M)$.")
        st.markdown(r"2. It is computationally infeasible to invert $H$ (find $M$ from $h$).")
        st.markdown(r"3. It is computationally infeasible to find two different inputs $M_1 \neq M_2$ such that $H(M_1) = H(M_2)$ (collision resistance).")
        
        st.markdown(f"For SHA-256, the output is a 256-bit (64-character hexadecimal) string. We will store these hashes in our registry to quickly check if a document has already been processed.")
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

        # Define the async function to run the pipeline
        async def run_pipeline_async():
            if not ciks_list:
                st.error("Please enter at least one CIK.")
                st.session_state.processing_in_progress = False # Ensure flag is reset
                return

            st.session_state.processing_in_progress = True
            st.session_state.pipeline_report = None
            st.session_state.selected_filing_for_view = None
            st.session_state.view_text = None
            st.session_state.view_chunks = None
            
            with st.spinner("Running pipeline... This may take a while depending on CIKs and limits."):
                try:
                    report = await st.session_state.sec_pipeline_instance.run_pipeline(
                        ciks=ciks_list,
                        filing_types=st.session_state.filing_types_selected,
                        after_date=st.session_state.after_date_input.strftime("%Y-%m-%d"),
                        limit=st.session_state.limit_input
                    )
                    st.session_state.pipeline_report = report
                    st.success("Pipeline execution completed!")
                    st.session_state.current_page = 'Processed Filings & Analysis'
                except Exception as e:
                    st.error(f"Pipeline execution failed: {e}")
                finally:
                    st.session_state.processing_in_progress = False
                    st.rerun() # Rerun to update the page to 'Processed Filings & Analysis' or reflect completion

        # Trigger the async function with st.button
        st.button(
            "Run SEC Data Pipeline", 
            on_click=run_pipeline_async, # Pass the async function to on_click
            disabled=st.session_state.processing_in_progress or not ciks_list,
            key="run_pipeline_button"
        )


# Page: Processed Filings & Analysis
elif st.session_state.current_page == 'Processed Filings & Analysis':
    st.markdown(f"# 3. Orchestrating the End-to-End SEC Data Pipeline and Reporting")
    st.markdown(f"Now that we have all the individual components – the rate-limited downloader, the deduplication registry, the universal parser, and the smart chunker – it's time to integrate them into a seamless, end-to-end data pipeline. As a Software Developer, your responsibility is to ensure that these modules work together harmoniously, transforming raw SEC filings into ready-to-use data for QuantInsight Analytics. This final step involves orchestrating the entire workflow and generating a clear report of its operations, detailing what was processed, what was skipped, and any issues encountered.")
    st.markdown(f"---")

    if st.session_state.pipeline_report is None:
        st.info("No pipeline report available. Please run the pipeline from the 'Download & Process Filings' page.")
        if st.button("Go to Download & Process Filings", key="go_to_download_from_analysis_button"):
            st.session_state.current_page = 'Download & Process Filings'
            st.rerun()
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
                
                # Determine initial selection for the dropdown
                current_selection_label = ""
                if st.session_state.selected_filing_for_view:
                    potential_label = f"{st.session_state.selected_filing_for_view['cik']} - {st.session_state.selected_filing_for_view['filing_type']} - {st.session_state.selected_filing_for_view['accession_number']}"
                    if potential_label in display_options:
                        current_selection_label = potential_label
                
                selected_display = st.selectbox(
                    "Select a successfully processed filing to view its content:",
                    options=[""] + display_options, # Add an empty string for "no selection"
                    index= (display_options.index(current_selection_label) + 1) if current_selection_label else 0,
                    key="select_filing_to_view"
                )

                # Find the corresponding metadata for the selected display option
                current_selected_meta = None
                if selected_display: # If a filing is selected (not the empty string)
                    try:
                        selected_index_in_display_options = display_options.index(selected_display)
                        current_selected_meta = processed_filings[selected_index_in_display_options]
                    except ValueError:
                        pass # Should not happen if selected_display comes from display_options

                # Check if a new filing is selected OR if the content for the current selection is not loaded
                # This prevents re-extracting text on every rerun if the selection hasn't changed.
                if current_selected_meta and (st.session_state.selected_filing_for_view != current_selected_meta or st.session_state.view_text is None):
                    st.session_state.selected_filing_for_view = current_selected_meta
                    # Clear previous view content before loading new
                    st.session_state.view_text = None 
                    st.session_state.view_chunks = None 
                    
                    with st.spinner(f"Extracting text and chunking for {st.session_state.selected_filing_for_view['accession_number']}..."):
                        try:
                            pipeline = st.session_state.sec_pipeline_instance
                            file_path = st.session_state.selected_filing_for_view['path']
                            parsed_content = pipeline.parser.parse_filing(file_path)
                            full_text = parsed_content["text"]
                            chunks = pipeline.chunker.chunk_document(full_text)
                            
                            st.session_state.view_text = full_text
                            st.session_state.view_chunks = chunks
                            st.success("Text extracted and chunked!")
                        except Exception as e:
                            st.error(f"Error viewing filing: {e}")
                elif not selected_display: # If the empty option is selected
                    st.session_state.selected_filing_for_view = None
                    st.session_state.view_text = None
                    st.session_state.view_chunks = None

                
                if st.session_state.view_text is not None: # Check explicitly for None, as empty string is valid
                    st.markdown(f"#### Raw Extracted Text")
                    st.text_area(
                        "Full Text Content", 
                        st.session_state.view_text, 
                        height=300, 
                        key="full_text_viewer"
                    )
                    st.markdown(f"#### Generated Chunks ({len(st.session_state.view_chunks)} chunks)")
                    for i, chunk in enumerate(st.session_state.view_chunks):
                        with st.expander(f"Chunk {i+1} (approx. {len(chunk.split())} words)", key=f"chunk_expander_{i}"):
                            st.text_area(f"Chunk {i+1}", chunk, height=150, key=f"chunk_viewer_{i}")
            else:
                st.info("No filings were successfully processed to view details.")

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
