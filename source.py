import asyncio
import hashlib
import re
import os
import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

from sec_edgar_downloader import Downloader
from bs4 import BeautifulSoup
import pdfplumber
import fitz # PyMuPDF
import structlog
import time
from tqdm.asyncio import tqdm as async_tqdm

# Configure logging for better visibility into pipeline operations
logger = structlog.get_logger()
class SECDownloader:
    """
    Downloads SEC filings with integrated rate limiting and stores them locally.
    """
    FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]
    # SEC requests that you limit your requests to no more than 10 per second.
    # To be conservative, we'll enforce 1 request every 0.1 seconds (10 req/s).
    REQUEST_DELAY = 0.1 # seconds

    def __init__(self, download_dir: str = "./sec_filings",
                 company_name: str = "QuantInsight Analytics",
                 email_address: str = "your_email@quantinsight.com"):
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

        logger.info("Starting download for company", cik=cik, filing_types=filing_types_to_download)

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
                            html_file = next(accession_number_dir.glob("*.html"), None)
                            pdf_file = next(accession_number_dir.glob("*.pdf"), None)
                            txt_file = next(accession_number_dir.glob("*.txt"), None)
                            file_path = html_file or pdf_file or txt_file
                            if file_path:
                                results.append({
                                    "cik": cik,
                                    "filing_type": filing_type,
                                    "accession_number": accession_number_dir.name,
                                    "path": str(file_path),
                                    "processed_hash": None # Placeholder for later deduplication
                                })
                            else:
                                logger.warning("No HTML or PDF found in downloaded directory",
                                               path=str(accession_number_dir))


                logger.info("Filings downloaded successfully", cik=cik, filing_type=filing_type,
                            count=len([r for r in results if r['cik'] == cik and r['filing_type'] == filing_type]))

            except Exception as e:
                logger.error("Download failed for filing type", cik=cik, filing_type=filing_type, error=str(e))

        return results

# Example Usage:
# We'll use a couple of well-known CIKs for demonstration
# Apple Inc.: 0000320193
# Microsoft Corp: 0000789019
# Tesla Inc.: 0001318605

async def run_downloader_demo():
    sec_downloader = SECDownloader()
    sample_ciks = ["0000320193", "0000789019"] # Apple and Microsoft
    sample_filing_types = ["10-K", "10-Q"]

    # Download 2 latest 10-K and 10-Q filings for each company
    # We'll use a specific after_date to ensure we get recent filings for demonstration
    # Note: sec-edgar-downloader automatically creates a structured directory.
    # We will then find the actual file paths within those directories.

    all_downloaded_filings = []
    for cik in sample_ciks:
        downloaded_filings = await sec_downloader.download_company_filings(
            cik=cik,
            filing_types=sample_filing_types,
            after_date="2022-01-01",
            limit=2
        )
        all_downloaded_filings.extend(downloaded_filings)

    logger.info("Finished downloading all sample filings.")
    return all_downloaded_filings

downloaded_filings_metadata = await run_downloader_demo()
import hashlib
from pathlib import Path
# Assuming a logger is configured elsewhere, or using a placeholder
import logging

logger = logging.getLogger(__name__)

class DocumentRegistry:
    """
    Manages the registry of processed documents using content hashes for deduplication.
    """
    def __init__(self, registry_file: str = "document_registry.txt"):
        self.registry_file = Path(registry_file)
        self.processed_hashes = set()
        self._load_registry()
        logger.info("DocumentRegistry initialized", registry_file=str(self.registry_file))

    def _load_registry(self):
        """Loads processed hashes from a file into memory."""
        if self.registry_file.exists():
            with open(self.registry_file, 'r') as f:
                for line in f:
                    self.processed_hashes.add(line.strip())
            logger.info("Loaded existing registry", count=len(self.processed_hashes))

    def _save_registry(self):
        """Saves current processed hashes to the file."""
        with open(self.registry_file, 'w') as f:
            for h in self.processed_hashes:
                f.write(h + '\n')
        logger.info("Registry saved", count=len(self.processed_hashes))

    def compute_content_hash(self, content: str) -> str:
        """
        Computes the SHA-256 hash of the given string content.

        Args:
            content (str): The text content of the document.

        Returns:
            str: The SHA-256 hash in hexadecimal format.
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def is_processed(self, content_hash: str) -> bool:
        """
        Checks if a document with the given hash has already been processed.

        Args:
            content_hash (str): The SHA-256 hash of the document content.

        Returns:
            bool: True if processed, False otherwise.
        """
        return content_hash in self.processed_hashes

    def mark_as_processed(self, content_hash: str):
        """
        Adds a content hash to the registry, marking the document as processed.

        Args:
            content_hash (str): The SHA-256 hash of the document content.
        """
        if content_hash not in self.processed_hashes:
            self.processed_hashes.add(content_hash)
            self._save_registry()  # Persist the registry change

# --- Example Usage ---
document_registry = DocumentRegistry()

# Simulate some document content
doc_content_1 = "This is the content of financial report A for Q1."
doc_content_2 = "This is the content of financial report B for Q2."
doc_content_1_duplicate = "This is the content of financial report A for Q1." # Identical to doc_content_1
doc_content_3_diff_spacing = "This is the content of financial report A for Q1. " # Different due to trailing space

hash_1 = document_registry.compute_content_hash(doc_content_1)
hash_2 = document_registry.compute_content_hash(doc_content_2)
hash_1_dup = document_registry.compute_content_hash(doc_content_1_duplicate)
hash_3_diff = document_registry.compute_content_hash(doc_content_3_diff_spacing)

print(f"Hash for Doc 1: {hash_1}")
print(f"Hash for Doc 2: {hash_2}")
print(f"Hash for Doc 1 (duplicate): {hash_1_dup}")
print(f"Hash for Doc 3 (diff spacing): {hash_3_diff}")

# Test deduplication logic
print(f"\nIs Doc 1 processed? {document_registry.is_processed(hash_1)}")

document_registry.mark_as_processed(hash_1)

print(f"Is Doc 1 processed after marking? {document_registry.is_processed(hash_1)}")
print(f"Is Doc 1 duplicate processed? {document_registry.is_processed(hash_1_dup)}") # Should be True
print(f"Is Doc 2 processed? {document_registry.is_processed(hash_2)}")
print(f"Is Doc 3 processed? {document_registry.is_processed(hash_3_diff)}")

# Verify the registry file content
with open(document_registry.registry_file, 'r') as f:
    print("\nRegistry file content:")
    for line in f:
        print(line.strip())
import re
from pathlib import Path
from typing import Tuple, List, Dict, Any
# Assuming these libraries are installed and imported in the full environment
# from bs4 import BeautifulSoup
# import pdfplumber
# import fitz  # PyMuPDF
# import logging

# logger = logging.getLogger(__name__)

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
            headers = [th.get_text(strip=True) for th in table_tag.find_all('th')]
            rows = []
            for tr_tag in table_tag.find_all('tr'):
                # Exclude header rows if they are also found as tr
                if tr_tag.find('th'):
                    continue
                cells = [td.get_text(strip=True) for td in tr_tag.find_all('td')]
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
                            tables_data.append({"page": page_num + 1, "headers": headers, "rows": rows})
                        elif table:
                            # If no explicit headers, just provide rows
                            tables_data.append({"page": page_num + 1, "rows": table})

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
            logger.warning(f"PyMuPDF text extraction failed for {file_path}: {e}")

        final_text = "\n".join(full_text).strip()
        final_text = re.sub(r'\n\s*\n', '\n', final_text)  # Clean up whitespace
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

# --- Example Usage ---
document_parser = DocumentParser()

# To demonstrate, we need an actual HTML and potentially a PDF file.
# We'll use one of the downloaded HTML files and simulate a PDF.

# Use the first downloaded HTML filing for demonstration
if downloaded_filings_metadata:
    html_file_path = downloaded_filings_metadata[0]["path"]
    # if the path is a .txt file, save it as a .html file
    if html_file_path.endswith(".txt"):
        with open(html_file_path, 'r') as txt_file:
            html_content = txt_file.read()
            html_file_path = html_file_path.replace(".txt", ".html")
            with open(html_file_path, 'w') as html_file:
                html_file.write(html_content)

    logger.info(f"Using sample HTML file: {html_file_path}")

    print(f"Parsing sample HTML file: {html_file_path}")
    parsed_html_content = document_parser.parse_filing(html_file_path)

    print("\n--- Extracted HTML Text Sample (first 500 chars) ---")
    print(parsed_html_content['text'][:500])

    print("\n--- Extracted HTML Tables Sample ---")
    if parsed_html_content['tables']:
        print(f"Found {len(parsed_html_content['tables'])} tables.")
        print(parsed_html_content['tables'][0])
    else:
        print("No tables extracted from HTML sample.")

# Simulate a PDF file for parsing demonstration
# In a real scenario, this PDF would be part of the SEC filings or linked within HTML
synthetic_pdf_path = Path("./sec_filings/sample_report.pdf")

# Create a dummy PDF file for testing
if not synthetic_pdf_path.exists():
    import reportlab
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors

    doc = SimpleDocTemplate(str(synthetic_pdf_path))
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("Sample Financial Overview", styles['h1']))
    story.append(Paragraph("This is a simulated PDF financial report for demonstration purposes. It contains some text and a small table.", styles['Normal']))
    story.append(Paragraph("Key metrics for Q1 2023:", styles['h2']))

    data = [['Metric', 'Value'],
            ['Revenue', '$100M'],
            ['Net Income', '$15M'],
            ['EPS', '$1.25']]

    table = Table(data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    story.append(table)
    story.append(Paragraph("Further details can be found in the accompanying HTML filing.", styles['Normal']))

    doc.build(story)
    print(f"\nCreated synthetic PDF: {synthetic_pdf_path}")
else:
    print(f"\nUsing existing synthetic PDF: {synthetic_pdf_path}")

print(f"Parsing synthetic PDF file: {synthetic_pdf_path}")
parsed_pdf_content = document_parser.parse_filing(str(synthetic_pdf_path))

print("\n--- Extracted PDF Text Sample (first 500 chars) ---")
print(parsed_pdf_content['text'][:500])

print("\n--- Extracted PDF Tables Sample ---")
if parsed_pdf_content['tables']:
    print(f"Found {len(parsed_pdf_content['tables'])} tables.")
    print(parsed_pdf_content['tables'][0])
else:
    print("No tables extracted from PDF sample.")

# Clean up synthetic PDF
if synthetic_pdf_path.exists():
    synthetic_pdf_path.unlink()
    logger.info("Cleaned up synthetic PDF file.")
import pytest
import re
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from unittest.mock import MagicMock, patch

# Configure logging for better visibility into pipeline operations
import structlog
logger = structlog.get_logger()

# --- Code to test (copied from the prompt for clarity in the test file) ---
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
        logger.info("DocumentChunker initialized", chunk_size=chunk_size, chunk_overlap=chunk_overlap)

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
            if start_idx >= total_words and self.chunk_overlap > 0: # Ensure the last chunk isn't tiny due to aggressive overlap
                if (total_words - (start_idx - (self.chunk_size - self.chunk_overlap))) < self.chunk_overlap:
                    break # Avoid creating a very small overlapping last chunk

        logger.debug(f"Chunked document into {len(chunks)} pieces.")
        return chunks

# Example Usage:
document_chunker = DocumentChunker(chunk_size=800, chunk_overlap=100) # Aim for 800-word chunks with 100-word overlap

# Use the text from the previously parsed HTML document
if downloaded_filings_metadata and parsed_html_content['text']:
    sample_text = parsed_html_content['text']
    print(f"Original document length (words): {len(document_chunker._split_text_into_words(sample_text))}")

    document_chunks = document_chunker.chunk_document(sample_text)
    print(f"\nNumber of chunks created: {len(document_chunks)}")

    if document_chunks:
        print("\n--- First Chunk (first 500 chars) ---")
        print(document_chunks[0][:500])
        print(f"Length of first chunk (words): {len(document_chunker._split_text_into_words(document_chunks[0]))}")

    if len(document_chunks) > 1:
        print("\n--- Second Chunk (first 500 chars) ---")
        print(document_chunks[1][:500])
        print(f"Length of second chunk (words): {len(document_chunker._split_text_into_words(document_chunks[1]))}")

    # Demonstrate overlap (conceptually) by finding a common phrase
    # For exact overlap validation, one would compare words.
    # Here, we just observe if the start of chunk 2 looks like the end of chunk 1.
    print("\n--- Conceptual Overlap Check ---")
    first_chunk_words = document_chunker._split_text_into_words(document_chunks[0])
    second_chunk_words = document_chunker._split_text_into_words(document_chunks[1])

    if len(first_chunk_words) > document_chunker.chunk_overlap and \
       len(second_chunk_words) > document_chunker.chunk_overlap:
        print(f"End of first chunk (last {document_chunker.chunk_overlap} words): {' '.join(first_chunk_words[-document_chunker.chunk_overlap:])[:200]}...")
        print(f"Start of second chunk (first {document_chunker.chunk_overlap} words): {' '.join(second_chunk_words[:document_chunker.chunk_overlap])[:200]}...")
else:
    print("No parsed text available for chunking demonstration. Please ensure previous steps ran successfully.")
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Any
# Assuming tqdm is installed: from tqdm.asyncio import tqdm as async_tqdm
# and previous classes (SECDownloader, DocumentRegistry, etc.) are available.

class SECPipeline:
    """
    Orchestrates the end-to-end SEC data ingestion pipeline.
    """
    def __init__(self, download_dir: str = "./sec_filings", registry_file: str = "document_registry.txt", chunk_size: int = 750, chunk_overlap: int = 50):
        self.downloader = SECDownloader(download_dir=download_dir)
        self.registry = DocumentRegistry(registry_file=registry_file)
        self.parser = DocumentParser()
        self.chunker = DocumentChunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
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
            self.processed_summary["attempted_downloads"] += (len(filing_types or self.downloader.FILING_TYPES) * limit)
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
                    raise ValueError(f"No text extracted from filing: {file_path}")

                # B. Deduplicate using Content Hash
                content_hash = self.registry.compute_content_hash(full_text)

                if self.registry.is_processed(content_hash):
                    self.processed_summary["skipped_duplicates"] += 1
                    detail_entry.update(status="skipped", message="Duplicate filing (content hash exists)")
                    logger.info("Skipped duplicate filing", cik=cik, filing_type=filing_type, accession_number=accession_number, content_hash=content_hash)
                    self.processed_summary["details"].append(detail_entry)
                    continue

                # Mark as processed BEFORE chunking to prevent re-processing if chunking fails
                # (Optional decision: could also mark AFTER successful chunking)
                self.registry.mark_as_processed(content_hash)
                filing_meta["processed_hash"] = content_hash

                # C. Chunk Document
                chunks = self.chunker.chunk_document(full_text)

                if not chunks:
                    raise ValueError(f"No chunks generated from filing: {file_path}")

                self.processed_summary["unique_filings_processed"] += 1
                detail_entry.update(
                    status="processed",
                    message=f"Processed successfully into {len(chunks)} chunks.",
                    num_chunks=len(chunks),
                    num_tables=len(tables),
                    content_hash=content_hash
                )
                logger.info("Filing processed", cik=cik, filing_type=filing_type, accession_number=accession_number, num_chunks=len(chunks), num_tables=len(tables))

            except ValueError as ve:
                self.processed_summary["parsing_errors"] += 1
                detail_entry.update(status="failed", message=f"Processing error: {ve}")
                logger.error("Processing failed for filing", cik=cik, filing_type=filing_type, accession_number=accession_number, error=str(ve))

            except Exception as e:
                self.processed_summary["parsing_errors"] += 1
                detail_entry.update(status="failed", message=f"Unexpected error during processing: {e}")
                logger.error("Unexpected error during processing", cik=cik, filing_type=filing_type, accession_number=accession_number, error=str(e))

            self.processed_summary["details"].append(detail_entry)

        logger.info("Full SEC pipeline run completed.")
        return self.processed_summary

# --- Execution Block ---

async def run_full_pipeline_demo():
    pipeline = SECPipeline()
    sample_ciks_for_full_run = ["0000320193", "0001318605"] # Apple, Tesla
    sample_filing_types_for_full_run = ["10-K", "10-Q"]

    # Let's try downloading 1 filing of each type for each company.
    # If the registry is persistent, subsequent runs will skip duplicates.
    pipeline_report = await pipeline.run_pipeline(
        ciks=sample_ciks_for_full_run,
        filing_types=sample_filing_types_for_full_run,
        after_date="2022-01-01",
        limit=1
    )
    return pipeline_report

# Note: The 'await' keyword at the top level works in Jupyter Notebooks.
# If running this as a standalone script, wrap this in asyncio.run(run_full_pipeline_demo())
final_pipeline_report = await run_full_pipeline_demo()

# Display the summary report
print("\n" + "="*50)
print("SEC EDGAR Pipeline Summary Report")
print("="*50)
print(f"Total attempted downloads (based on CIKs * FilingTypes * Limit): {final_pipeline_report['attempted_downloads']}")
print(f"Unique filings successfully processed: {final_pipeline_report['unique_filings_processed']}")
print(f"Filings skipped due to deduplication: {final_pipeline_report['skipped_duplicates']}")
print(f"Filings failed during parsing/chunking: {final_pipeline_report['parsing_errors']}")

print("\n--- Details of Processed Filings ---")
for detail in final_pipeline_report['details']:
    print(f" CIK: {detail['cik']}, Type: {detail['filing_type']}, Acc #: {detail['accession_number']}, Status: {detail['status']}, Message: {detail['message']}")
print("="*50)