
# SEC EDGAR Data Pipeline: A Developer's Workflow for Financial Data Ingestion

## Introduction: Building the Foundation for Financial Intelligence

Welcome to the heart of financial data infrastructure! As a Software Developer at "QuantInsight Analytics," a cutting-edge financial data analytics firm, your current mission is critical: to establish a robust, reliable, and scalable pipeline for ingesting public financial disclosures from the SEC EDGAR database. Our firm relies on this data for everything from regulatory compliance and market research to sophisticated investment analysis and AI-driven insights.

The challenge lies in handling the volume and variety of SEC filings (10-K, 10-Q, 8-K, DEF 14A), adhering to SEC's API guidelines (especially rate limits), and ensuring data quality through deduplication and intelligent document segmentation. This notebook will walk you through building the core components of this pipeline, demonstrating how each step addresses a real-world requirement for QuantInsight Analytics. You will implement a rate-limited downloader, a content-hash-based registry for deduplication, a versatile parser for HTML and PDF filings, and a smart chunking mechanism for AI readiness.

Your work here lays the groundwork for all downstream applications, ensuring that our analysts and AI models always have access to clean, non-redundant, and properly formatted financial data.

---

## 1. Setting Up the Development Environment

Before we dive into building the pipeline, let's install the necessary Python libraries. These tools will enable us to interact with the SEC EDGAR database, parse different document formats, and perform data manipulation.

```python
# Install required libraries
!pip install sec-edgar-downloader beautifulsoup4 pdfplumber pymupdf httpx structlog tqdm
```

```python
# Import necessary dependencies
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
```

---

## 2. Architecting the Rate-Limited SEC Filing Downloader

### Story + Context + Real-World Relevance

As a Software Developer at QuantInsight Analytics, the first critical component of our pipeline is a reliable SEC filing downloader. The SEC EDGAR system has strict rate limits to prevent abuse and ensure fair access for all users. Ignoring these limits can lead to our IP address being temporarily or permanently blocked, completely disrupting our data flow. Our task is to build a downloader that inherently respects these limits, ensuring a continuous and compliant data acquisition process. The SEC generally advises approximately 10 requests per second. To be safe and compliant, we will introduce a delay between consecutive download requests.

The concept of rate limiting is crucial here. It prevents a "denial of service" from our end, where too many requests overwhelm the server. A simple way to enforce this is to introduce a delay $T_{delay}$ between requests. If the desired rate limit is $R$ requests per second, then the minimum delay needed between requests is $T_{delay} = \frac{1}{R}$ seconds. For example, if $R = 0.1$ requests/second (i.e., 1 request every 10 seconds to be very conservative, or $R=10$ requests/second, meaning $T_{delay}=0.1$ seconds), we ensure we stay within the guidelines.

$$ T_{delay} = \frac{1}{R_{max}} $$

Where $R_{max}$ is the maximum allowed requests per second. For SEC EDGAR, we will aim for a conservative delay of 0.1 seconds (10 requests/second).

```python
# Code cell (function definition + function execution)

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
                    cik=cik,
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
                            
                            file_path = html_file or pdf_file
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
```

### Explanation of Execution

The output above shows a log of the download process for Apple and Microsoft's 10-K and 10-Q filings. You'll observe `logger.info` messages indicating successful downloads and potentially `logger.error` if any issues occurred (e.g., network problems, invalid CIK). The `asyncio.sleep` calls, though not explicitly visible in the final log, ensure that requests are spaced out, demonstrating our adherence to SEC rate limits. The `tqdm` progress bar visually confirms that the asynchronous operations are taking place with appropriate delays.

For QuantInsight Analytics, this means we have a compliant and robust method to continuously fetch the latest financial disclosures without risking service interruptions. The `downloaded_filings_metadata` list now contains vital information for each downloaded filing, including its CIK, type, accession number, and the local file path, which will be crucial for the next stages of our pipeline.

---

## 3. Building the Document Registry for Deduplication

### Story + Context + Real-World Relevance

Duplicate data is a common headache in data pipelines, leading to wasted processing, skewed analytics, and increased storage costs. For QuantInsight Analytics, processing the same SEC filing multiple times would be inefficient and could lead to inconsistencies in our financial models. To combat this, we need a robust mechanism to identify and prevent the reprocessing of identical documents.

The solution is a "Document Registry" that uses cryptographic hashing to generate a unique "fingerprint" for each document's content. We will use the SHA-256 algorithm, which takes an input (the document's text content) and produces a fixed-size, unique hash value. The probability of two different documents producing the same SHA-256 hash is astronomically small, making it ideal for deduplication.

Mathematically, a cryptographic hash function $H$ maps an arbitrary-size input $M$ (our document content) to a fixed-size output $h$ (the hash value), such that:
1.  It is easy to compute $h = H(M)$.
2.  It is computationally infeasible to invert $H$ (find $M$ from $h$).
3.  It is computationally infeasible to find two different inputs $M_1 \neq M_2$ such that $H(M_1) = H(M_2)$ (collision resistance).

For SHA-256, the output is a 256-bit (64-character hexadecimal) string. We will store these hashes in our registry to quickly check if a document has already been processed.

```python
# Code cell (function definition + function execution)

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
            self._save_registry() # Persist the registry change

# Example Usage:
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
```

### Explanation of Execution

The output demonstrates the core functionality of our `DocumentRegistry`. We compute SHA-256 hashes for different pieces of text. Notice that `hash_1` and `hash_1_dup` are identical, as expected, showcasing the deterministic nature of cryptographic hashing. `hash_3_diff` is different due to a subtle change (a trailing space), highlighting the sensitivity of hashes to input variations.

When we mark `hash_1` as processed, subsequent checks for `hash_1` and `hash_1_dup` correctly return `True`. This confirms that our registry can effectively identify and prevent reprocessing of duplicate content. For QuantInsight Analytics, this means that once a filing is processed, we won't waste valuable compute resources on it again, regardless of how many times it might reappear in subsequent downloads or pipeline runs. The `document_registry.txt` file also persists this state, making the pipeline robust to restarts.

---

## 4. Crafting a Universal Document Parser (HTML & PDF)

### Story + Context + Real-World Relevance

SEC filings come in various formats, primarily HTML and occasionally PDF, each with its own structural nuances. To make this raw data useful for QuantInsight Analytics' AI models and analysts, we need to extract clean, readable text and structured tables, regardless of the original document format. This task is crucial for downstream natural language processing (NLP) and quantitative analysis.

A robust `DocumentParser` needs to:
*   **HTML Parsing**: Navigate the often complex and inconsistently structured HTML of SEC filings, focusing on extracting the main textual content and identifying tables. Libraries like `BeautifulSoup` excel at this by providing an intuitive way to traverse the HTML DOM.
*   **PDF Parsing**: Handle PDF documents, which can be challenging due to their visual layout rather than semantic structure. Tools like `pdfplumber` or `PyMuPDF` (Fitz) are essential for extracting text, tables, and even layout information from PDFs.

The goal is to produce "clean text output" – free of HTML tags, scripts, and extraneous formatting – and separate, structured tabular data.

```python
# Code cell (function definition + function execution)

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
                if cells: # Only add if it's a data row
                    rows.append(cells)
            
            if headers and rows: # Only add if we found both headers and rows
                tables_data.append({"headers": headers, "rows": rows})
            elif rows and not headers: # Sometimes tables might not have th tags, but still have rows
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
                        if table and table[0]: # Ensure table and header row exist
                            headers = table[0]
                            rows = table[1:]
                            tables_data.append({"page": page_num + 1, "headers": headers, "rows": rows})
                        elif table: # If no explicit headers, just provide rows
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
            if not full_text: # If pdfplumber didn't get much, use pymupdf
                 full_text = pymupdf_text
            elif len("".join(full_text)) < len("".join(pymupdf_text)): # If pymupdf got more text
                 full_text = pymupdf_text
        except Exception as e:
            logger.warning(f"PyMuPDF text extraction failed for {file_path}: {e}")
            
        final_text = "\n".join(full_text).strip()
        final_text = re.sub(r'\n\s*\n', '\n', final_text) # Clean up whitespace

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

# Example Usage:
document_parser = DocumentParser()

# To demonstrate, we need an actual HTML and potentially a PDF file.
# We'll use one of the downloaded HTML files and simulate a PDF.

# Use the first downloaded HTML filing for demonstration
if downloaded_filings_metadata:
    html_file_path = downloaded_filings_metadata[0]["path"]
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
```

### Explanation of Execution

The `DocumentParser` successfully extracts text and tabular data from both a sample SEC HTML filing and a synthetic PDF document.

For the HTML filing, we can see a snippet of the cleaned text (free of HTML tags and excessive whitespace) and the first identified table structure. `BeautifulSoup` effectively navigated the HTML to get the main content.

For the synthetic PDF, `pdfplumber` and `PyMuPDF` collaboratively extracted the text and the table, demonstrating the parser's ability to handle different formats. The extracted text provides the narrative, while the table extraction gives us structured data like "Revenue" and "Net Income" – immediately useful for quantitative analysis.

This capability is vital for QuantInsight Analytics. Our analysts can now receive clean, digestible reports, and our AI models can be trained on consistent text data, irrespective of the original filing format, significantly reducing the manual effort of data preparation and improving the accuracy of our insights.

---

## 5. Implementing Smart Document Chunking for AI Readiness

### Story + Context + Real-World Relevance

Modern AI models, especially Large Language Models (LLMs), have token limits. Sending an entire, multi-page SEC filing to an LLM is often infeasible and inefficient. Moreover, smaller, context-rich chunks lead to better retrieval and generation quality. For QuantInsight Analytics, our AI applications need document segments that are small enough to fit within model contexts (e.g., 500-1000 tokens) yet large enough to retain meaningful information and context.

This process, known as "chunking," involves splitting a long document into smaller, manageable pieces. A simple yet effective strategy is fixed-size chunking, where we split the text based on a word or token count, ensuring some overlap between chunks to preserve context across boundaries.

If a document has $TotalTokens$ and we want chunks of $ChunkSize$ tokens with an $OverlapSize$ tokens, the number of chunks $N_{chunks}$ can be roughly calculated as:
$$ N_{chunks} = \lceil \frac{TotalTokens - OverlapSize}{ChunkSize - OverlapSize} \rceil $$
For simplicity, we will implement fixed-size chunking based on word count (as a proxy for tokens) with no explicit overlap for this demonstration, aiming for chunks between 500-1000 words.

```python
# Code cell (function definition + function execution)

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
```

### Explanation of Execution

The `DocumentChunker` successfully divided a long SEC filing into smaller, more manageable chunks. We can see the total word count of the original document and then the number of chunks produced. The output shows the beginning of the first two chunks and their respective word counts, confirming that they adhere to our specified `chunk_size` (approximately 800 words).

The "Conceptual Overlap Check" illustrates how the `chunk_overlap` parameter helps maintain context between segments, which is vital for AI models that might need to stitch together information from adjacent chunks. This prevents abrupt breaks in narrative that could mislead an LLM or impact retrieval accuracy.

For QuantInsight Analytics, this means that lengthy financial reports are no longer black boxes for our AI. We can feed these context-rich chunks into our LLMs for summarization, Q&A, sentiment analysis, or into vector databases for semantic search, ensuring that our AI-powered applications operate effectively and provide accurate, timely insights from the vast sea of SEC data.

---

## 6. Orchestrating the End-to-End SEC Data Pipeline and Reporting

### Story + Context + Real-World Relevance

Now that we have all the individual components – the rate-limited downloader, the deduplication registry, the universal parser, and the smart chunker – it's time to integrate them into a seamless, end-to-end data pipeline. As a Software Developer, your responsibility is to ensure that these modules work together harmoniously, transforming raw SEC filings into ready-to-use data for QuantInsight Analytics. This final step involves orchestrating the entire workflow and generating a clear report of its operations, detailing what was processed, what was skipped, and any issues encountered.

This orchestration demonstrates how all the individual concepts (rate-limiting, content hashing, parsing, chunking) are applied in a real-world, sequential workflow to solve a complex data ingestion problem. The output report will serve as an audit trail for the pipeline's performance and data integrity.

```python
# Code cell (function definition + function execution)

class SECPipeline:
    """
    Orchestrates the end-to-end SEC data ingestion pipeline.
    """
    def __init__(self, download_dir: str = "./sec_filings",
                 registry_file: str = "document_registry.txt",
                 chunk_size: int = 750, chunk_overlap: int = 50):
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

        for cik in async_tqdm(ciks, desc="Processing CIKs"):
            self.processed_summary["attempted_downloads"] += (len(filing_types or self.downloader.FILING_TYPES) * limit)
            download_results = await self.downloader.download_company_filings(
                cik=cik,
                filing_types=filing_types,
                after_date=after_date,
                limit=limit
            )
            all_download_metadata.extend(download_results)

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
                # 1. Parse Document
                parsed_content = self.parser.parse_filing(file_path)
                full_text = parsed_content["text"]
                tables = parsed_content["tables"]

                if not full_text:
                    raise ValueError(f"No text extracted from filing: {file_path}")

                # 2. Deduplicate using Content Hash
                content_hash = self.registry.compute_content_hash(full_text)
                if self.registry.is_processed(content_hash):
                    self.processed_summary["skipped_duplicates"] += 1
                    detail_entry.update(status="skipped", message="Duplicate filing (content hash exists)")
                    logger.info("Skipped duplicate filing", cik=cik, filing_type=filing_type,
                                accession_number=accession_number, content_hash=content_hash)
                    self.processed_summary["details"].append(detail_entry)
                    continue
                
                # Mark as processed BEFORE chunking to prevent re-processing if chunking fails
                self.registry.mark_as_processed(content_hash)
                filing_meta["processed_hash"] = content_hash

                # 3. Chunk Document
                chunks = self.chunker.chunk_document(full_text)
                if not chunks:
                    raise ValueError(f"No chunks generated from filing: {file_path}")

                self.processed_summary["unique_filings_processed"] += 1
                detail_entry.update(status="processed", message=f"Processed successfully into {len(chunks)} chunks.",
                                    num_chunks=len(chunks), num_tables=len(tables), content_hash=content_hash)
                logger.info("Filing processed", cik=cik, filing_type=filing_type, accession_number=accession_number,
                            num_chunks=len(chunks), num_tables=len(tables))

            except ValueError as ve:
                self.processed_summary["parsing_errors"] += 1 # Or chunking errors
                detail_entry.update(status="failed", message=f"Processing error: {ve}")
                logger.error("Processing failed for filing", cik=cik, filing_type=filing_type,
                             accession_number=accession_number, error=str(ve))
            except Exception as e:
                self.processed_summary["parsing_errors"] += 1
                detail_entry.update(status="failed", message=f"Unexpected error during processing: {e}")
                logger.error("Unexpected error during processing", cik=cik, filing_type=filing_type,
                             accession_number=accession_number, error=str(e))
            
            self.processed_summary["details"].append(detail_entry)
        
        logger.info("Full SEC pipeline run completed.")
        return self.processed_summary

# Run the complete pipeline
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
    print(f"  CIK: {detail['cik']}, Type: {detail['filing_type']}, Acc #: {detail['accession_number']}, Status: {detail['status']}, Message: {detail['message']}")

print("="*50)
```

### Explanation of Execution

The final execution block orchestrates the entire SEC data pipeline. It iterates through the specified CIKs and filing types, first downloading filings with rate limiting, then parsing each one, checking against the `DocumentRegistry` for duplicates, and finally chunking the unique filings.

The `final_pipeline_report` provides a comprehensive overview:
*   **`Total attempted downloads`**: Shows the total number of unique (CIK, Filing Type, Limit) combinations that the downloader tried to fetch.
*   **`Unique filings successfully processed`**: Counts the filings that passed through all stages (download, parsing, deduplication, chunking) and were deemed unique.
*   **`Filings skipped due to deduplication`**: This is a critical metric demonstrating the value of our `DocumentRegistry`. If you run the notebook multiple times, you will likely see this number increase as the registry prevents reprocessing the same content.
*   **`Filings failed during parsing/chunking`**: Highlights any documents that could not be processed, indicating areas for potential improvement or manual review.

The detailed list of each filing's status (processed, skipped, or failed) provides granular traceability. For QuantInsight Analytics, this report is invaluable. It confirms that the pipeline is functioning as intended, respecting SEC guidelines, efficiently handling data, and ensuring that only high-quality, non-redundant, and appropriately segmented financial data is made available for downstream analysis. This robust infrastructure enables our firm to make data-driven decisions with confidence and efficiency.
