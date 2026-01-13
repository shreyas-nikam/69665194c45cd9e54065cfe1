id: 69665194c45cd9e54065cfe1_documentation
summary: SEC EDGAR Pipeline Documentation
feedback link: https://docs.google.com/forms/d/e/1FAIpQLSfWkOK-in_bMMoHSZfcIvAeO58PAH9wrDqcxnJABHaxiDqhSA/viewform?usp=sf_link
environments: Web
status: Published
# Building a Robust SEC EDGAR Data Pipeline with Streamlit

## 1. Understanding the SEC EDGAR Pipeline: Foundation for Financial Intelligence
Duration: 0:05

Welcome to a critical mission at "QuantInsight Analytics"! As a Software Developer, you're tasked with building a robust, reliable, and scalable pipeline for ingesting public financial disclosures from the SEC EDGAR database. This pipeline is the backbone of our firm's operations, powering everything from regulatory compliance and market research to sophisticated investment analysis and AI-driven insights.

The challenge lies in handling the volume and variety of SEC filings (like 10-K, 10-Q, 8-K, DEF 14A), adhering to SEC's API guidelines (especially rate limits), and ensuring data quality through deduplication and intelligent document segmentation. This codelab will guide you through implementing each core component, addressing real-world requirements for financial data ingestion. Your work here will ensure our analysts and AI models always have access to clean, non-redundant, and properly formatted financial data.

### Key Objectives
*   **Remember:** List SEC filing types (10-K, 10-Q, 8-K, DEF-14A) and their significance.
*   **Understand:** Explain why document chunking affects extraction quality and AI model performance.
*   **Apply:** Implement a complete SEC download and parsing pipeline.
*   **Analyze:** Compare PDF vs. HTML extraction strategies.

### Tools Introduced
| Tool                   | Purpose                      | Why This Tool                                  |
| : | : | : |
| `sec-edgar-downloader` | SEC filing retrieval         | Official SEC API wrapper, handles basic downloads |
| `pdfplumber`           | PDF text and table extraction | Highly accurate for structured PDF data        |
| `PyMuPDF (fitz)`       | PDF processing               | Fast, memory-efficient PDF document handling   |
| `BeautifulSoup`        | HTML parsing                 | Powerful for navigating and extracting from HTML DOM |
| `asyncio` & `httpx`    | Asynchronous operations      | Enables non-blocking I/O, crucial for rate limiting |

### Key Concepts
*   **SEC Filing Types:** Understanding the various types of disclosures and their content.
*   **Document Chunking Strategies:** Techniques for splitting large documents into smaller, context-rich segments suitable for AI models.
*   **Content Deduplication:** Methods to prevent reprocessing and storage of identical documents.
*   **Rate Limiting:** Adhering to API usage policies to ensure continuous, compliant data acquisition.
*   **Asynchronous Programming:** Utilizing `asyncio` for efficient, non-blocking operations, especially relevant for network requests.

<aside class="positive">
<b>Why is this important for AI?</b> Financial documents are often very long. Large Language Models (LLMs) have token limits. Without intelligent chunking and parsing, you cannot effectively feed these documents to an LLMs for summarization, Q&A, or sentiment analysis. Deduplication saves computational resources and improves data quality for training and inference.
</aside>



## 2. Configuration & Pipeline Initialization
Duration: 0:10

Before we begin downloading and processing, we need to configure our pipeline. These settings are crucial for compliance with SEC guidelines and efficient data handling.

The core of our pipeline is encapsulated in the `SECPipeline` class, which orchestrates all operations. Let's look at the configuration parameters and the architectural considerations for rate limiting and document chunking.

### Architecting the Rate-Limited SEC Filing Downloader

The SEC EDGAR system has strict rate limits (generally advising approximately 10 requests per second) to prevent abuse. Ignoring these limits can lead to IP blocks, disrupting our data flow. Our downloader must inherently respect these limits.

To enforce this, we introduce a delay $T_{delay}$ between consecutive download requests. If the desired maximum rate limit is $R_{max}$ requests per second, then the minimum delay needed between requests is:

$$ T_{delay} = \frac{1}{R_{max}} $$

For example, for $R_{max} = 10$ requests/second, $T_{delay} = \frac{1}{10} = 0.1$ seconds. This ensures we stay within the guidelines.

### Document Chunking Strategy

Modern AI models have token limits. Sending an entire, multi-page SEC filing to an LLM is often infeasible and inefficient. Smaller, context-rich chunks lead to better retrieval and generation quality. For QuantInsight Analytics, our AI applications need document segments that are small enough to fit within model contexts (e.g., 500-1000 tokens) yet large enough to retain meaningful information.

This process, known as 'chunking,' involves splitting a long document into smaller, manageable pieces. A simple yet effective strategy is fixed-size chunking with overlap to preserve context across boundaries.

If a document has $TotalTokens$ and we want chunks of $ChunkSize$ tokens with an $OverlapSize$ tokens, the number of chunks $N_{chunks}$ can be roughly calculated as:

$$ N_{chunks} = \lceil \frac{TotalTokens - OverlapSize}{ChunkSize - OverlapSize} \rceil $$

For simplicity in our implementation, we will use word count as a proxy for tokens.

### Pipeline Configuration Parameters in Streamlit

In the Streamlit application, these parameters are set via text inputs and number inputs:

```python
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
    max_value=int(st.session_state.chunk_size * 0.5),
    value=st.session_state.chunk_overlap, 
    step=10,
    key="chunk_overlap_input",
    help="The number of words to overlap between consecutive chunks to maintain context."
)
```

Once configured, the `SECPipeline` instance is created:

```python
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
```

This initialization sets up the necessary directories and prepares the `SECPipeline` object with all its sub-components (`SECDownloader`, `DocumentParser`, `DocumentChunker`).



## 3. Implementing the SEC Downloader
Duration: 0:15

The `SECDownloader` class is responsible for interacting with the SEC EDGAR database, downloading filings while strictly adhering to rate limits. It leverages the `sec-edgar-downloader` library for the actual download, but wraps it with asynchronous rate-limiting logic.

### Rate Limiting Mechanism

The `_rate_limit_pause` method is crucial. It ensures that there's a minimum delay between consecutive requests, preventing our IP from being blocked by the SEC. We use `asyncio.sleep` to non-blockingly pause the execution.

```python
class SECDownloader:
    # ... (initializer)

    RATE_LIMIT_DELAY = 0.1 # 10 requests per second

    async def _rate_limit_pause(self):
        """Ensures requests are rate-limited."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            await asyncio.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.monotonic()
```

### Downloading Filings

The `download_filing` method orchestrates the download. Since `sec-edgar-downloader` is a synchronous library, we use `asyncio.to_thread` to run its `get` method in a separate thread. This prevents blocking the main `asyncio` event loop of the Streamlit application, maintaining responsiveness.

The method also includes logic to identify newly downloaded files by comparing the directory contents before and after the download. It then heuristically extracts accession numbers to provide useful metadata.

```python
class SECDownloader:
    # ... (initializer and _rate_limit_pause)

    async def download_filing(self, cik: str, filing_type: str, after_date: str, limit: int = 1):
        """
        Downloads SEC filings for a given CIK and filing type, respecting rate limits.
        Returns a list of downloaded file paths and accession numbers.
        """
        downloaded_metadata = []
        try:
            await self._rate_limit_pause()
            
            # Construct the expected target directory path
            filing_type_folder_name = filing_type.replace('/', '_')
            target_dir = os.path.join(self.download_dir, cik, filing_type_folder_name)
            os.makedirs(target_dir, exist_ok=True)

            files_before = set()
            for root, _, fnames in os.walk(target_dir):
                for fname in fnames:
                    files_before.add(os.path.join(root, fname))
            
            # Run the synchronous downloader in a separate thread
            await asyncio.to_thread(self.downloader.get, filing_type, cik=cik, after=after_date, limit=limit)

            files_after = set()
            os.makedirs(target_dir, exist_ok=True) 
            for root, _, fnames in os.walk(target_dir):
                for fname in fnames:
                    files_after.add(os.path.join(root, fname))
            
            new_files = list(files_after - files_before)

            for file_path in new_files:
                match = re.search(r'(\d{10}-\d{2}-\d{6})\.txt', os.path.basename(file_path))
                if match:
                    accession_number = match.group(1).replace('-', '') 
                    downloaded_metadata.append({
                        "cik": cik, "filing_type": filing_type, 
                        "accession_number": accession_number, "path": file_path
                    })
                else:
                    downloaded_metadata.append({
                        "cik": cik, "filing_type": filing_type, 
                        "accession_number": os.path.basename(file_path), "path": file_path
                    })

        except Exception as e:
            st.error(f"Error downloading {filing_type} for CIK {cik}: {e}")
        return downloaded_metadata
```

<aside class="negative">
It's crucial to properly configure the company name and email address for the `User-Agent` string. The SEC may block requests lacking this information or using generic strings.
</aside>



## 4. Building the Document Registry for Deduplication
Duration: 0:10

Duplicate data leads to wasted processing, skewed analytics, and increased storage costs. For QuantInsight Analytics, reprocessing the same SEC filing multiple times is inefficient and can introduce inconsistencies. To combat this, we implement a "Document Registry" that uses cryptographic hashing.

### Cryptographic Hashing for Uniqueness

We use the SHA-256 algorithm to generate a unique "fingerprint" for each document's content. SHA-256 takes an input (the document's text content) and produces a fixed-size, unique hash value. The probability of two different documents producing the same SHA-256 hash is astronomically small, making it ideal for deduplication.

Mathematically, a cryptographic hash function $H$ maps an arbitrary-size input $M$ (our document content) to a fixed-size output $h$ (the hash value), such that:

$$ h = H(M) $$

1.  It is easy to compute $h = H(M)$.
2.  It is computationally infeasible to invert $H$ (find $M$ from $h$).
3.  It is computationally infeasible to find two different inputs $M_1 \neq M_2$ such that $H(M_1) = H(M_2)$ (collision resistance).

For SHA-256, the output is a 256-bit (64-character hexadecimal) string. We store these hashes in our registry to quickly check if a document has already been processed.

### Registry Management

The `SECPipeline` class manages the document registry.

*   `_load_registry()`: Reads previously processed document hashes from a file, populating a `set` for efficient lookup.
*   `_save_registry()`: Writes the current set of unique document hashes back to the file.
*   The `run_pipeline` method checks `content_hash in self.registry` before processing a document.

```python
class SECPipeline:
    # ... (initializer)

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

    # ... (run_pipeline method where hashing and registry check occurs)
```

In the `run_pipeline` method, after parsing the text content, the hash is computed and checked:

```python
# ... inside run_pipeline method
                        text_content = parsed_content["text"]
                        
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
# ...
```



## 5. Crafting a Universal Document Parser (HTML & PDF)
Duration: 0:20

SEC filings come in various formats, primarily HTML and occasionally PDF. To make this raw data useful for QuantInsight Analytics' AI models and analysts, we need to extract clean, readable text and structured tables, regardless of the original document format. This task is crucial for downstream natural language processing (NLP) and quantitative analysis.

The `DocumentParser` class handles both HTML and PDF formats.

```python
class DocumentParser:
    """
    Parses SEC filings (HTML or PDF) to extract clean text.
    """
    def parse_filing(self, file_path: str) -> dict:
        """
        Parses a document (HTML or PDF) and extracts its text content.
        Returns a dictionary with 'text' and potentially 'tables'.
        """
        if file_path.lower().endswith(".pdf"):
            return self._parse_pdf(file_path)
        
        # Check if .txt file contains HTML
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content_sample = f.read(4096) 
            
            if "<html" in content_sample.lower() or "<!doctype html>" in content_sample.lower():
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    full_content = f.read()
                return self._parse_html_string(full_content)
            else:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    full_content = f.read()
                return {"text": full_content, "tables": []}
        except Exception as e:
            st.warning(f"Could not determine type or read file {file_path}, attempting as plain text: {e}")
            try: # Fallback to plain text if initial detection or read fails
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    full_content = f.read()
                return {"text": full_content, "tables": []}
            except Exception as e_fallback:
                raise ValueError(f"Failed to parse file {file_path} as any known type: {e_fallback}")
```

### HTML Parsing (`_parse_html_string`)

For HTML documents (often downloaded as `.txt` files containing HTML), we use `BeautifulSoup`. It allows us to:
1.  Remove non-content elements like scripts, styles, headers, and footers.
2.  Extract all visible text.
3.  Clean up excessive whitespace.
4.  Perform basic table extraction, converting HTML tables into Pandas DataFrames (stored as lists of dictionaries for JSON compatibility).

```python
class DocumentParser:
    # ... (parse_filing method)

    def _parse_html_string(self, html_content: str) -> dict:
        """Parses an HTML string to extract text and tables."""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove scripts, styles, and other non-content elements
        for script_or_style in soup(["script", "style", "header", "footer", "nav", "meta", "link", "noscript"]):
            script_or_style.decompose()

        text = soup.get_text(separator='\n')
        text = re.sub(r'\n\s*\n', '\n\n', text) 
        text = re.sub(r' +', ' ', text)         
        text = text.strip()

        tables = []
        for table_tag in soup.find_all('table'):
            table_data = []
            for row in table_tag.find_all('tr'):
                cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
                table_data.append(cols)
            if table_data:
                try:
                    df = pd.DataFrame(table_data[1:], columns=table_data[0]) if table_data and len(table_data) > 1 else pd.DataFrame(table_data)
                    tables.append(df.to_dict('records'))
                except Exception:
                    tables.append(table_data)

        return {"text": text, "tables": tables}
```

### PDF Parsing (`_parse_pdf`)

For PDF documents, `pdfplumber` is used. It's excellent for extracting text and tables from structured PDFs.

1.  It iterates through each page of the PDF.
2.  `page.extract_text()` extracts text content.
3.  `page.extract_tables()` extracts structured tables, which are then converted to Pandas DataFrames.
4.  The extracted text is then cleaned similarly to HTML text.

```python
class DocumentParser:
    # ... (parse_filing and _parse_html_string methods)

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
                    
                    page_tables = page.extract_tables()
                    for pt in page_tables:
                        if pt:
                            try:
                                df = pd.DataFrame(pt[1:], columns=pt[0]) if pt and len(pt) > 1 else pd.DataFrame(pt)
                                tables.append(df.to_dict('records'))
                            except Exception:
                                tables.append(pt)
            
            text = re.sub(r'\n\s*\n', '\n\n', text)
            text = re.sub(r' +', ' ', text)
            text = text.strip()

        except Exception as e:
            st.error(f"Error parsing PDF {file_path}: {e}")
            return {"text": "", "tables": []} 
        
        return {"text": text, "tables": tables}
```

The goal is to produce "clean text output" – free of extraneous formatting – and separate, structured tabular data.



## 6. Implementing Document Chunking
Duration: 0:10

After parsing, large documents need to be broken down into smaller, manageable chunks. This process is handled by the `DocumentChunker` class, which implements a fixed-size chunking strategy with configurable overlap.

### Chunking Logic

The `chunk_document` method takes the entire text content of a document and splits it into a list of strings, each representing a chunk.

*   **`chunk_size`**: The target number of words for each chunk.
*   **`chunk_overlap`**: The number of words that will be repeated at the end of one chunk and the beginning of the next, to maintain context.

```python
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
        
        self.chunk_overlap = max(0, min(self.chunk_overlap, self.chunk_size - 1))
        
        while start_idx < len(words):
            end_idx = min(start_idx + self.chunk_size, len(words))
            chunk = " ".join(words[start_idx:end_idx])
            chunks.append(chunk)
            
            if end_idx == len(words):
                break 
            
            start_idx += (self.chunk_size - self.chunk_overlap)
            
            if start_idx >= len(words):
                 break
        
        # Post-processing: Merge small last chunk with previous one
        if chunks and len(chunks) > 1:
            last_chunk_len = len(chunks[-1].split())
            if last_chunk_len < self.chunk_size * 0.5:
                chunks[-2] = " ".join([chunks[-2], chunks[-1]])
                chunks.pop()

        return chunks
```

The post-processing step for merging small trailing chunks is a practical optimization to prevent very short chunks that might lack sufficient context for downstream AI tasks.

<aside class="positive">
Experiment with different `chunk_size` and `chunk_overlap` values based on the specific LLM you are using and the nature of the questions you want to ask. Larger overlap might capture more context but also increases redundancy.
</aside>



## 7. Orchestrating the End-to-End Pipeline and Analysis
Duration: 0:15

With all individual components ready, the `SECPipeline` class brings them together into a seamless, end-to-end data processing workflow. This final step orchestrates the entire process and generates a clear report of its operations, detailing what was processed, what was skipped, and any issues encountered.

### The `run_pipeline` Method

This asynchronous method iterates through specified CIKs and filing types, performing the following steps for each document:

1.  **Download:** Calls `SECDownloader.download_filing`.
2.  **Parse:** Calls `DocumentParser.parse_filing` to extract text and tables.
3.  **Deduplicate:** Computes a SHA-256 hash of the extracted text and checks against the `registry`. If it's a duplicate, it's skipped.
4.  **Chunk:** Calls `DocumentChunker.chunk_document` to split the unique text into smaller pieces.
5.  **Report:** Aggregates statistics and details about each filing's processing status.

```python
class SECPipeline:
    # ... (initializer, _load_registry, _save_registry)

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
                downloaded_meta = await self.downloader.download_filing(cik, f_type, after_date, limit)
                
                for meta in downloaded_meta:
                    report["attempted_downloads"] += 1
                    file_path = meta["path"]
                    accession_number = meta["accession_number"]
                    
                    if not os.path.exists(file_path):
                        # ... handle file not found error ...
                        continue

                    try:
                        parsed_content = self.parser.parse_filing(file_path)
                        text_content = parsed_content["text"]

                        if not text_content.strip():
                            # ... handle empty text error ...
                            continue

                        content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

                        if content_hash in self.registry:
                            # ... handle skipped duplicate ...
                            continue

                        self.registry.add(content_hash)
                        self._save_registry()

                        chunks = self.chunker.chunk_document(text_content)

                        report["unique_filings_processed"] += 1
                        report["details"].append({
                            "cik": cik, "filing_type": f_type, "accession_number": accession_number,
                            "status": "processed", "path": file_path, "num_chunks": len(chunks)
                        })

                    except Exception as e:
                        # ... handle parsing error ...
                        pass
        return report
```

### Streamlit UI for Execution and Reporting

The Streamlit application provides an interface to define the CIKs, filing types, and date filters. The pipeline execution is triggered by a button that calls an asynchronous wrapper function:

```python
# Streamlit UI elements for CIKs, filing types, date, limit
st.session_state.ciks_input = st.text_area("CIKs...", st.session_state.ciks_input)
ciks_list = [re.sub(r'\D', '', cik) for cik in st.session_state.ciks_input.split('\n') if cik.strip()]
st.session_state.filing_types_selected = st.multiselect("Select Filing Types", options=SECDownloader.FILING_TYPES, default=st.session_state.filing_types_selected)
st.session_state.after_date_input = st.date_input("Download filings after date", st.session_state.after_date_input)
st.session_state.limit_input = st.number_input("Max number of filings per type per CIK", min_value=1, max_value=100, value=st.session_state.limit_input)

async def run_pipeline_async():
    if not ciks_list:
        st.error("Please enter at least one CIK.")
        st.session_state.processing_in_progress = False
        return

    st.session_state.processing_in_progress = True
    with st.spinner("Running pipeline..."):
        try:
            report = await st.session_state.sec_pipeline_instance.run_pipeline(
                ciks=ciks_list,
                filing_types=st.session_state.filing_types_selected,
                after_date=st.session_state.after_date_input.strftime("%Y-%m-%d"),
                limit=st.session_state.limit_input
            )
            st.session_state.pipeline_report = report
            st.success("Pipeline execution completed!")
            st.session_state.current_page = 'Processed Filings & Analysis' # Navigate to results
        except Exception as e:
            st.error(f"Pipeline execution failed: {e}")
        finally:
            st.session_state.processing_in_progress = False
            st.rerun()

st.button(
    "Run SEC Data Pipeline", 
    on_click=run_pipeline_async,
    disabled=st.session_state.processing_in_progress or not ciks_list
)
```

After the pipeline completes, a comprehensive report is displayed, summarizing the number of attempted downloads, unique filings processed, skipped duplicates, and parsing errors. A detailed log provides line-by-line information for each filing.

Users can also select a successfully processed filing from a dropdown to view its full extracted text and the resulting chunks, enabling immediate verification of the parsing and chunking quality. This interactive visualization is key for developers to understand and debug the pipeline's output.

This comprehensive pipeline ensures that QuantInsight Analytics always has access to high-quality, relevant financial data for its analytical and AI initiatives.
