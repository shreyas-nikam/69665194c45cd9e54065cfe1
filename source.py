from typing import Any, Dict, List, Optional
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
import hashlib
import re
import asyncio
import json

import os
import pdfkit
from sec_edgar_downloader import Downloader
from bs4 import BeautifulSoup
import pdfplumber
import fitz  # PyMuPDF
import structlog
from tqdm.asyncio import tqdm as async_tqdm

logger = structlog.get_logger()


FILING_TYPES = [
    "10-K", "10-Q", "8-K", "DEF 14A"
]
# ============================================================================
# STEP 1: Initialize Pipeline
# ============================================================================


class PipelineState:
    """Holds the state of the pipeline across all steps."""

    def __init__(self, company_name: str, email_address: str, download_dir: str = "./sec_filings"):
        self.company_name = company_name
        self.email_address = email_address
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Components (initialized in later steps)
        self.downloader = None
        self.registry = None
        self.parser = None
        self.chunker = None

        # Pipeline data
        self.downloaded_filings = []
        self.parsed_filings = []
        self.deduplicated_filings = []
        self.chunked_filings = []

        # Summary
        self.summary = {
            "attempted_downloads": 0,
            "unique_filings_processed": 0,
            "skipped_duplicates": 0,
            "parsing_errors": 0,
            "details": []
        }

        logger.info("Pipeline state initialized",
                    company_name=company_name,
                    email_address=email_address)


def step1_initialize_pipeline(company_name: str, email_address: str,
                              download_dir: str = "./sec_filings") -> PipelineState:
    """
    Step 1: Initialize the pipeline with company information.

    Args:
        company_name: Your company/organization name
        email_address: Your contact email
        download_dir: Directory to store downloaded filings

    Returns:
        PipelineState object containing all pipeline state
    """
    state = PipelineState(company_name, email_address, download_dir)
    print(f"✓ Pipeline initialized for {company_name}")
    return state


# ============================================================================
# STEP 2: Add Downloader
# ============================================================================

def step2_add_downloader(state: PipelineState) -> PipelineState:
    """
    Step 2: Initialize the SEC downloader component.

    Args:
        state: Current pipeline state

    Returns:
        Updated pipeline state with downloader initialized
    """
    state.downloader = Downloader(
        company_name=state.company_name,
        email_address=state.email_address,
        download_folder=str(state.download_dir)
    )
    logger.info("SEC Downloader added to pipeline")
    print("✓ SEC Downloader initialized")
    return state


# ============================================================================
# STEP 3: Configure Rate Limiting
# ============================================================================

def step3_configure_rate_limiting(state: PipelineState,
                                  request_delay: float = 0.1) -> PipelineState:
    """
    Step 3: Configure rate limiting for SEC requests.

    Args:
        state: Current pipeline state
        request_delay: Delay between requests in seconds (default: 0.1s = 10 req/s)

    Returns:
        Updated pipeline state with rate limiting configured
    """
    state.request_delay = request_delay
    logger.info("Rate limiting configured", request_delay=request_delay)
    print(
        f"✓ Rate limiting set to {request_delay}s between requests ({1/request_delay:.0f} req/s)")
    return state


# ============================================================================
# STEP 4: Download Filings
# ============================================================================


def convert_filing_type(html_file_path: str, pdf_path: str) -> str:
    # Convert HTML to PDF using pdfkit
    with open(html_file_path, 'r') as f:
        html_content = f.read()
    options = {"load-error-handling": "ignore"}
    pdfkit.from_string(html_content, pdf_path, options=options)
    return pdf_path


async def step4_download_filings(state: PipelineState,
                                 ciks: List[str],
                                 filing_types: Optional[List[str]] = None,
                                 after_date: Optional[str] = None,
                                 limit: int = 10) -> PipelineState:
    """
    Step 4: Download SEC filings for specified companies.

    Args:
        state: Current pipeline state
        ciks: List of CIK numbers to download
        filing_types: List of filing types (e.g., ["10-K", "10-Q"])
        after_date: Download filings after this date (YYYY-MM-DD)
        limit: Maximum number of filings per type per CIK

    Returns:
        Updated pipeline state with downloaded filings metadata
    """
    if not state.downloader:
        raise ValueError(
            "Downloader not initialized. Run step2_add_downloader first.")

    filing_types = filing_types or ["10-K", "10-Q", "8-K", "DEF 14A"]
    state.downloaded_filings = []

    for cik in async_tqdm(ciks, desc="Downloading CIKs"):
        state.summary["attempted_downloads"] += len(filing_types) * limit

        for filing_type in filing_types:
            try:
                # Rate limiting
                await asyncio.sleep(state.request_delay)

                # Download
                state.downloader.get(
                    filing_type, cik, after=after_date, limit=limit)

                # Find downloaded files
                company_type_dir = state.download_dir / "sec-edgar-filings" / cik / filing_type
                if company_type_dir.exists():
                    for accession_dir in company_type_dir.iterdir():
                        if accession_dir.is_dir():
                            # Find filing file
                            html_file = next(
                                accession_dir.glob("*.html"), None)
                            pdf_file = next(accession_dir.glob("*.pdf"), None)
                            txt_file = next(accession_dir.glob("*.txt"), None)
                            file_path = html_file or pdf_file or txt_file

                            if txt_file:
                                txt_content = open(txt_file, "r").read()
                                txt_content = txt_content[:txt_content.index(
                                    "</html>")+7]
                                with open(txt_file, "w") as f:
                                    f.write(txt_content)

                                html_file = Path(txt_file).with_suffix(".html")
                                with open(html_file, "w") as f:
                                    f.write(txt_content)
                                state.downloaded_filings.append({
                                    "cik": cik,
                                    "filing_type": filing_type,
                                    "accession_number": accession_dir.name,
                                    "path": str(html_file)
                                })

                                try:
                                    print("Converting HTML to PDF:", html_file)
                                    pdf_file = str(
                                        html_file.with_suffix('.pdf'))

                                    convert_filing_type(
                                        Path(html_file).absolute(), pdf_file)
                                except Exception as e:
                                    logger.error("Conversion to PDF failed",
                                                 filing=str(html_file), error=str(e))
                            # print(pdf_file, os.path.exists(pdf_file))
                            if os.path.exists(pdf_file):
                                state.downloaded_filings.append({
                                    "cik": cik,
                                    "filing_type": filing_type,
                                    "accession_number": accession_dir.name,
                                    "path": str(pdf_file)
                                })

                logger.info("Downloaded filings", cik=cik,
                            filing_type=filing_type)

            except Exception as e:
                logger.error("Download failed", cik=cik,
                             filing_type=filing_type, error=str(e))

    print(f"✓ Downloaded {len(state.downloaded_filings)} filings")
    return state


# ============================================================================
# STEP 5: Parse Documents
# ============================================================================

class DocumentParser:
    """Parses SEC filings to extract text and tables."""

    def __init__(self):
        logger.info("DocumentParser initialized")

    def _parse_html(self, file_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
        """Extract text and tables from HTML."""

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            soup = BeautifulSoup(f, 'lxml')

        # Remove scripts and styles
        for element in soup(['script', 'style']):
            element.extract()

        # Extract text
        text = soup.get_text(separator='\n')
        text = re.sub(r'\n\s*\n', '\n', text).strip()

        # Extract tables
        tables_data = []
        for table in soup.find_all('table'):
            headers = [th.get_text(strip=True) for th in table.find_all('th')]
            rows = []
            for tr in table.find_all('tr'):
                if tr.find('th'):
                    continue
                cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                if cells:
                    rows.append(cells)

            if headers and rows:
                tables_data.append({"headers": headers, "rows": rows})
            elif rows:
                tables_data.append({"rows": rows})

        return text, tables_data

    def _parse_pdf(self, file_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
        """Extract text and tables from PDF."""
        full_text = []
        tables_data = []

        # Use pdfplumber
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if page_text:
                    full_text.append(page_text)

                # Extract tables
                page_tables = page.extract_tables()
                if page_tables:
                    for table in page_tables:
                        if table and table[0]:
                            headers = table[0]
                            rows = table[1:]
                            tables_data.append(
                                {"page": page_num + 1, "headers": headers, "rows": rows})
                        elif table:
                            tables_data.append(
                                {"page": page_num + 1, "rows": table})

        # Fallback to PyMuPDF if needed
        try:
            doc = fitz.open(file_path)
            pymupdf_text = [page.get_text("text") for page in doc]
            doc.close()

            if not full_text or len("".join(full_text)) < len("".join(pymupdf_text)):
                full_text = pymupdf_text
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")

        final_text = "\n".join(full_text).strip()
        final_text = re.sub(r'\n\s*\n', '\n', final_text)
        return final_text, tables_data

    def parse_filing(self, file_path: str) -> Dict[str, Any]:
        """Parse a filing file based on extension."""
        path = Path(file_path)

        if path.suffix in ['.html', '.txt']:
            print("Parsing HTML file:", file_path)
            text, tables = self._parse_html(path)
        elif path.suffix == '.pdf':
            print("Parsing PDF file:", file_path)
            text, tables = self._parse_pdf(path)
        else:
            logger.warning(f"Unsupported file type: {file_path}")
            text = f"Content for {path.name} (unsupported type)"
            tables = []

        return {"text": text, "tables": tables}


async def step5_parse_documents(state: PipelineState) -> PipelineState:
    """
    Step 5: Parse downloaded documents to extract text and tables.

    Args:
        state: Current pipeline state with downloaded filings

    Returns:
        Updated pipeline state with parsed content
    """
    if not state.downloaded_filings:
        raise ValueError(
            "No downloaded filings. Run step4_download_filings first.")

    state.parser = DocumentParser()
    state.parsed_filings = []

    for filing in async_tqdm(state.downloaded_filings, desc="Parsing documents"):
        try:
            parsed = state.parser.parse_filing(filing["path"])

            if not parsed["text"]:
                raise ValueError(f"No text extracted from {filing['path']}")

            filing["parsed_text"] = parsed["text"]
            filing["parsed_tables"] = parsed["tables"]
            state.parsed_filings.append(filing)

        except Exception as e:
            state.summary["parsing_errors"] += 1
            logger.error("Parsing failed", filing=filing["path"], error=str(e))
            state.summary["details"].append({
                "cik": filing["cik"],
                "filing_type": filing["filing_type"],
                "accession_number": filing["accession_number"],
                "status": "parsing_failed",
                "error": str(e)
            })
            raise e

    print(
        f"✓ Parsed {len(state.parsed_filings)} documents ({state.summary['parsing_errors']} errors)")
    return state


# ============================================================================
# STEP 6: Deduplicate Documents
# ============================================================================

class DocumentRegistry:
    """Manages document deduplication using content hashes."""

    def __init__(self, registry_file: str = "document_registry.txt"):
        self.registry_file = Path(registry_file)
        self.processed_hashes = set()
        self._load_registry()

    def _load_registry(self):
        if self.registry_file.exists():
            with open(self.registry_file, 'r') as f:
                self.processed_hashes = {line.strip() for line in f}
            logger.info("Loaded registry", count=len(self.processed_hashes))

    def _save_registry(self):
        with open(self.registry_file, 'w') as f:
            for h in self.processed_hashes:
                f.write(h + '\n')

    def compute_content_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def is_processed(self, content_hash: str) -> bool:
        return content_hash in self.processed_hashes

    def mark_as_processed(self, content_hash: str):
        if content_hash not in self.processed_hashes:
            self.processed_hashes.add(content_hash)
            self._save_registry()


def step6_deduplicate_documents(state: PipelineState,
                                registry_file: str = "document_registry.txt") -> PipelineState:
    """
    Step 6: Remove duplicate documents based on content hash.

    Args:
        state: Current pipeline state with parsed filings
        registry_file: Path to registry file for tracking processed documents

    Returns:
        Updated pipeline state with deduplicated filings
    """
    if not state.parsed_filings:
        raise ValueError("No parsed filings. Run step5_parse_documents first.")

    state.registry = DocumentRegistry(registry_file)
    state.deduplicated_filings = []
    skipped = 0

    for filing in state.parsed_filings:
        content_hash = state.registry.compute_content_hash(
            filing["parsed_text"])
        filing["content_hash"] = content_hash

        if state.registry.is_processed(content_hash):
            skipped += 1
            state.summary["skipped_duplicates"] += 1
            state.summary["details"].append({
                "cik": filing["cik"],
                "filing_type": filing["filing_type"],
                "accession_number": filing["accession_number"],
                "status": "duplicate_skipped",
                "content_hash": content_hash
            })
            logger.info("Skipped duplicate", hash=content_hash[:16])
        else:
            state.registry.mark_as_processed(content_hash)
            state.deduplicated_filings.append(filing)

    print(
        f"✓ Deduplicated: {len(state.deduplicated_filings)} unique documents ({skipped} duplicates removed)")
    return state


# ============================================================================
# STEP 7: Chunk Text
# ============================================================================

class DocumentChunker:
    """Splits documents into manageable chunks."""

    def __init__(self, chunk_size: int = 750, chunk_overlap: int = 50):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        logger.info("DocumentChunker initialized",
                    chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    def chunk_document(self, text: str) -> List[str]:
        if not text:
            return []

        words = text.split()
        chunks = []
        start_idx = 0

        while start_idx < len(words):
            end_idx = min(start_idx + self.chunk_size, len(words))
            chunk = " ".join(words[start_idx:end_idx])
            chunks.append(chunk)

            start_idx += (self.chunk_size - self.chunk_overlap)

            if start_idx >= len(words):
                break

        return chunks


async def step7_chunk_text(state: PipelineState,
                           chunk_size: int = 750,
                           chunk_overlap: int = 50) -> PipelineState:
    """
    Step 7: Split document text into manageable chunks.

    Args:
        state: Current pipeline state with deduplicated filings
        chunk_size: Target size of each chunk in words
        chunk_overlap: Number of words to overlap between chunks

    Returns:
        Updated pipeline state with chunked documents
    """
    if not state.deduplicated_filings:
        raise ValueError(
            "No deduplicated filings. Run step6_deduplicate_documents first.")

    state.chunker = DocumentChunker(chunk_size, chunk_overlap)
    state.chunked_filings = []

    for filing in async_tqdm(state.deduplicated_filings, desc="Chunking documents"):
        try:
            chunks = state.chunker.chunk_document(filing["parsed_text"])

            if not chunks:
                raise ValueError(f"No chunks generated for {filing['path']}")

            filing["chunks"] = chunks
            filing["num_chunks"] = len(chunks)
            state.chunked_filings.append(filing)

            state.summary["unique_filings_processed"] += 1
            state.summary["details"].append({
                "cik": filing["cik"],
                "filing_type": filing["filing_type"],
                "accession_number": filing["accession_number"],
                "status": "success",
                "num_chunks": len(chunks),
                "parsed_tables": filing.get("parsed_tables"),
                "num_tables": len(filing.get("parsed_tables", [])),
                "content_hash": filing["content_hash"]
            })

        except Exception as e:
            logger.error("Chunking failed",
                         filing=filing["path"], error=str(e))
            state.summary["details"].append({
                "cik": filing["cik"],
                "filing_type": filing["filing_type"],
                "accession_number": filing["accession_number"],
                "status": "chunking_failed",
                "error": str(e)
            })

    print(f"✓ Chunked {len(state.chunked_filings)} documents into chunks")
    return state


# ============================================================================
# STEP 8: Build Pipeline (Validation)
# ============================================================================

def step8_build_pipeline(state: PipelineState) -> PipelineState:
    """
    Step 8: Validate pipeline and prepare for final output.

    Args:
        state: Current pipeline state

    Returns:
        Validated pipeline state
    """
    # Validate all components are ready
    if not state.chunked_filings:
        raise ValueError(
            "Pipeline incomplete. Ensure all previous steps completed successfully.")

    print("✓ Pipeline built successfully")
    print(f"  - Total filings processed: {len(state.chunked_filings)}")
    print(
        f"  - Total chunks created: {sum(f['num_chunks'] for f in state.chunked_filings)}")
    print(f"  - Duplicates skipped: {state.summary['skipped_duplicates']}")
    print(f"  - Parsing errors: {state.summary['parsing_errors']}")

    return state


# ============================================================================
# STEP 9: Generate Report and Files
# ============================================================================

def step9_generate_report(state: PipelineState,
                          output_dir: str = "./pipeline_output") -> Dict[str, Any]:
    """
    Step 9: Generate final report and export processed data.

    Args:
        state: Completed pipeline state
        output_dir: Directory to save output files

    Returns:
        Summary report dictionary
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate report
    report = {
        "pipeline_summary": {
            "attempted_downloads": state.summary["attempted_downloads"],
            "unique_filings_processed": state.summary["unique_filings_processed"],
            "skipped_duplicates": state.summary["skipped_duplicates"],
            "parsing_errors": state.summary["parsing_errors"],
            "total_chunks": sum(f.get("num_chunks", 0) for f in state.chunked_filings)
        },
        "filings": []
    }

    # Export chunked data
    for filing in state.chunked_filings:
        filing_data = {
            "cik": filing["cik"],
            "filing_type": filing["filing_type"],
            "accession_number": filing["accession_number"],
            "content_hash": filing["content_hash"],
            "num_chunks": filing["num_chunks"],
            "num_tables": len(filing.get("parsed_tables", [])),
            "parsed_tables": filing.get("parsed_tables"),
            "chunks": filing["chunks"]
        }
        report["filings"].append(filing_data)

        # Save individual filing data
        filing_output = output_path / \
            f"{filing['cik']}_{filing['filing_type']}_{filing['accession_number']}.json"
        with open(filing_output, 'w') as f:
            json.dump(filing_data, f, indent=2)

    # Save complete report
    report_file = output_path / "pipeline_report.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

    # Save summary text
    summary_file = output_path / "summary.txt"
    with open(summary_file, 'w') as f:
        f.write("="*60 + "\n")
        f.write("SEC EDGAR Pipeline Summary Report\n")
        f.write("="*60 + "\n\n")
        f.write(
            f"Attempted downloads: {report['pipeline_summary']['attempted_downloads']}\n")
        f.write(
            f"Unique filings processed: {report['pipeline_summary']['unique_filings_processed']}\n")
        f.write(
            f"Duplicates skipped: {report['pipeline_summary']['skipped_duplicates']}\n")
        f.write(
            f"Parsing errors: {report['pipeline_summary']['parsing_errors']}\n")
        f.write(
            f"Total chunks created: {report['pipeline_summary']['total_chunks']}\n\n")
        f.write("="*60 + "\n")
        f.write("Filing Details\n")
        f.write("="*60 + "\n\n")
        for detail in state.summary["details"]:
            f.write(f"CIK: {detail['cik']}, Type: {detail['filing_type']}, "
                    f"Acc #: {detail['accession_number']}, Status: {detail['status']}\n")

    print(f"\n✓ Report generated in {output_dir}/")
    print(f"  - Summary: {summary_file}")
    print(f"  - Full report: {report_file}")
    print(f"  - Individual filings: {len(state.chunked_filings)} JSON files")

    return report


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

async def main():
    """Example of running the pipeline step by step."""

    # Step 1: Initialize
    state = step1_initialize_pipeline(
        company_name="QuantInsight Analytics",
        email_address="your_email@quantinsight.com",
        download_dir="./sec_filings"
    )

    # Step 2: Add downloader
    state = step2_add_downloader(state)

    # Step 3: Configure rate limiting
    state = step3_configure_rate_limiting(state, request_delay=0.1)

    # Step 4: Download filings
    state = await step4_download_filings(
        state,
        ciks=["0000320193"],  # Apple
        filing_types=["10-K"],
        after_date="2023-01-01",
        limit=1
    )

    # Step 5: Parse documents
    state = await step5_parse_documents(state)

    # Step 6: Deduplicate
    state = step6_deduplicate_documents(state)

    # Step 7: Chunk text
    state = await step7_chunk_text(state, chunk_size=750, chunk_overlap=50)

    # Step 8: Build pipeline
    state = step8_build_pipeline(state)

    # Step 9: Generate report
    report = step9_generate_report(state, output_dir="./pipeline_output")

    return state, report


# To run:
# state, report = await main()


_EMPTY_RE = re.compile(r"^\s*$")


def _clean_cell(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return str(x)
    s = str(x).strip()
    if s.lower() in {"null", "none", "nan"} or _EMPTY_RE.match(s):
        return None
    return s


def _make_unique_headers(headers: List[Optional[str]]) -> List[str]:
    """
    Ensure headers are non-empty and unique.
    - None/empty -> col_i
    - duplicates -> suffix _2, _3 ...
    """
    out: List[str] = []
    seen: Dict[str, int] = {}

    for i, h in enumerate(headers):
        base = (h or "").strip()
        if not base:
            base = f"col_{i}"

        # de-dupe
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")

    return out


def _headers_look_like_data(headers: List[Optional[str]]) -> bool:
    """
    Heuristic: SEC/PDF extracted 'headers' are actually a data row if:
    - many numeric/currency-like tokens OR
    - lots of None/empty mixed with values
    """
    cleaned = [(h or "").strip() for h in headers]
    if not cleaned:
        return True

    empties = sum(1 for h in cleaned if not h)
    nonempties = len(cleaned) - empties

    # numeric-ish tokens
    num_like = 0
    for h in cleaned:
        if not h:
            continue
        if re.fullmatch(r"[\$]?", h) or re.fullmatch(r"[\d,]+(\.\d+)?", h):
            num_like += 1

    # If it's mostly empty, or many numeric-ish values, treat as "not real header"
    if nonempties == 0:
        return True
    if empties / len(cleaned) >= 0.35:
        return True
    if num_like / max(nonempties, 1) >= 0.40:
        return True

    return False


def normalize_parsed_tables(parsed_tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []

    for t in parsed_tables:
        page = t.get("page")
        headers_raw = t.get("headers") or []
        rows_raw = t.get("rows") or []

        headers = [_clean_cell(h) for h in headers_raw]
        rows = [[_clean_cell(c) for c in (r or [])] for r in rows_raw]

        max_row_len = max([len(r) for r in rows], default=0)
        ncols = max(len(headers), max_row_len)

        if ncols == 0:
            normalized.append({"page": page, "headers": [], "rows": []})
            continue

        headers = (headers + [None] * ncols)[:ncols]

        # If headers are garbage / data-like, replace entirely
        if _headers_look_like_data(headers):
            headers = [f"col_{i}" for i in range(ncols)]

        # Pad/truncate rows to ncols
        fixed_rows = []
        for r in rows:
            fixed_rows.append((r + [None] * ncols)[:ncols])

        # Final: ensure unique + non-empty column names (prevents your ValueError)
        headers = _make_unique_headers(headers)

        normalized.append(
            {"page": page, "headers": headers, "rows": fixed_rows})

    return normalized
