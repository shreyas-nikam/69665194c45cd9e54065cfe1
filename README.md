# QuLab: SEC EDGAR Ingestion Pipeline - A Streamlit Application for Financial Data

![QuantInsight Analytics Logo](https://www.quantuniversity.com/assets/img/logo5.jpg)

## Project Title and Description

**QuLab: SEC EDGAR Ingestion Pipeline** is a Streamlit-powered laboratory project developed for "QuantInsight Analytics," designed to demonstrate a robust and scalable pipeline for ingesting public financial disclosures from the SEC EDGAR database.

As a Software Developer at QuantInsight Analytics, this project addresses the critical need to establish a reliable data infrastructure for downstream financial analysis, regulatory compliance, market research, and AI-driven insights. It showcases the implementation of key components necessary for handling the volume and variety of SEC filings (e.g., 10-K, 10-Q, 8-K), adhering to SEC's API guidelines (especially rate limits), and ensuring data quality through advanced techniques like deduplication and intelligent document segmentation (chunking) for AI readiness.

This application provides an interactive interface to configure, execute, and inspect each stage of the data pipeline, laying the groundwork for sophisticated financial data applications.

## Key Objectives of This Lab Project

*   **Remember:** Identify common SEC filing types (10-K, 10-Q, 8-K, DEF-14A).
*   **Understand:** Explain the rationale behind document chunking and its impact on extraction quality for AI/LLM applications.
*   **Apply:** Implement a complete SEC filing download, parsing, deduplication, and chunking pipeline.
*   **Analyze:** Compare and contrast strategies for extracting text and tables from diverse document formats (HTML vs. PDF).

## Features

This Streamlit application implements the following core features of an SEC EDGAR data pipeline:

*   **Rate-Limited SEC Filing Downloader**: Compliant downloading of SEC filings from the EDGAR database, adhering to SEC's recommended rate limits to prevent IP blocking.
    *   **Mathematical Concept**: `T_delay = 1 / R_max` where `R_max` is the maximum allowed requests per second. (Implemented with a 0.1-second delay for 10 requests/second).
*   **Universal Document Parser (HTML & PDF)**: Robust parsing capabilities to extract clean text and structured tables from both HTML-based `.txt` files (common for SEC filings) and PDF documents. Utilizes `BeautifulSoup` for HTML and `pdfplumber` for PDFs.
*   **Content-Based Deduplication (Document Registry)**: A mechanism to prevent reprocessing of identical documents using SHA-256 cryptographic hashing. A registry file stores hashes of already processed content.
    *   **Mathematical Concept**: `h = H(M)` where `H` is a cryptographic hash function (SHA-256) mapping document content `M` to a fixed-size hash `h`.
*   **Intelligent Document Chunker**: Splits long documents into smaller, context-rich chunks of configurable size and overlap, optimized for consumption by Large Language Models (LLMs) and other AI applications.
    *   **Mathematical Concept**: `N_chunks = ceil((TotalTokens - OverlapSize) / (ChunkSize - OverlapSize))` (approximated using word count for this lab).
*   **Interactive Streamlit UI**:
    *   **Configuration**: User-friendly interface to set pipeline parameters like company name, email, download directory, chunk size, and chunk overlap.
    *   **Execution Control**: Input CIKs, select filing types, specify date ranges, and limit downloads, then trigger the pipeline execution.
    *   **Reporting**: Displays a summary report of attempted downloads, unique filings processed, skipped duplicates, and parsing errors.
    *   **Detailed View**: Allows users to select a processed filing and inspect its raw extracted text and the generated chunks.
*   **Persistent Configuration**: Uses Streamlit's `session_state` to maintain user inputs and pipeline status across reruns and navigation.

## Getting Started

Follow these instructions to get a copy of the project up and running on your local machine.

### Prerequisites

*   Python 3.8+
*   `pip` (Python package installer)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/sec-edgar-pipeline-lab.git
    cd sec-edgar-pipeline-lab
    ```
    *(Note: Replace `your-username/sec-edgar-pipeline-lab` with the actual repository URL if this project is hosted.)*

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    Create a `requirements.txt` file in the project root with the following content:
    ```
    streamlit
    sec-edgar-downloader
    pandas
    beautifulsoup4
    pdfplumber
    PyMuPDF # for 'fitz'
    httpx # Included as per original code import
    ```
    Then, install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  **Run the Streamlit application:**
    From the project root directory, execute:
    ```bash
    streamlit run app.py
    ```
    This will open the application in your web browser, typically at `http://localhost:8501`.

2.  **Navigate the Application:**
    Use the sidebar navigation to move between pages:
    *   **Home**: Introduction and project overview.
    *   **Configuration & Setup**: Set global parameters for the pipeline, including your company name and email (required for SEC User-Agent), download directory, registry file name, and chunking strategy. Click "Initialize Pipeline Components" to apply changes.
    *   **Download & Process Filings**: Input CIKs (Company Identification Numbers), select desired filing types (e.g., 10-K, 10-Q), specify a date range, and set a download limit per CIK/filing type. Click "Run SEC Data Pipeline" to start the ingestion process.
    *   **Processed Filings & Analysis**: View the pipeline's summary report and a detailed log of processed filings. You can select any successfully processed filing to inspect its full extracted text and how it was split into chunks.

**Important Note for SEC Downloads:**
The SEC requires a User-Agent header with your company name and email address for all requests. Please ensure you provide accurate information in the "Configuration & Setup" section. Disregarding rate limits or failing to provide a proper User-Agent can lead to your IP being temporarily or permanently blocked by the SEC.

## Project Structure

For this lab project, the core pipeline logic and the Streamlit application code are contained within a single `app.py` file to simplify demonstration.

```
.
├── app.py                      # Main Streamlit application and pipeline logic
├── requirements.txt            # Python dependencies
├── README.md                   # This file
└── sec_filings/                # Default directory for downloaded SEC filings and document_registry.txt
    └── document_registry.txt   # File to store SHA-256 hashes for deduplication
    └── <CIK>/                  # Subdirectories for each CIK
        └── <FILING_TYPE>/      # Subdirectories for each filing type
            └── <ACCESSION_NUMBER>.txt # Downloaded filing content
```

In a production environment, the `SECDownloader`, `DocumentParser`, `DocumentChunker`, and `SECPipeline` classes would typically reside in separate Python modules (e.g., `src/pipeline/`) to promote modularity and maintainability.

## Technology Stack

*   **Python 3.8+**: The primary programming language.
*   **Streamlit**: For creating the interactive web application user interface.
*   **sec-edgar-downloader**: Python library for downloading SEC filings.
*   **BeautifulSoup4 (`bs4`)**: For parsing HTML content extracted from `.txt` filings.
*   **pdfplumber**: For robust text and table extraction from PDF documents.
*   **PyMuPDF (`fitz`)**: A high-performance PDF manipulation library, used indirectly by `pdfplumber` or available for other PDF tasks.
*   **pandas**: For data manipulation, especially for handling and displaying tabular data.
*   **asyncio**: Python's built-in library for writing concurrent code, used for handling asynchronous operations and integrating with Streamlit's event loop.
*   **httpx**: An asynchronous HTTP client (imported but not explicitly used in the provided pipeline logic, signaling potential for future async HTTP needs).
*   **hashlib**: Python's module for cryptographic hashing, used for SHA-256 content deduplication.

## Contributing

This project is primarily a laboratory exercise designed for educational purposes at "QuantInsight Analytics." As such, external contributions are not typically expected. However, you are welcome to fork the repository, experiment with the code, and adapt it for your own learning or projects.

For internal team members at QuantInsight Analytics, please refer to our internal guidelines for contributions and code reviews.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details (if applicable, otherwise state it's under an implied MIT license for open source labs).

```
MIT License

Copyright (c) 2023 QuantInsight Analytics (QuLab)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Contact

For questions, feedback, or further information regarding this project within "QuantInsight Analytics," please contact:

*   **Email**: `development_team@quantinsight.com`
*   **Project Lead**: `[Your Name/Team Lead Name]`
*   **Company Website**: `https://www.quantuniversity.com` (as per logo)
