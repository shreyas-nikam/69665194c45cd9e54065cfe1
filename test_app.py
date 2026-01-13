
import pytest
from streamlit.testing.v1 import AppTest
from unittest.mock import MagicMock, patch
import datetime
import os

# Note: These tests assume the Streamlit application code provided in the prompt
# is saved in a file named `app.py` in the same directory where these tests are executed.
# For the tests to run correctly without needing the actual `source.py` file,
# the `app.py` must include mock implementations of `SECDownloader`, `DocumentParser`,
# `DocumentChunker`, and `SECPipeline` classes, as illustrated in the thought process.


@pytest.fixture(autouse=True)
def mock_os_makedirs():
    """Fixture to mock os.makedirs during tests to prevent actual directory creation."""
    with patch("os.makedirs") as mock_makedirs:
        yield mock_makedirs

@pytest.fixture
def app_test_instance():
    """Provides a fresh AppTest instance for each test."""
    return AppTest.from_file("app.py")

def test_initial_page_is_home(app_test_instance):
    """Verify that the initial page displayed is 'Home'."""
    at = app_test_instance.run()
    assert at.session_state["current_page"] == "Home"
    assert "SEC EDGAR Data Pipeline: A Developer's Workflow" in at.markdown[0].value

def test_home_to_config_navigation(app_test_instance):
    """Test navigation from Home page to Configuration & Setup page."""
    at = app_test_instance.run()
    # Click the "Go to Configuration" button on the Home page
    at.button[0].click().run()
    assert at.session_state["current_page"] == "Configuration & Setup"
    assert "Setting Up the SEC EDGAR Data Pipeline" in at.markdown[0].value

def test_config_page_defaults_and_updates(app_test_instance):
    """Test default values and updating inputs on the Configuration & Setup page."""
    at = app_test_instance.set_session_state(current_page='Configuration & Setup').run()
    
    # Verify default values of text_inputs by label
    assert at.text_input(label="Your Company Name (for SEC User-Agent)").value == "QuantInsight Analytics"
    assert at.text_input(label="Your Email Address (for SEC User-Agent)").value == "your_email@quantinsight.com"
    assert at.text_input(label="Local Download Directory").value == "./sec_filings"
    assert at.text_input(label="Document Registry File").value == "document_registry.txt"
    
    # Verify default values of number_inputs by label
    assert at.number_input(label="Chunk Size (words)").value == 750
    assert at.number_input(label="Chunk Overlap (words)").value == 50

    # Update input values
    at.text_input(label="Your Company Name (for SEC User-Agent)").set_value("Test Company").run()
    at.text_input(label="Your Email Address (for SEC User-Agent)").set_value("test@example.com").run()
    at.number_input(label="Chunk Size (words)").set_value(1000).run()
    at.number_input(label="Chunk Overlap (words)").set_value(100).run()

    # Verify session state reflects updated values
    assert at.session_state["company_name"] == "Test Company"
    assert at.session_state["email_address"] == "test@example.com"
    assert at.session_state["chunk_size"] == 1000
    assert at.session_state["chunk_overlap"] == 100

def test_initialize_pipeline_success_and_navigation(app_test_instance, mock_os_makedirs):
    """Test successful pipeline initialization and navigation to Download & Process Filings."""
    with patch("app.SECPipeline") as mock_sec_pipeline_class:
        mock_pipeline_instance = MagicMock()
        mock_sec_pipeline_class.return_value = mock_pipeline_instance

        at = app_test_instance.set_session_state(current_page='Configuration & Setup').run()
        
        # Click the "Initialize Pipeline Components" button (the only button on this page)
        at.button[0].click().run()
        
        # Verify success message and session state updates
        assert at.success[0].value == "Pipeline components initialized successfully!"
        assert at.session_state["pipeline_initialized"] is True
        assert at.session_state["sec_pipeline_instance"] is mock_pipeline_instance
        assert at.session_state["current_page"] == "Download & Process Filings"
        
        # Verify that SECPipeline was instantiated with the correct parameters
        mock_sec_pipeline_class.assert_called_once_with(
            download_dir="./sec_filings",
            registry_file="document_registry.txt",
            chunk_size=750,
            chunk_overlap=50,
            company_name="QuantInsight Analytics",
            email_address="your_email@quantinsight.com"
        )
        # Verify os.makedirs was called (mocked by the fixture)
        mock_os_makedirs.assert_called_once_with("./sec_filings", exist_ok=True)

def test_download_process_filings_not_initialized_navigation(app_test_instance):
    """Test Download & Process Filings page warning and navigation when pipeline is not initialized."""
    at = app_test_instance.set_session_state(current_page='Download & Process Filings', pipeline_initialized=False).run()
    
    # Verify warning message is displayed
    assert at.warning[0].value == "Pipeline not initialized. Please go to 'Configuration & Setup' first."
    # Click the "Go to Configuration & Setup" button (the only button on this page)
    at.button[0].click().run()
    assert at.session_state["current_page"] == "Configuration & Setup"

def test_download_process_filings_inputs_and_run_pipeline_no_ciks(app_test_instance):
    """Test inputs and error message for no CIKs on Download & Process Filings page."""
    # Mock SECPipeline instance to be present in session state, as the page expects it
    mock_pipeline_instance = MagicMock()
    
    at = app_test_instance.set_session_state(
        current_page='Download & Process Filings',
        pipeline_initialized=True,
        sec_pipeline_instance=mock_pipeline_instance,
        ciks_input="" # Set CIKs input to empty string
    ).run()

    # Verify input fields and their default/set values
    assert at.text_area(label="CIKs (e.g., 0000320193 for Apple Inc.)").value == ""
    assert at.multiselect(label="Select Filing Types").value == ["10-K", "10-Q"]
    assert at.date_input(label="Download filings after date (YYYY-MM-DD)").value == datetime.date(2022, 1, 1)
    assert at.number_input(label="Max number of filings per type per CIK").value == 2

    # Attempt to run pipeline with no CIKs (the "Run SEC Data Pipeline" button is the first button if the warning isn't shown)
    at.button[0].click().run() 
    assert at.error[0].value == "Please enter at least one CIK."
    # Ensure the pipeline's run_pipeline method was NOT called because of the validation error
    mock_pipeline_instance.run_pipeline.assert_not_called()

def test_run_pipeline_success(app_test_instance):
    """Test successful pipeline execution and navigation to Processed Filings & Analysis."""
    mock_pipeline_instance = MagicMock()
    mock_report = {
        "attempted_downloads": 4,
        "unique_filings_processed": 2,
        "skipped_duplicates": 1,
        "parsing_errors": 0,
        "details": [
            {"cik": "0000320193", "filing_type": "10-K", "accession_number": "00012345-23-000001", "status": "processed", "path": "path/to/apple_10k.html"},
            {"cik": "0000320193", "filing_type": "10-Q", "accession_number": "00012345-23-000002", "status": "skipped_duplicate", "path": "path/to/apple_10q.html"},
            {"cik": "0000789019", "filing_type": "10-K", "accession_number": "00012345-23-000003", "status": "processed", "path": "path/to/msft_10k.html"},
        ]
    }
    # Configure the mock run_pipeline method to return our predefined report
    mock_pipeline_instance.run_pipeline.return_value = mock_report

    # Patch asyncio.run so it directly returns our mock_report, bypassing actual async execution
    with patch("asyncio.run", return_value=mock_report):
        at = app_test_instance.set_session_state(
            current_page='Download & Process Filings',
            pipeline_initialized=True,
            sec_pipeline_instance=mock_pipeline_instance,
            ciks_input="0000320193\n0000789019", # Provide CIKs
            filing_types_selected=["10-K", "10-Q"],
            after_date_input=datetime.date(2022, 1, 1),
            limit_input=1
        ).run()

        # Click the "Run SEC Data Pipeline" button
        at.button[0].click().run()

        # Verify processing_in_progress state (should be False after completion)
        assert at.session_state["processing_in_progress"] is False
        assert at.success[0].value == "Pipeline execution completed!"
        assert at.session_state["pipeline_report"] == mock_report
        assert at.session_state["current_page"] == "Processed Filings & Analysis"
        
        # Verify run_pipeline was called with the expected arguments
        mock_pipeline_instance.run_pipeline.assert_called_once_with(
            ciks=["0000320193", "0000789019"],
            filing_types=["10-K", "10-Q"],
            after_date="2022-01-01",
            limit=1
        )

def test_processed_filings_no_report_navigation(app_test_instance):
    """Test Processed Filings & Analysis page warning and navigation when no report is available."""
    at = app_test_instance.set_session_state(current_page='Processed Filings & Analysis', pipeline_report=None).run()
    
    # Verify the info message is displayed
    assert at.info[0].value == "No pipeline report available. Please run the pipeline from the 'Download & Process Filings' page."
    # Click "Go to Download & Process Filings" button
    at.button[0].click().run()
    assert at.session_state["current_page"] == "Download & Process Filings"

def test_processed_filings_with_report_and_viewing(app_test_instance):
    """Test Processed Filings & Analysis page with a report, including viewing a filing."""
    mock_pipeline_instance = MagicMock()
    mock_report = {
        "attempted_downloads": 4,
        "unique_filings_processed": 2,
        "skipped_duplicates": 1,
        "parsing_errors": 0,
        "details": [
            {"cik": "0000320193", "filing_type": "10-K", "accession_number": "00012345-23-000001", "status": "processed", "path": "path/to/apple_10k.html"},
            {"cik": "0000320193", "filing_type": "10-Q", "accession_number": "00012345-23-000002", "status": "skipped_duplicate", "path": "path/to/apple_10q.html"},
            {"cik": "0000789019", "filing_type": "10-K", "accession_number": "00012345-23-000003", "status": "processed", "path": "path/to/msft_10k.html"},
        ]
    }
    
    # Configure mocks for parser and chunker methods within the mock pipeline instance
    mock_pipeline_instance.parser.parse_filing.return_value = {"text": "This is the mocked full text content for the selected filing.", "tables": []}
    mock_pipeline_instance.chunker.chunk_document.return_value = [
        "This is the mocked", "full text content", "for the selected filing."
    ]

    at = app_test_instance.set_session_state(
        current_page='Processed Filings & Analysis',
        pipeline_report=mock_report,
        sec_pipeline_instance=mock_pipeline_instance # Essential for the view functionality
    ).run()

    # Verify summary metrics displayed on the page
    assert at.metric[0].value == "4" # Attempted Downloads
    assert at.metric[1].value == "2" # Unique Filings Processed
    assert at.metric[2].value == "1" # Skipped Duplicates
    assert at.metric[3].value == "0" # Processing Errors

    # Verify the detailed processing log dataframe is displayed and its content matches the mock
    assert at.dataframe[0] is not None
    assert at.dataframe[0].to_dict('records') == mock_report['details']

    # Select a filing from the dropdown to view its content.
    # The `options` list includes an initial empty string, so the first actual filing is at index 1.
    at.selectbox(label="Select a successfully processed filing to view its content:").set_value(
        "0000320193 - 10-K - 00012345-23-000001"
    ).run()

    # Click the "Extract and Chunk Selected Filing" button (the first button after the selectbox)
    at.button[0].click().run()

    # Verify success message after extraction and chunking
    assert at.success[0].value == "Text extracted and chunked!"
    
    # Verify the text area for full extracted text
    assert at.text_area(label="Full Text Content").value == "This is the mocked full text content for the selected filing."

    # Verify that the generated chunks section is present and its content is in session state
    assert "Generated Chunks (3 chunks)" in at.markdown[-1].value 
    assert at.session_state["view_text"] == "This is the mocked full text content for the selected filing."
    assert at.session_state["view_chunks"] == ["This is the mocked", "full text content", "for the selected filing."]

    # Test the scenario where the report contains no successfully processed filings
    mock_report_no_processed = {
        "attempted_downloads": 1,
        "unique_filings_processed": 0,
        "skipped_duplicates": 1,
        "parsing_errors": 0,
        "details": [
            {"cik": "0000320193", "filing_type": "10-K", "accession_number": "00012345-23-000001", "status": "skipped_duplicate", "path": "path/to/apple_10k.html"},
        ]
    }
    at_no_processed = app_test_instance.set_session_state(
        current_page='Processed Filings & Analysis',
        pipeline_report=mock_report_no_processed,
        sec_pipeline_instance=mock_pipeline_instance # Still need the instance for page logic
    ).run()
    # The last info message on this page should indicate no processed filings to view
    assert at_no_processed.info[-1].value == "No filings were successfully processed to view details."
