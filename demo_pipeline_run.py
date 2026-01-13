import asyncio
from source import SECPipeline


async def main():
    # 1. Initialize the pipeline
    pipeline = SECPipeline(
        download_dir="./sec_filings",
        registry_file="document_registry.txt",
        chunk_size=750,
        chunk_overlap=50,
        company_name="QuantInsight Analytics",
        email_address="demo@quantinsight.com"
    )

    # 2. Download the files (Apple and Microsoft, 10-K and 10-Q, after 2022-01-01, limit 2)
    ciks = ["0000320193", "0000789019"]
    filing_types = ["10-K", "10-Q"]
    after_date = "2022-01-01"
    limit = 2
    report = await pipeline.run_pipeline(
        ciks=ciks,
        filing_types=filing_types,
        after_date=after_date,
        limit=limit
    )

    # 3. Print summary and details
    print("\nSEC EDGAR Pipeline Summary Report")
    print("="*50)
    print(f"Total attempted downloads: {report['attempted_downloads']}")
    print(f"Unique filings processed: {report['unique_filings_processed']}")
    print(f"Skipped duplicates: {report['skipped_duplicates']}")
    print(f"Processing errors: {report['parsing_errors']}")
    print("\n--- Details of Processed Filings ---")
    for detail in report['details']:
        print(f"CIK: {detail['cik']}, Type: {detail['filing_type']}, Acc #: {detail['accession_number']}, Status: {detail['status']}, Message: {detail['message']}")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(main())
