#!/usr/bin/env python3
"""Test script to verify all imports and basic functionality"""

try:
    from source import SECDownloader, DocumentParser, DocumentChunker, SECPipeline
    print("✓ All imports successful!")

    # Test SECPipeline initialization
    pipeline = SECPipeline(
        download_dir="./test_sec_filings",
        registry_file="test_registry.txt",
        chunk_size=500,
        chunk_overlap=50,
        company_name="Test Company",
        email_address="test@test.com"
    )
    print("✓ SECPipeline initialized successfully!")
    print(f"  - Download dir: {pipeline.download_dir}")
    print(f"  - Registry file: {pipeline.registry.registry_file}")
    print(f"  - Chunk size: {pipeline.chunker.chunk_size}")
    print(f"  - Chunk overlap: {pipeline.chunker.chunk_overlap}")

    print("\n✓ All tests passed!")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
