import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed

from workflow.host_summary.host_pipeline import (
    build_default_pipeline,
    DEFAULT_PAGE_SIZE,
    DEFAULT_MAX_COUNT,
)


def test_process_all_sequential():
    # Ensure process_all works with concurrency=1 sequentially
    pipeline = build_default_pipeline(
        page_size=2,
        max_count=5,
        concurrency=1,
    )

    try:
        processed = pipeline.process_all()
    except Exception as e:
        pytest.skip(f"LLM/Hive not available or misconfigured: {e}")
    assert isinstance(processed, int)
    assert processed == 5



def test_process_all_parallel():
    # Ensure process_all works with concurrency=1 sequentially
    pipeline = build_default_pipeline(
        page_size=2,
        max_count=5,
        concurrency=2,
    )

    try:
        processed = pipeline.process_all()
    except Exception as e:
        pytest.skip(f"LLM/Hive not available or misconfigured: {e}")
    assert isinstance(processed, int)
    assert processed == 5
