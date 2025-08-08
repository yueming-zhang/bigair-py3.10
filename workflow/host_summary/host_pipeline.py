from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Dict, Optional, Tuple

from .conn_factory import ConnectionFactory
from .hive_qa import get_host_summary_qa, HostSummaryQA


# Tunables
DEFAULT_PAGE_SIZE = 10
DEFAULT_CONCURRENCY = 4
DEFAULT_MAX_COUNT = 100


class HostPipeline:
    """
    Single-instance host pipeline that:
    - Pages host_ids from Hive using ConnectionFactory (keyset pagination; no OFFSET)
    - Processes each page-sized chunk concurrently via process_active_listings()
    - Uses a class-level shared HostSummaryQA instance (assumed thread-safe)
    """

    # Class-level shared QA instance (assumed thread-safe)
    qa: Optional[HostSummaryQA] = None

    def __init__(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_count: Optional[int] = DEFAULT_MAX_COUNT,
        concurrency: int = DEFAULT_CONCURRENCY,
    ) -> None:
        if HostPipeline.qa is None:
            HostPipeline.qa = get_host_summary_qa()

        self.page_size = page_size
        self.max_count = max_count
        self.concurrency = max(1, concurrency)

    @staticmethod
    def get_ds_for_query() -> str:
        """Return the date string (YYYY-MM-DD) for the day before yesterday in UTC."""
        return (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")

    def fetch_host_ids_page_keyset(
            self,
            page_size: int,
            ds: str,
            cursor: Optional[Tuple[int, int]] = None,  # (last_m_active_listings, last_host_id)
    ) -> Tuple[List[int], Optional[Tuple[int, int]]]:
            """
            Keyset pagination: fetch a page after the given cursor using
            ORDER BY m_active_listings DESC, host_id ASC.
            Returns (host_ids, new_cursor).
            """
            where_cursor = ""
            if cursor is not None:
                    last_m, last_h = cursor
                    where_cursor = f"""
                        AND (
                            m_active_listings < {last_m}
                            OR (m_active_listings = {last_m} AND id_host > {last_h})
                        )
                    """

            sql = f"""
            SELECT id_host, m_active_listings
            FROM homes.host__dim_active
            WHERE ds = '{ds}'
            {where_cursor}
            ORDER BY m_active_listings DESC, id_host ASC
            LIMIT {page_size}
            """.strip()

            rows = ConnectionFactory.execute_hive_query(sql) or []
            ids: List[int] = [int(r["id_host"]) for r in rows]

            new_cursor: Optional[Tuple[int, int]] = None
            if rows:
                last = rows[-1]
                new_cursor = (int(last["m_active_listings"]), int(last["id_host"]))
            return ids, new_cursor

    def iter_host_ids_chunks(
        self, page_size: Optional[int] = None, max_count: Optional[int] = None
    ) -> Iterator[List[int]]:
        """Yield lists of host_ids in pages until max_count is reached or data ends (keyset pagination)."""
        page_size = page_size or self.page_size
        max_count = max_count if max_count is not None else self.max_count
        ds = self.get_ds_for_query()

        cursor: Optional[Tuple[int, int]] = None
        total_emitted = 0
        while True:
            ids, cursor = self.fetch_host_ids_page_keyset(
                page_size=page_size, ds=ds, cursor=cursor
            )
            if not ids:
                break

            if max_count is not None:
                remaining = max_count - total_emitted
                if remaining <= 0:
                    break
                if len(ids) > remaining:
                    ids = ids[:remaining]

            yield ids
            total_emitted += len(ids)

            # If the last page was shorter than page_size, we're done
            if len(ids) < page_size:
                break

            if max_count is not None and total_emitted >= max_count:
                break

    def process_active_listings(self, host_ids: List[int]) -> List[Dict]:
        """
        For each host_id in the chunk, ask the QA system:
        "How many listings does the host have, and what % is active?"
        Returns a list of dicts per host.
        """
        results: List[Dict] = []
        for host_id in host_ids:
            q = f"How many listings does the host {host_id} have, and what % is active?"
            answer = HostPipeline.qa.ask(  # type: ignore[union-attr]
                q, print_results=False, auto_train=False, visualize=False
            )
            results.append({"host_id": host_id, "answer_text": str(answer)})
        return results

    def process_all(self) -> int:
        """Orchestrate paging and concurrent processing of chunks, returning total processed hosts."""
        total_processed = 0
        futures = []
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            for chunk in self.iter_host_ids_chunks():
                # Backpressure: keep at most `concurrency` in-flight tasks
                while len(futures) >= self.concurrency:
                    done, futures = self._wait_one(futures)
                    total_processed += self._handle_done_future(done)

                futures.append(executor.submit(self.process_active_listings, chunk))

            # Drain remaining tasks
            for f in as_completed(futures):
                total_processed += self._handle_done_future(f)

        return total_processed

    def _wait_one(self, futures):
        for f in as_completed(futures, timeout=None):
            remaining = [x for x in futures if x is not f]
            return f, remaining
        # Should not reach here
        return None, futures

    def _handle_done_future(self, future) -> int:
        if future is None:
            return 0
        try:
            results = future.result()
            return len(results) if isinstance(results, list) else 0
        except Exception as e:
            # Swallow or re-raise depending on desired behavior; here we re-raise
            raise e


def build_default_pipeline(
    page_size: int = DEFAULT_PAGE_SIZE,
    max_count: Optional[int] = DEFAULT_MAX_COUNT,
    concurrency: int = DEFAULT_CONCURRENCY,
):
    """Factory to build a pipeline with default shared QA."""
    return HostPipeline(
        page_size=page_size,
        max_count=max_count,
        concurrency=concurrency,
    )


__all__ = [
    "HostPipeline",
    "build_default_pipeline",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_CONCURRENCY",
    "DEFAULT_MAX_COUNT",
]
