import time

from zotero_arxiv_daily.protocol import Paper
from zotero_arxiv_daily.retriever.base import BaseRetriever


class SlowRetriever(BaseRetriever):
    name = "arxiv"

    def _retrieve_raw_papers(self) -> list[float]:
        return [0.1, 0.1, 4.0]

    def convert_to_paper(self, raw_paper: float) -> Paper | None:
        time.sleep(raw_paper)
        return Paper(
            source=self.name,
            title=f"paper-{raw_paper}",
            authors=["tester"],
            abstract="test",
            url=f"https://example.com/{raw_paper}",
        )


class FlakyRetriever(BaseRetriever):
    name = "arxiv"

    def _retrieve_raw_papers(self) -> list[int]:
        return [1, 2, 3]

    def convert_to_paper(self, raw_paper: int) -> Paper | None:
        if raw_paper == 2:
            raise RuntimeError("boom")
        return Paper(
            source=self.name,
            title=f"paper-{raw_paper}",
            authors=["tester"],
            abstract="test",
            url=f"https://example.com/{raw_paper}",
        )


def test_retrieve_papers_skips_timed_out_items(config):
    config.executor.max_workers = 2
    config.executor.paper_timeout_seconds = 1.5

    retriever = SlowRetriever(config)

    start = time.monotonic()
    papers = retriever.retrieve_papers()
    elapsed = time.monotonic() - start

    assert len(papers) == 2
    assert elapsed < 4


def test_retrieve_papers_skips_failed_items(config):
    config.executor.max_workers = 2
    config.executor.paper_timeout_seconds = 5

    retriever = FlakyRetriever(config)
    papers = retriever.retrieve_papers()

    assert len(papers) == 2
    assert {paper.title for paper in papers} == {"paper-1", "paper-3"}
