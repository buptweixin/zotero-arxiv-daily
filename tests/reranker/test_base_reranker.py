from datetime import datetime

import numpy as np

from zotero_arxiv_daily.protocol import CorpusPaper, Paper
from zotero_arxiv_daily.reranker.base import BaseReranker


class DummyReranker(BaseReranker):
    def get_similarity_score(self, s1: list[str], s2: list[str]) -> np.ndarray:
        return np.array(
            [
                [0.9, 0.1, 0.7],
                [0.2, 0.8, 0.3],
            ]
        )


def test_rerank_attaches_top_3_related_papers(config):
    candidates = [
        Paper(
            source="arxiv",
            title="Candidate A",
            authors=["Author A"],
            abstract="Candidate abstract A",
            url="https://example.com/a",
        ),
        Paper(
            source="arxiv",
            title="Candidate B",
            authors=["Author B"],
            abstract="Candidate abstract B",
            url="https://example.com/b",
        ),
    ]
    corpus = [
        CorpusPaper(
            title="Corpus 1",
            abstract="Corpus abstract 1",
            added_date=datetime(2026, 3, 3),
            paths=[],
        ),
        CorpusPaper(
            title="Corpus 2",
            abstract="Corpus abstract 2",
            added_date=datetime(2026, 3, 2),
            paths=[],
        ),
        CorpusPaper(
            title="Corpus 3",
            abstract="Corpus abstract 3",
            added_date=datetime(2026, 3, 1),
            paths=[],
        ),
    ]

    reranked = DummyReranker(config).rerank(candidates, corpus)

    assert reranked[0].title == "Candidate A"
    assert reranked[0].related_papers == ["Corpus 1", "Corpus 3", "Corpus 2"]
    assert reranked[1].related_papers == ["Corpus 2", "Corpus 3", "Corpus 1"]
