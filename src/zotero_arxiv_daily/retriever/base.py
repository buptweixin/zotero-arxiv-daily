from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from multiprocessing import get_context
from queue import Empty
from time import monotonic
from typing import Any, Type

from loguru import logger
from omegaconf import DictConfig

from ..protocol import Paper, RawPaperItem


@dataclass
class _RunningConversion:
    index: int
    label: str
    process: Any
    started_at: float


def _describe_raw_paper(raw_paper: RawPaperItem) -> str:
    if hasattr(raw_paper, "entry_id"):
        return str(getattr(raw_paper, "entry_id"))
    if hasattr(raw_paper, "title"):
        return str(getattr(raw_paper, "title"))
    if isinstance(raw_paper, dict):
        for key in ("doi", "id", "title"):
            value = raw_paper.get(key)
            if value:
                return str(value)
    return str(raw_paper)


def _convert_worker(
    retriever: "BaseRetriever",
    raw_paper: RawPaperItem,
    index: int,
    result_queue,
) -> None:
    try:
        result_queue.put((index, "ok", retriever.convert_to_paper(raw_paper)))
    except Exception as exc:
        result_queue.put((index, "error", f"{type(exc).__name__}: {exc}"))


def _log_progress(name: str, completed_count: int, raw_paper_count: int, progress_interval: int) -> None:
    if completed_count == 1 or completed_count % progress_interval == 0 or completed_count == raw_paper_count:
        logger.info(f"{name}: converted {completed_count}/{raw_paper_count} papers")


class BaseRetriever(ABC):
    name: str
    def __init__(self, config:DictConfig):
        self.config = config
        self.retriever_config = getattr(config.source,self.name)

    @abstractmethod
    def _retrieve_raw_papers(self) -> list[RawPaperItem]:
        pass

    @abstractmethod
    def convert_to_paper(self, raw_paper:RawPaperItem) -> Paper | None:
        pass

    def retrieve_papers(self) -> list[Paper]:
        logger.info(f"{self.name}: starting raw paper retrieval")
        raw_papers = self._retrieve_raw_papers()
        raw_paper_count = len(raw_papers)
        if raw_paper_count == 0:
            logger.info(f"{self.name}: no raw papers retrieved")
            return []

        logger.info(
            f"{self.name}: retrieved {raw_paper_count} raw papers, converting with "
            f"{self.config.executor.max_workers} workers"
        )
        papers: list[Paper | None] = [None] * raw_paper_count
        progress_interval = max(1, raw_paper_count // 10)
        max_workers = int(self.config.executor.max_workers)
        paper_timeout_seconds = self.config.executor.get("paper_timeout_seconds", 300)
        paper_timeout_seconds = None if paper_timeout_seconds in (None, 0) else float(paper_timeout_seconds)
        logger.info(
            f"{self.name}: per-paper timeout is "
            f"{paper_timeout_seconds if paper_timeout_seconds is not None else 'disabled'} seconds"
        )

        ctx = get_context("spawn")
        result_queue = ctx.Queue()
        pending = deque(enumerate(raw_papers))
        running: dict[int, _RunningConversion] = {}
        finished_indexes: set[int] = set()
        completed_count = 0

        try:
            while pending or running:
                while pending and len(running) < max_workers:
                    index, raw_paper = pending.popleft()
                    label = _describe_raw_paper(raw_paper)
                    process = ctx.Process(
                        target=_convert_worker,
                        args=(self, raw_paper, index, result_queue),
                        name=f"{self.name}-paper-{index}",
                    )
                    process.start()
                    running[index] = _RunningConversion(
                        index=index,
                        label=label,
                        process=process,
                        started_at=monotonic(),
                    )

                try:
                    index, status, payload = result_queue.get(timeout=1)
                except Empty:
                    index = None
                else:
                    if index in finished_indexes:
                        continue
                    run_state = running.pop(index, None)
                    if run_state is not None:
                        run_state.process.join(timeout=0.1)
                        if not run_state.process.is_alive():
                            run_state.process.close()
                    finished_indexes.add(index)
                    completed_count += 1
                    if status == "ok":
                        papers[index] = payload
                    else:
                        logger.warning(f"{self.name}: failed converting {run_state.label if run_state else index}: {payload}")
                    _log_progress(self.name, completed_count, raw_paper_count, progress_interval)

                now = monotonic()
                for index, run_state in list(running.items()):
                    process = run_state.process
                    if process.is_alive() and paper_timeout_seconds is not None and now - run_state.started_at > paper_timeout_seconds:
                        logger.warning(
                            f"{self.name}: timed out after {paper_timeout_seconds:g}s while converting "
                            f"{run_state.label}; skipping this paper"
                        )
                        process.terminate()
                        process.join(timeout=5)
                        if process.is_alive() and hasattr(process, "kill"):
                            process.kill()
                            process.join(timeout=5)
                        process.close()
                        running.pop(index, None)
                        finished_indexes.add(index)
                        completed_count += 1
                        _log_progress(self.name, completed_count, raw_paper_count, progress_interval)
                        continue

                    if process.is_alive():
                        continue

                    exit_code = process.exitcode
                    if exit_code in (0, None):
                        continue

                    process.join(timeout=0.1)
                    process.close()
                    running.pop(index, None)
                    finished_indexes.add(index)
                    completed_count += 1
                    logger.warning(
                        f"{self.name}: worker exited unexpectedly for {run_state.label} "
                        f"with exit code {exit_code}; skipping this paper"
                    )
                    _log_progress(self.name, completed_count, raw_paper_count, progress_interval)
        finally:
            for run_state in running.values():
                process = run_state.process
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                    if process.is_alive() and hasattr(process, "kill"):
                        process.kill()
                        process.join(timeout=5)
                process.close()
            result_queue.close()
            result_queue.join_thread()

        valid_papers = [p for p in papers if p is not None]
        logger.info(
            f"{self.name}: conversion finished, kept {len(valid_papers)}/{raw_paper_count} papers"
        )
        return valid_papers

registered_retrievers = {}

def register_retriever(name:str):
    def decorator(cls):
        registered_retrievers[name] = cls
        cls.name = name
        return cls
    return decorator

def get_retriever_cls(name:str) -> Type[BaseRetriever]:
    if name not in registered_retrievers:
        raise ValueError(f"Retriever {name} not found")
    return registered_retrievers[name]
