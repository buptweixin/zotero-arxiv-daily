from abc import ABC, abstractmethod
from omegaconf import DictConfig
from ..protocol import Paper, RawPaperItem
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Type
from loguru import logger
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
        with ProcessPoolExecutor(max_workers=self.config.executor.max_workers) as exec_pool:
            future_to_index = {
                exec_pool.submit(self.convert_to_paper, raw_paper): index
                for index, raw_paper in enumerate(raw_papers)
            }
            for completed_count, future in enumerate(as_completed(future_to_index), start=1):
                index = future_to_index[future]
                papers[index] = future.result()
                if completed_count == 1 or completed_count % progress_interval == 0 or completed_count == raw_paper_count:
                    logger.info(
                        f"{self.name}: converted {completed_count}/{raw_paper_count} papers"
                    )

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
