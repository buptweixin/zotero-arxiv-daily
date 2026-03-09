import requests
from .base import BaseRetriever, register_retriever
from ..protocol import Paper
from loguru import logger
from typing import Any
from time import sleep

@register_retriever("biorxiv")
class BiorxivRetriever(BaseRetriever):
    server = "biorxiv"

    def __init__(self, config):
        super().__init__(config)
        if self.retriever_config.category is None:
            raise ValueError(f"category must be specified for {self.name}")

    def _retrieve_raw_papers(self) -> list[dict[str, Any]]:
        api_url = f"https://api.biorxiv.org/details/{self.server}/2d"
        retry_num = 10
        delay_time = 10
        logger.info(f"{self.name}: requesting latest papers from {api_url}")
        for i in range(retry_num):
            try:
                response = requests.get(api_url)
                response.raise_for_status()
                logger.info(f"{self.name}: API request succeeded on attempt {i + 1}/{retry_num}")
                break
            except Exception as e:
                if i == retry_num - 1:
                    raise e
                else:
                    logger.warning(f"Failed to retrieve papers: {str(e)}. Retry in {delay_time} seconds.")
                    sleep(delay_time)
        result = response.json()
        collection = result['collection']
        logger.info(f"{self.name}: API returned {len(collection)} papers before date/category filtering")
        if len(collection) == 0:
            logger.warning(f"No paper found. API Message: {result['messages']}")
            return []
        all_dates = set(c['date'] for c in collection)
        latest_date = sorted(all_dates)[-1]
        logger.info(f"{self.name}: latest available date is {latest_date}")
        collection = [c for c in collection if c['date'] == latest_date]
        categories = [c.lower() for c in self.retriever_config.category]
        logger.info(f"{self.name}: filtering latest papers by categories={categories}")
        collection = [c for c in collection if c['category'] in categories]
        logger.info(f"{self.name}: kept {len(collection)} papers after date/category filtering")
        if self.config.executor.debug:
            collection = collection[:10]
            logger.info(f"{self.name}: debug mode enabled, limiting papers to 10")
        return collection


    def convert_to_paper(self, raw_paper:dict[str, Any]) -> Paper | None:
        title = raw_paper['title']
        authors = [a.strip() for a in raw_paper['authors'].split(';')]
        abstract = raw_paper['abstract']
        pdf_url = f"https://www.{self.server}.org/content/{raw_paper['doi']}v{raw_paper['version']}.full.pdf"
        full_text = None # biorxiv forbids scraping its pdf
        return Paper(
            source=self.name,
            title=title,
            authors=authors,
            abstract=abstract,
            url=pdf_url,
            pdf_url=pdf_url,
            full_text=full_text
        )
