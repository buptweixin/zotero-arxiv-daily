from .base import BaseRetriever, register_retriever
import arxiv
from arxiv import Result as ArxivResult
from ..protocol import Paper
from ..utils import extract_markdown_from_pdf, extract_tex_code_from_tar
from tempfile import TemporaryDirectory
import feedparser
from urllib.request import urlopen
from tqdm import tqdm
import os
import shutil
from loguru import logger
@register_retriever("arxiv")
class ArxivRetriever(BaseRetriever):
    def __init__(self, config):
        super().__init__(config)
        if self.config.source.arxiv.category is None:
            raise ValueError("category must be specified for arxiv.")
    def _retrieve_raw_papers(self) -> list[ArxivResult]:
        client = arxiv.Client(num_retries=10,delay_seconds=10)
        query = '+'.join(self.config.source.arxiv.category)
        logger.info(
            f"arxiv: fetching RSS feed for categories={self.config.source.arxiv.category} "
            f"(query={query})"
        )
        # Get the latest paper from arxiv rss feed
        feed = feedparser.parse(f"https://rss.arxiv.org/atom/{query}")
        if 'Feed error for query' in feed.feed.title:
            raise Exception(f"Invalid ARXIV_QUERY: {query}.")
        raw_papers = []
        all_paper_ids = [i.id.removeprefix("oai:arXiv.org:") for i in feed.entries if i.get("arxiv_announce_type","new") == 'new']
        logger.info(f"arxiv: found {len(all_paper_ids)} new paper ids from RSS feed")
        if self.config.executor.debug:
            all_paper_ids = all_paper_ids[:10]
            logger.info("arxiv: debug mode enabled, limiting RSS paper ids to 10")

        # Get full information of each paper from arxiv api
        total_batches = (len(all_paper_ids) + 19) // 20
        bar = tqdm(total=len(all_paper_ids))
        for batch_index, i in enumerate(range(0,len(all_paper_ids),20), start=1):
            batch_ids = all_paper_ids[i:i+20]
            logger.info(
                f"arxiv: retrieving batch {batch_index}/{total_batches} "
                f"({len(batch_ids)} papers) from API"
            )
            search = arxiv.Search(id_list=batch_ids)
            batch = list(client.results(search))
            bar.update(len(batch))
            raw_papers.extend(batch)
            logger.info(
                f"arxiv: retrieved {len(raw_papers)}/{len(all_paper_ids)} papers from API so far"
            )
        bar.close()
        logger.info(f"arxiv: raw paper retrieval finished with {len(raw_papers)} papers")

        return raw_papers

    def convert_to_paper(self, raw_paper:ArxivResult) -> Paper:
        title = raw_paper.title
        authors = [a.name for a in raw_paper.authors]
        abstract = raw_paper.summary
        pdf_url = raw_paper.pdf_url
        download_timeout_seconds = self.config.executor.get("download_timeout_seconds", 60)
        download_timeout_seconds = None if download_timeout_seconds in (None, 0) else float(download_timeout_seconds)
        full_text = extract_text_from_pdf(raw_paper, timeout_seconds=download_timeout_seconds)
        if full_text is None:
            full_text = extract_text_from_tar(raw_paper, timeout_seconds=download_timeout_seconds)
        return Paper(
            source=self.name,
            title=title,
            authors=authors,
            abstract=abstract,
            url=raw_paper.entry_id,
            pdf_url=pdf_url,
            full_text=full_text
        )

def _download_to_path(url: str, path: str, timeout_seconds: float | None) -> None:
    with urlopen(url, timeout=timeout_seconds) as response, open(path, "wb") as output_file:
        shutil.copyfileobj(response, output_file)


def extract_text_from_pdf(paper: ArxivResult, timeout_seconds: float | None = 60) -> str | None:
    with TemporaryDirectory() as temp_dir:
        path = os.path.join(temp_dir, "paper.pdf")
        if paper.pdf_url is None:
            logger.warning(f"No PDF URL available for {paper.title}")
            return None
        try:
            _download_to_path(paper.pdf_url, path, timeout_seconds)
        except Exception as e:
            logger.warning(f"Failed to download PDF of {paper.title}: {e}")
            return None
        try:
            full_text = extract_markdown_from_pdf(path)
        except Exception as e:
            logger.warning(f"Failed to extract full text of {paper.title} from pdf: {e}")
            full_text = None
        return full_text

def extract_text_from_tar(paper: ArxivResult, timeout_seconds: float | None = 60) -> str | None:
    with TemporaryDirectory() as temp_dir:
        path = os.path.join(temp_dir, "paper.tar.gz")
        source_url = paper.source_url()
        if source_url is None:
            logger.warning(f"No source URL available for {paper.title}")
            return None
        try:
            _download_to_path(source_url, path, timeout_seconds)
        except Exception as e:
            logger.warning(f"Failed to download source tarball of {paper.title}: {e}")
            return None
        try:
            file_contents = extract_tex_code_from_tar(path, paper.entry_id)
            if "all" not in file_contents:
                logger.warning(f"Failed to extract full text of {paper.title} from tar: Main tex file not found.")
                return None
            full_text = file_contents["all"]
        except Exception as e:
            logger.warning(f"Failed to extract full text of {paper.title} from tar: {e}")
            full_text = None
        return full_text
