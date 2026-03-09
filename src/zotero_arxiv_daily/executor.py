from loguru import logger
from pyzotero import zotero
from omegaconf import DictConfig
from .utils import glob_match
from .retriever import get_retriever_cls
from .protocol import CorpusPaper
import random
from datetime import datetime
from .reranker import get_reranker_cls
from .construct_email import render_email
from .utils import send_email
from openai import OpenAI
from tqdm import tqdm
class Executor:
    def __init__(self, config:DictConfig):
        self.config = config
        self.retrievers = {
            source: get_retriever_cls(source)(config) for source in config.executor.source
        }
        self.reranker = get_reranker_cls(config.executor.reranker)(config)
        self.openai_client = OpenAI(api_key=config.llm.api.key, base_url=config.llm.api.base_url)
    def fetch_zotero_corpus(self) -> list[CorpusPaper]:
        logger.info("Fetching zotero corpus")
        zot = zotero.Zotero(self.config.zotero.user_id, 'user', self.config.zotero.api_key)
        collections = zot.everything(zot.collections())
        collections = {c['key']:c for c in collections}
        logger.info(f"Fetched {len(collections)} zotero collections")
        corpus = zot.everything(zot.items(itemType='conferencePaper || journalArticle || preprint'))
        logger.info(f"Fetched {len(corpus)} zotero items before abstract filtering")
        corpus = [c for c in corpus if c['data']['abstractNote'] != '']
        def get_collection_path(col_key:str) -> str:
            if p := collections[col_key]['data']['parentCollection']:
                return get_collection_path(p) + '/' + collections[col_key]['data']['name']
            else:
                return collections[col_key]['data']['name']
        for c in corpus:
            paths = [get_collection_path(col) for col in c['data']['collections']]
            c['paths'] = paths
        logger.info(f"Fetched {len(corpus)} zotero papers")
        return [CorpusPaper(
            title=c['data']['title'],
            abstract=c['data']['abstractNote'],
            added_date=datetime.strptime(c['data']['dateAdded'], '%Y-%m-%dT%H:%M:%SZ'),
            paths=c['paths']
        ) for c in corpus]
    
    def filter_corpus(self, corpus:list[CorpusPaper]) -> list[CorpusPaper]:
        if not self.config.zotero.include_path:
            logger.info("No zotero include_path configured, using full corpus")
            return corpus
        new_corpus = []
        logger.info(f"Selecting zotero papers matching include_path: {self.config.zotero.include_path}")
        for c in corpus:
            match_results = [glob_match(p, self.config.zotero.include_path) for p in c.paths]
            if any(match_results):
                new_corpus.append(c)
        samples = random.sample(new_corpus, min(5, len(new_corpus)))
        samples = '\n'.join([c.title + ' - ' + '\n'.join(c.paths) for c in samples])
        logger.info(f"Selected {len(new_corpus)} zotero papers:\n{samples}\n...")
        return new_corpus

    
    def run(self):
        logger.info(
            f"Starting pipeline with sources={list(self.retrievers.keys())}, "
            f"reranker={self.config.executor.reranker}, max_paper_num={self.config.executor.max_paper_num}"
        )
        corpus = self.fetch_zotero_corpus()
        corpus = self.filter_corpus(corpus)
        if len(corpus) == 0:
            logger.error(f"No zotero papers found. Please check your zotero settings:\n{self.config.zotero}")
            return
        all_papers = []
        total_sources = len(self.retrievers)
        for source_index, (source, retriever) in enumerate(self.retrievers.items(), start=1):
            logger.info(f"[{source_index}/{total_sources}] Retrieving {source} papers...")
            papers = retriever.retrieve_papers()
            if len(papers) == 0:
                logger.info(f"No {source} papers found")
                continue
            logger.info(f"Retrieved {len(papers)} {source} papers")
            all_papers.extend(papers)
            logger.info(f"Accumulated {len(all_papers)} papers after {source}")
        logger.info(f"Total {len(all_papers)} papers retrieved from all sources")
        reranked_papers = []
        if len(all_papers) > 0:
            logger.info(
                f"Reranking {len(all_papers)} candidate papers against {len(corpus)} zotero corpus papers..."
            )
            reranked_papers = self.reranker.rerank(all_papers, corpus)
            reranked_papers = reranked_papers[:self.config.executor.max_paper_num]
            logger.info(
                f"Reranking finished, keeping top {len(reranked_papers)} papers for enrichment"
            )
            logger.info("Generating TLDR and affiliations...")
            for paper_index, p in enumerate(tqdm(reranked_papers), start=1):
                logger.info(
                    f"[{paper_index}/{len(reranked_papers)}] Generating TLDR for {p.title}"
                )
                p.generate_tldr(self.openai_client, self.config.llm)
                logger.info(
                    f"[{paper_index}/{len(reranked_papers)}] Extracting affiliations for {p.title}"
                )
                p.generate_affiliations(self.openai_client, self.config.llm)
            logger.info("Finished generating TLDR and affiliations")
        elif not self.config.executor.send_empty:
            logger.info("No new papers found. No email will be sent.")
            return
        logger.info("Sending email...")
        email_content = render_email(reranked_papers)
        send_email(self.config, email_content)
        logger.info("Email sent successfully")
