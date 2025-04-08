import os
import json
from glob import glob
from datetime import datetime
from dataclasses import dataclass

import arxiv
from tqdm import tqdm
from retry import retry
from rich.console import Console

console = Console()


@dataclass
class ArxivPaper:

    title: str
    abstract: str
    published: str
    link: str
    categories: list[str]
    primary_category: str


# @retry(tries=5, delay=1, backoff=2)
def fetch_arxiv_papers(subject, max_results: int = 10):
    console.log(f"Starting arxiv search `{subject}`.")
    client = arxiv.Client()
    search = arxiv.Search(
        query=f"cat:{subject}", 
        max_results=max_results, 
        sort_by=arxiv.SortCriterion.SubmittedDate,
    )
    results = client.results(search)
    results = list(results)
    console.log(f"Found {len(results)} results.")
    
    papers = []
    for result in results:
        published = result.published.isoformat()
        dt = datetime.fromisoformat(published)
        date_part = dt.strftime('%Y-%m-%d')
        time_part = dt.strftime('%H:%M:%S')

        paper = ArxivPaper(
            title=result.title, 
            abstract=result.summary, 
            published=f"{date_part} {time_part}", 
            link=result.entry_id, 
            categories=result.categories, 
            primary_category=result.primary_category
        )
        # console.log(paper)
        papers.append(paper)

    return papers


def save_to_jsonl(papers: list[ArxivPaper]):
    existing_papers = set()
    for jsonl_filepath in glob(os.path.join('data', '*.jsonl')):
        with open(jsonl_filepath, 'r') as f:
            for line in f:
                entry = json.loads(line)
                paper = ArxivPaper(**entry)
                existing_papers.add(paper.link)

    for paper in papers:
        published = paper.published
        date_part, time_part = published.split()

        jsonl_filepath = os.path.join('data', f'{date_part}.jsonl')
        if paper.link not in existing_papers:
            with open(jsonl_filepath, 'a') as f:
                entry = paper.__dict__
                f.write(json.dumps(entry) + '\n')
        else:
            console.log(f"Skip. Paper `{paper.link}` exists.")


def main():
    subjects = [
        'cs.CL', 'cs.AI', 'cs.CV', 'cs.DM', 'cs.IR', 'cs.IT', 'cs.LG', 'cs.MA', 'cs.NA', 
        'q-fin.CP', 'q-fin.MF', 'q-fin.ST', 'q-fin.TR', 
        'stat.ML', 
        'eess.AS', 'eess.SP'
    ]
    for subject in subjects:
        papers = fetch_arxiv_papers(subject, max_results=200)
        save_to_jsonl(papers)


if __name__ == '__main__':
    main()