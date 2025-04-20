import os
import json
import time
import random
from glob import glob
from datetime import datetime
from dataclasses import dataclass
from functools import wraps

import arxiv
from tqdm import tqdm
from rich.console import Console

console = Console()


def retry_decorator(max_attempts=3, initial_delay=1, backoff_factor=2, exceptions=(Exception,)):
    """
    A decorator that retries the decorated function when specified exceptions occur.
    
    Args:
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        backoff_factor: Factor by which the delay increases with each retry
        exceptions: Tuple of exceptions that trigger a retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            delay = initial_delay
            
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        console.log(f"[bold red]Failed after {max_attempts} attempts: {str(e)}[/bold red]")
                        raise
                    
                    # Add some jitter to avoid thundering herd problem
                    jitter = random.uniform(0.1, 0.3) * delay
                    wait_time = delay + jitter
                    
                    console.log(f"[yellow]Attempt {attempt} failed: {str(e)}. Retrying in {wait_time:.2f}s...[/yellow]")
                    time.sleep(wait_time)
                    
                    # Exponential backoff
                    delay *= backoff_factor
        return wrapper
    return decorator


@dataclass
class ArxivPaper:
    title: str
    abstract: str
    published: str
    link: str
    categories: list[str]
    primary_category: str


@retry_decorator(
    max_attempts=5, initial_delay=2, backoff_factor=2, 
    exceptions=(arxiv.HTTPError, arxiv.UnexpectedEmptyPageError, ConnectionError, TimeoutError)
)
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
        papers.append(paper)

    return papers


@retry_decorator(max_attempts=3, initial_delay=1, exceptions=(IOError, OSError))
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
    
    # Add retry for the entire subject processing
    for subject in subjects:
        try:
            papers = fetch_arxiv_papers(subject, max_results=200)
            save_to_jsonl(papers)
        except Exception as e:
            console.log(f"Failed to process subject {subject}: {str(e)}")
            # Continue with next subject instead of failing the entire process
            continue


if __name__ == '__main__':
    # Make sure the data directory exists
    os.makedirs('data', exist_ok=True)
    main()
