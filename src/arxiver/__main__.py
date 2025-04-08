from radicli import Radicli, Arg

cli = Radicli()


@cli.command(
    "download", 
    max_results=Arg(
        "--max_results", 
        help="The maximum number of results to be returned. To fetch every result available, set `max_results=None`."
    )
)
def download(max_results: int = 10):
    """Download new data."""
    from .download import fetch_arxiv_papers, save_to_jsonl

    subjects = [
        'cs.CL', 'cs.AI', 'cs.CV', 'cs.DM', 'cs.IR', 'cs.IT', 'cs.LG', 'cs.MA', 'cs.NA', 
        'q-fin.CP', 'q-fin.MF', 'q-fin.ST', 'q-fin.TR', 
        'stat.ML', 
        'eess.AS', 'eess.SP'
    ]
    for subject in subjects:
        papers = fetch_arxiv_papers(subject, max_results=max_results)
        save_to_jsonl(papers)


if __name__ == "__main__":
    cli.run()