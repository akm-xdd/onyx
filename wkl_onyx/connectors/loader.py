from connectors.base import BaseCrawler
from connectors.google_drive import GoogleDriveCrawler
# from connectors.github import GithubCrawler
# from connectors.notion import NotionCrawler
from connectors.dropbox import DropboxCrawler
# from connectors.jira import JiraCrawler
# from connectors.web import WebCrawler
# from connectors.airtable import AirtableCrawler
# from connectors.gmail import GmailCrawler
# from connectors.sharepoint import SharepointCrawler

"""
Only google_drive and dropbox are complete. Others might break because of too many changes from onyx and in our own code
"""

WKL_CRAWLER_MAP: dict[str, type[BaseCrawler]] = {
    "google_drive": GoogleDriveCrawler,
    # "github": GithubCrawler,
    # "notion": NotionCrawler,
    "dropbox": DropboxCrawler,
    # "jira": JiraCrawler,
    # "web": WebCrawler,
    # "airtable": AirtableCrawler,
    # "gmail": GmailCrawler,
    # "sharepoint": SharepointCrawler,
}


def get_crawler(source_type: str, crawl_job, credential) -> BaseCrawler:
    if source_type not in WKL_CRAWLER_MAP:
        from onyx.configs.constants import DocumentSource
        try:
            DocumentSource(source_type)
        except ValueError:
            raise ValueError(f"Unknown source_type: {source_type}")
        raise ValueError(f"No crawler implemented yet for source_type: {source_type}")
    return WKL_CRAWLER_MAP[source_type](crawl_job, credential)