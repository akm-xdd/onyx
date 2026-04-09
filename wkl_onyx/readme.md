# WKL-Connectors

This is the connector service for Wokelo. It is responsible for crawling data from various sources and uploading it to the Wokelo storage.

The service is dumb. It only picks up files and upserts them in storage. It does not handle deletes. If a file is newly created or modified, it will incrementally reflect that change. But if a file is deleted from the source, it will not be deleted from the storage. This is by design.

## Basic Setup

```bash
# change directory to wkl_onyx (this is our current directory)
cd wkl_onyx

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the server
uvicorn main:app --reload


# Run celery worker (solo mode for local dev)
celery -A core.celery_app worker --loglevel=info --pool=solo

# Run celery beat (if you want to check incremental crawl)
celery -A core.celery_app beat --loglevel=info

```

## Basic Info

The server only expects credentials from the frontend. Backend will not handle any oauth flow. The frontend will then send rrequest to create a CrawlJob. Each crawl job defines what to crawl and how to crawl it, based on the type of source.

For Google Drive:
Specific options

- Shared Drive URLs (Drives shared to a user)

- Particular Folder URLs

General Crawling

- All files and folders

For Dropbox:

- Particular Folder Paths

General Crawling

- All files and folders



## Flow

1. Frontend sends request to create a Credential.
2. Frontend sends request to create a CrawlJob.
3. Backend triggers an initial full crawl, which will crawl all files and folders from the specified config, for example all files from the specified folder.
4. Backend will then incrementally keep syncing by running a job every 5 minutes. If a new file is added (created date > last run) or if a file is modified (modified date > last run), it will be crawled and upserted in storage.
5. User has option to cancel a running job, and also to manually start a full crawl or incremental crawl.
6. If a run has failed files, they are added to a failed files table, and retried once immediately after the run ends. If they still fail, they are not retried again. User will be notified on the interface for failures. This is an additional layer on top of Onyx's retry mechanism.

If a job is deleted, it is soft deleted. The job will not be run again, but the existing data will be retained.

Status Endpoints are present to check the status of a connector - number of files, last run time, errors if any, etc.
