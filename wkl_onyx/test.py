# scripts/seed_connector_types.py
from core.db import SessionLocal
from models.connector_type import ConnectorType
from models.connector_field import ConnectorFieldSchema

CONNECTOR_TYPES = [
    {
        "source_type": "google_drive",
        "display_name": "Google Drive",
        "auth_type": "oauth",
        "oauth_url": "/oauth/authorize/google",
        "fields": [
            {"field_category": "config", "field_name": "include_my_drives", "field_type": "bool", "label": "Include My Drive", "required": False, "default_value": {"value": True}, "display_order": 1},
            {"field_category": "config", "field_name": "include_shared_drives", "field_type": "bool", "label": "Include Shared Drives", "required": False, "default_value": {"value": False}, "display_order": 2},
            {"field_category": "config", "field_name": "include_files_shared_with_me", "field_type": "bool", "label": "Include Shared With Me", "required": False, "default_value": {"value": False}, "display_order": 3},
            {"field_category": "config", "field_name": "shared_folder_urls", "field_type": "str", "label": "Shared Folder URLs (comma-separated)", "required": False, "display_order": 4},
            {"field_category": "config", "field_name": "my_drive_emails", "field_type": "str", "label": "My Drive Emails (comma-separated)", "required": False, "display_order": 5},
        ],
    },
    {
        "source_type": "github",
        "display_name": "GitHub",
        "auth_type": "token",
        "fields": [
            {"field_category": "credential", "field_name": "github_access_token", "field_type": "str", "label": "Personal Access Token", "required": True, "display_order": 1},
            {"field_category": "config", "field_name": "repo_owner", "field_type": "str", "label": "Repository Owner", "required": True, "display_order": 1},
            {"field_category": "config", "field_name": "repositories", "field_type": "str", "label": "Repositories (comma-separated, empty = all)", "required": False, "display_order": 2},
            {"field_category": "config", "field_name": "include_prs", "field_type": "bool", "label": "Include Pull Requests", "required": False, "default_value": {"value": True}, "display_order": 3},
            {"field_category": "config", "field_name": "include_issues", "field_type": "bool", "label": "Include Issues", "required": False, "default_value": {"value": True}, "display_order": 4},
            {"field_category": "config", "field_name": "state_filter", "field_type": "str", "label": "State Filter", "required": False, "default_value": {"value": "all"}, "options": ["all", "open", "closed"], "display_order": 5},
        ],
    },
    {
        "source_type": "jira",
        "display_name": "Jira",
        "auth_type": "token",
        "fields": [
            {"field_category": "credential", "field_name": "jira_user_email", "field_type": "str", "label": "Jira User Email (required for Cloud)", "required": False, "display_order": 1},
            {"field_category": "credential", "field_name": "jira_api_token", "field_type": "str", "label": "API Token", "required": True, "display_order": 2},
            {"field_category": "config", "field_name": "jira_base_url", "field_type": "str", "label": "Jira Base URL", "required": True, "display_order": 1},
            {"field_category": "config", "field_name": "project_key", "field_type": "str", "label": "Project Key", "required": False, "display_order": 2},
            {"field_category": "config", "field_name": "jql_query", "field_type": "str", "label": "Custom JQL Query", "required": False, "display_order": 3},
            {"field_category": "config", "field_name": "labels_to_skip", "field_type": "list[str]", "label": "Labels to Skip (comma-separated)", "required": False, "default_value": {"value": []}, "display_order": 4},
        ],
    },
    {
        "source_type": "notion",
        "display_name": "Notion",
        "auth_type": "token",
        "fields": [
            {"field_category": "credential", "field_name": "notion_integration_token", "field_type": "str", "label": "Integration Token", "required": True, "display_order": 1},
            {"field_category": "config", "field_name": "root_page_id", "field_type": "str", "label": "Root Page ID", "required": False, "display_order": 1},
            {"field_category": "config", "field_name": "recursive_index_enabled", "field_type": "bool", "label": "Recursive Indexing", "required": False, "default_value": {"value": True}, "display_order": 2},
        ],
    },
    {
        "source_type": "dropbox",
        "display_name": "Dropbox",
        "auth_type": "token",
        "fields": [
            {"field_category": "credential", "field_name": "dropbox_access_token", "field_type": "str", "label": "Access Token", "required": True, "display_order": 1},
        ],
    },
    {
        "source_type": "web",
        "display_name": "Web Scraper",
        "auth_type": "none",
        "fields": [
            {"field_category": "config", "field_name": "base_url", "field_type": "str", "label": "URL", "required": True, "display_order": 1},
            {"field_category": "config", "field_name": "web_connector_type", "field_type": "str", "label": "Scrape Type", "required": False, "default_value": {"value": "recursive"}, "options": ["recursive", "single", "sitemap"], "display_order": 2},
            {"field_category": "config", "field_name": "scroll_before_scraping", "field_type": "bool", "label": "Scroll Before Scraping", "required": False, "default_value": {"value": False}, "display_order": 3},
        ],
    },
    {
        "source_type": "airtable",
        "display_name": "Airtable",
        "auth_type": "token",
        "fields": [
            {"field_category": "credential", "field_name": "airtable_access_token", "field_type": "str", "label": "Personal Access Token", "required": True, "display_order": 1},
            {"field_category": "config", "field_name": "airtable_url", "field_type": "str", "label": "Airtable URL (optional, auto-extracts base/table/view)", "required": False, "display_order": 1},
            {"field_category": "config", "field_name": "base_id", "field_type": "str", "label": "Base ID (if no URL)", "required": False, "display_order": 2},
            {"field_category": "config", "field_name": "table_name_or_id", "field_type": "str", "label": "Table Name or ID (if no URL)", "required": False, "display_order": 3},
            {"field_category": "config", "field_name": "view_id", "field_type": "str", "label": "View ID (optional)", "required": False, "display_order": 4},
            {"field_category": "config", "field_name": "treat_all_non_attachment_fields_as_metadata", "field_type": "bool", "label": "Treat all non-attachment fields as metadata", "required": False, "default_value": {"value": False}, "display_order": 5},
        ],
    },
    {
        "source_type": "gmail",
        "display_name": "Gmail",
        "auth_type": "oauth",
        "oauth_url": "/oauth/authorize/gmail",
        "fields": [],
    },
]


def seed():
    db = SessionLocal()
    try:
        for ct in CONNECTOR_TYPES:
            fields_data = ct.pop("fields", [])
            existing = db.query(ConnectorType).filter(ConnectorType.source_type == ct["source_type"]).first()

            if existing:
                existing.display_name = ct["display_name"]
                existing.auth_type = ct["auth_type"]
                existing.oauth_url = ct.get("oauth_url")
                # Delete old fields and re-insert
                db.query(ConnectorFieldSchema).filter(ConnectorFieldSchema.connector_type_id == existing.id).delete()
                db.flush()
                for field in fields_data:
                    db.add(ConnectorFieldSchema(connector_type_id=existing.id, **field))
            else:
                connector_type = ConnectorType(
                    source_type=ct["source_type"],
                    display_name=ct["display_name"],
                    auth_type=ct["auth_type"],
                    oauth_url=ct.get("oauth_url"),
                )
                db.add(connector_type)
                db.flush()
                for field in fields_data:
                    db.add(ConnectorFieldSchema(connector_type_id=connector_type.id, **field))

        db.commit()
        print(f"Seeded {len(CONNECTOR_TYPES)} connector types")
    finally:
        db.close()


if __name__ == "__main__":
    seed()