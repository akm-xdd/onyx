import time
from dataclasses import dataclass, field
from typing import Any

import requests


@dataclass
class ApiCallResult:
    """Represents a single API call made during credential validation."""
    url: str
    method: str
    status_code: int | None
    response_summary: str
    success: bool
    duration_ms: float


@dataclass
class ValidationResult:
    """Result of credential validation."""
    valid: bool
    source_type: str
    api_calls: list[ApiCallResult] = field(default_factory=list)
    error: str | None = None
    error_code: str | None = None


def _make_api_call(
    url: str,
    method: str,
    headers: dict | None = None,
    json_body: dict | None = None,
    timeout: int = 30,
) -> ApiCallResult:
    """Make an HTTP API call and return the result with timing."""
    start_time = time.time()
    status_code = None
    response_summary = ""
    success = False

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=timeout)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=json_body, timeout=timeout)
        else:
            response_summary = f"Unsupported method: {method}"
            return ApiCallResult(
                url=url,
                method=method,
                status_code=None,
                response_summary=response_summary,
                success=False,
                duration_ms=(time.time() - start_time) * 1000,
            )

        status_code = response.status_code

        try:
            response_json = response.json()
            if isinstance(response_json, dict):
                # Summarize key fields, truncate long values
                summary_parts = []
                for key in [
                    "id",
                    "login",
                    "name",
                    "email",
                    "workspace_name",
                    "error",
                    "message",
                    "type",
                    "object",
                ]:
                    if key in response_json:
                        value = response_json[key]
                        if isinstance(value, str) and len(value) > 100:
                            value = value[:100] + "..."
                        summary_parts.append(f"{key}={value}")
                response_summary = (
                    ", ".join(summary_parts) if summary_parts else str(response_json)[:200]
                )
            else:
                response_summary = str(response_json)[:200]
        except Exception:
            response_summary = response.text[:200] if response.text else "Empty response"

        success = 200 <= status_code < 300

    except requests.exceptions.Timeout:
        response_summary = f"Request timed out after {timeout}s"
    except requests.exceptions.ConnectionError as e:
        response_summary = f"Connection error: {str(e)[:100]}"
    except requests.exceptions.RequestException as e:
        response_summary = f"Request error: {str(e)[:100]}"
    except Exception as e:
        response_summary = f"Unexpected error: {str(e)[:100]}"

    duration_ms = (time.time() - start_time) * 1000

    return ApiCallResult(
        url=url,
        method=method,
        status_code=status_code,
        response_summary=response_summary,
        success=success,
        duration_ms=duration_ms,
    )


def validate_notion(credentials: dict[str, Any]) -> ValidationResult:
    """Validate Notion integration token."""
    result = ValidationResult(valid=False, source_type="notion")

    token = credentials.get("notion_integration_token")
    if not token:
        result.error = "Missing notion_integration_token"
        result.error_code = "MISSING_CREDENTIAL"
        return result

    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }

    call_result = _make_api_call(
        url="https://api.notion.com/v1/users/me",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(call_result)

    if not call_result.success:
        if call_result.status_code == 401:
            result.error = "Invalid or expired Notion integration token (HTTP 401)"
            result.error_code = "INVALID_TOKEN"
        elif call_result.status_code == 403:
            result.error = "Notion token does not have permission to access users (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        else:
            result.error = f"Notion API error: {call_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    search_result = _make_api_call(
        url="https://api.notion.com/v1/search",
        method="POST",
        headers=headers,
        json_body={"page_size": 1, "filter": {"property": "object", "value": "page"}},
    )
    result.api_calls.append(search_result)

    if not search_result.success:
        if search_result.status_code == 401:
            result.error = "Invalid or expired Notion integration token (HTTP 401)"
            result.error_code = "INVALID_TOKEN"
        elif search_result.status_code == 403:
            result.error = "Notion token does not have permission to search (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        elif search_result.status_code == 429:
            result.error = "Notion rate limit exceeded (HTTP 429). Please try again later."
            result.error_code = "RATE_LIMITED"
        else:
            result.error = f"Notion search API error: {search_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    result.valid = True
    return result


def validate_github(credentials: dict[str, Any]) -> ValidationResult:
    """Validate GitHub personal access token."""
    result = ValidationResult(valid=False, source_type="github")

    token = credentials.get("github_access_token")
    if not token:
        result.error = "Missing github_access_token"
        result.error_code = "MISSING_CREDENTIAL"
        return result

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    call_result = _make_api_call(
        url="https://api.github.com/user",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(call_result)

    if not call_result.success:
        if call_result.status_code == 401:
            result.error = "Invalid or expired GitHub personal access token (HTTP 401)"
            result.error_code = "INVALID_TOKEN"
        elif call_result.status_code == 403:
            result.error = "GitHub token does not have required permissions (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        else:
            result.error = f"GitHub API error: {call_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    rate_limit_result = _make_api_call(
        url="https://api.github.com/rate_limit",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(rate_limit_result)


    result.valid = True
    return result


def validate_jira(credentials: dict[str, Any]) -> ValidationResult:
    """Validate Jira API token and user email."""
    result = ValidationResult(valid=False, source_type="jira")

    api_token = credentials.get("jira_api_token")
    user_email = credentials.get("jira_user_email")
    base_url = credentials.get("jira_base_url", "").rstrip("/")

    if not api_token:
        result.error = "Missing jira_api_token"
        result.error_code = "MISSING_CREDENTIAL"
        return result

    if not base_url:
        result.error = "Missing jira_base_url"
        result.error_code = "MISSING_CREDENTIAL"
        return result

    # Build Basic Auth header
    import base64

    auth_value = f"{user_email or ''}:{api_token}".encode("utf-8")
    auth_header = base64.b64encode(auth_value).decode("utf-8")

    headers = {
        "Authorization": f"Basic {auth_header}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    # Step 1: Get current user to verify credentials
    call_result = _make_api_call(
        url=f"{base_url}/rest/api/3/myself",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(call_result)

    if not call_result.success:
        if call_result.status_code == 401:
            result.error = "Invalid Jira API token or user email (HTTP 401)"
            result.error_code = "INVALID_TOKEN"
        elif call_result.status_code == 403:
            result.error = "Jira token does not have required permissions (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        elif call_result.status_code == 404:
            result.error = f"Jira instance not found at {base_url} (HTTP 404)"
            result.error_code = "NOT_FOUND"
        else:
            result.error = f"Jira API error: {call_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    # Step 2: Get project list to verify access
    project_result = _make_api_call(
        url=f"{base_url}/rest/api/3/project",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(project_result)

    result.valid = True
    return result


def validate_dropbox(credentials: dict[str, Any]) -> ValidationResult:
    """Validate Dropbox access token."""
    result = ValidationResult(valid=False, source_type="dropbox")

    token = credentials.get("dropbox_access_token")
    if not token:
        result.error = "Missing dropbox_access_token"
        result.error_code = "MISSING_CREDENTIAL"
        return result

    headers = {
        "Authorization": f"Bearer {token}",
    }

    # Step 1: Get current account info to verify token
    # Note: Dropbox API expects an empty body for this endpoint
    # Using data="" to send an explicitly empty body
    start_time = time.time()
    try:
        response = requests.post(
            "https://api.dropboxapi.com/2/users/get_current_account",
            headers=headers,
            data="",  # Explicitly empty body - Dropbox API expects this
            timeout=30,
        )
        duration_ms = (time.time() - start_time) * 1000
        
        # Create a summary of the response
        try:
            response_json = response.json()
            if isinstance(response_json, dict):
                summary_parts = []
                for key in ["name", "email", "account_id"]:
                    if key in response_json:
                        value = response_json[key]
                        if isinstance(value, str) and len(value) > 100:
                            value = value[:100] + "..."
                        summary_parts.append(f"{key}={value}")
                response_summary = ", ".join(summary_parts) if summary_parts else str(response_json)[:200]
            else:
                response_summary = str(response_json)[:200]
        except Exception:
            response_summary = response.text[:200] if response.text else "Empty response"

        call_result = ApiCallResult(
            url="https://api.dropboxapi.com/2/users/get_current_account",
            method="POST",
            status_code=response.status_code,
            response_summary=response_summary,
            success=200 <= response.status_code < 300,
            duration_ms=duration_ms,
        )
    except requests.exceptions.RequestException as e:
        call_result = ApiCallResult(
            url="https://api.dropboxapi.com/2/users/get_current_account",
            method="POST",
            status_code=None,
            response_summary=f"Request error: {str(e)[:100]}",
            success=False,
            duration_ms=(time.time() - start_time) * 1000,
        )

    result.api_calls.append(call_result)

    if not call_result.success:
        if call_result.status_code == 401:
            result.error = "Invalid or expired Dropbox access token (HTTP 401)"
            result.error_code = "INVALID_TOKEN"
        elif call_result.status_code == 403:
            result.error = "Dropbox token does not have required permissions (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        else:
            result.error = f"Dropbox API error: {call_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    result.valid = True
    return result


def validate_airtable(credentials: dict[str, Any]) -> ValidationResult:
    """Validate Airtable personal access token."""
    result = ValidationResult(valid=False, source_type="airtable")

    token = credentials.get("airtable_access_token")
    if not token:
        result.error = "Missing airtable_access_token"
        result.error_code = "MISSING_CREDENTIAL"
        return result

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Step 1: Get meta bases to verify token
    call_result = _make_api_call(
        url="https://api.airtable.com/v0/meta/bases",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(call_result)

    if not call_result.success:
        if call_result.status_code == 401:
            result.error = "Invalid or expired Airtable personal access token (HTTP 401)"
            result.error_code = "INVALID_TOKEN"
        elif call_result.status_code == 403:
            result.error = "Airtable token does not have required permissions (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        elif call_result.status_code == 422:
            result.error = "Airtable token format is invalid (HTTP 422)"
            result.error_code = "INVALID_TOKEN"
        else:
            result.error = f"Airtable API error: {call_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    result.valid = True
    return result


def validate_web(credentials: dict[str, Any], config: dict[str, Any]) -> ValidationResult:
    """Validate web scraper configuration (URL accessibility)."""
    result = ValidationResult(valid=False, source_type="web")

    base_url = config.get("base_url")
    if not base_url:
        result.error = "Missing base_url in config"
        result.error_code = "MISSING_CONFIG"
        return result

    # Step 1: Try to fetch the base URL
    call_result = _make_api_call(
        url=base_url,
        method="GET",
        headers={"User-Agent": "WokeloBot/1.0 (+https://wokelo.ai)"},
        timeout=15,
    )
    result.api_calls.append(call_result)

    if not call_result.success:
        if call_result.status_code == 403:
            result.error = f"Access forbidden to {base_url} (HTTP 403). The site may be blocking scrapers."
            result.error_code = "ACCESS_DENIED"
        elif call_result.status_code == 404:
            result.error = f"URL not found: {base_url} (HTTP 404)"
            result.error_code = "NOT_FOUND"
        elif call_result.status_code is None:
            result.error = f"Could not connect to {base_url}: {call_result.response_summary}"
            result.error_code = "CONNECTION_ERROR"
        else:
            result.error = f"Could not access {base_url}: {call_result.response_summary}"
            result.error_code = "ACCESS_ERROR"
        return result

    result.valid = True
    return result


def _refresh_google_token(
    refresh_token: str,
    token_uri: str,
    client_id: str,
    client_secret: str,
) -> tuple[str | None, str | None]:
    """
    Try to refresh an expired Google access token.
    Returns (new_access_token, error_message) tuple.
    """
    try:
        response = requests.post(
            token_uri,
            data={
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        if response.ok:
            tokens = response.json()
            return tokens.get("access_token"), None
        else:
            return None, f"Token refresh failed: {response.status_code}"
    except Exception as e:
        return None, f"Token refresh error: {str(e)}"


def _extract_json_from_response_summary(response_summary: str) -> dict | None:
    """Try to parse response_summary as JSON, handling both JSON and Python dict string formats."""
    import ast
    # First try json.loads with double quotes
    import json
    try:
        return json.loads(response_summary)
    except Exception:
        pass
    # Fall back to ast.literal_eval for Python dict string representation (single quotes)
    try:
        return ast.literal_eval(response_summary)
    except Exception:
        return None


def validate_google_drive(
    credentials: dict[str, Any], config: dict[str, Any]
) -> ValidationResult:
    """Validate Google Drive OAuth2 access token."""
    result = ValidationResult(valid=False, source_type="google_drive")

    # Extract google_tokens from the credential structure (created via OAuth flow)
    google_tokens = credentials.get("google_tokens", {})
    access_token = google_tokens.get("token")
    refresh_token = google_tokens.get("refresh_token")
    token_uri = google_tokens.get("token_uri", "https://oauth2.googleapis.com/token")
    client_id = google_tokens.get("client_id")
    client_secret = google_tokens.get("client_secret")

    # Step 1: Try to use the access token
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Step 1a: Get user info to verify token
    call_result = _make_api_call(
        url="https://www.googleapis.com/oauth2/v2/userinfo",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(call_result)

    if not call_result.success:
        # Access token failed - try to refresh if we have refresh_token
        if call_result.status_code == 401 and refresh_token and client_id and client_secret:
            # Use _refresh_google_token directly to get proper JSON parsing
            new_access_token, refresh_error = _refresh_google_token(
                refresh_token=refresh_token,
                token_uri=token_uri,
                client_id=client_id,
                client_secret=client_secret,
            )

            if new_access_token:
                # Log the refresh as an API call
                refresh_result = ApiCallResult(
                    url=token_uri,
                    method="POST",
                    status_code=200,
                    response_summary=f"Token refreshed successfully",
                    success=True,
                    duration_ms=0,
                )
                result.api_calls.append(refresh_result)

                # Retry the validation with new token
                headers["Authorization"] = f"Bearer {new_access_token}"
                retry_result = _make_api_call(
                    url="https://www.googleapis.com/oauth2/v2/userinfo",
                    method="GET",
                    headers=headers,
                )
                result.api_calls.append(retry_result)
                if retry_result.success:
                    # Step 2: Check Drive API access with new token
                    drive_result = _make_api_call(
                        url="https://www.googleapis.com/drive/v3/about?fields=storageQuota,user",
                        method="GET",
                        headers=headers,
                    )
                    result.api_calls.append(drive_result)
                    result.valid = True
                    return result
                else:
                    result.error = f"Token refreshed but API still failing: {retry_result.response_summary}"
                    result.error_code = "API_ERROR"
                    return result
            else:
                result.error = f"Token refresh failed: {refresh_error}"
                result.error_code = "API_ERROR"
                return result

        # If we get here, refresh didn't work or wasn't attempted
        if call_result.status_code == 401:
            if refresh_token:
                result.error = "Access token expired. Token refresh also failed. Please re-authenticate via OAuth."
            else:
                result.error = "Invalid or expired Google access token (HTTP 401). No refresh token available."
            result.error_code = "TOKEN_EXPIRED"
        elif call_result.status_code == 403:
            result.error = "Google token does not have required permissions (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        else:
            result.error = f"Google API error: {call_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    # Step 2: Check Drive API access
    drive_result = _make_api_call(
        url="https://www.googleapis.com/drive/v3/about?fields=storageQuota,user",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(drive_result)

    result.valid = True
    return result


def validate_gmail(credentials: dict[str, Any]) -> ValidationResult:
    """Validate Gmail OAuth2 access token."""
    result = ValidationResult(valid=False, source_type="gmail")

    # Extract google_tokens from the credential structure (created via OAuth flow)
    google_tokens = credentials.get("google_tokens", {})
    access_token = google_tokens.get("token")
    refresh_token = google_tokens.get("refresh_token")
    token_uri = google_tokens.get("token_uri", "https://oauth2.googleapis.com/token")
    client_id = google_tokens.get("client_id")
    client_secret = google_tokens.get("client_secret")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Step 1: Get profile to verify token
    call_result = _make_api_call(
        url="https://gmail.googleapis.com/gmail/v1/profile",
        method="GET",
        headers=headers,
    )
    result.api_calls.append(call_result)

    if not call_result.success:
        # Access token failed - try to refresh if we have refresh_token
        if call_result.status_code == 401 and refresh_token and client_id and client_secret:
            # Use _refresh_google_token directly to get proper JSON parsing
            new_access_token, refresh_error = _refresh_google_token(
                refresh_token=refresh_token,
                token_uri=token_uri,
                client_id=client_id,
                client_secret=client_secret,
            )

            if new_access_token:
                # Log the refresh as an API call
                refresh_result = ApiCallResult(
                    url=token_uri,
                    method="POST",
                    status_code=200,
                    response_summary=f"Token refreshed successfully",
                    success=True,
                    duration_ms=0,
                )
                result.api_calls.append(refresh_result)

                # Retry the validation with new token
                headers["Authorization"] = f"Bearer {new_access_token}"
                retry_result = _make_api_call(
                    url="https://gmail.googleapis.com/gmail/v1/profile",
                    method="GET",
                    headers=headers,
                )
                result.api_calls.append(retry_result)
                if retry_result.success:
                    result.valid = True
                    return result
                else:
                    result.error = f"Token refreshed but API still failing: {retry_result.response_summary}"
                    result.error_code = "API_ERROR"
                    return result
            else:
                result.error = f"Token refresh failed: {refresh_error}"
                result.error_code = "API_ERROR"
                return result

        if call_result.status_code == 401:
            if refresh_token:
                result.error = "Access token expired. Token refresh also failed. Please re-authenticate via OAuth."
            else:
                result.error = "Invalid or expired Gmail access token (HTTP 401). No refresh token available."
            result.error_code = "TOKEN_EXPIRED"
        elif call_result.status_code == 403:
            result.error = "Gmail token does not have required permissions (HTTP 403)"
            result.error_code = "INSUFFICIENT_PERMISSIONS"
        else:
            result.error = f"Gmail API error: {call_result.response_summary}"
            result.error_code = "API_ERROR"
        return result

    result.valid = True
    return result


# Validator map for easy lookup
VALIDATOR_MAP = {
    "notion": validate_notion,
    "github": validate_github,
    "jira": validate_jira,
    "dropbox": validate_dropbox,
    "airtable": validate_airtable,
    "web": validate_web,
    "google_drive": validate_google_drive,
    "gmail": validate_gmail,
}
