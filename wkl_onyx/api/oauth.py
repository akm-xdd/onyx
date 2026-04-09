# api/oauth.py
"""
Purely for backend testing, will be removed in production. From frontend we will handle the oauth flow.
"""
import requests
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from core.config import settings
from core.db import get_db
from models.credential import Credential

router = APIRouter(prefix="/oauth", tags=["oauth"])

GOOGLE_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
    "https://www.googleapis.com/auth/admin.directory.group.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]


@router.get("/authorize/gmail")
def authorize_gmail():
    params = {
        "client_id": settings.GOOGLE_CLIENT_ID,
        "redirect_uri": settings.GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(GMAIL_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": "gmail",  # To differentiate in callback
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return RedirectResponse(f"{GOOGLE_AUTH_URL}?{query}")


@router.get("/callback/google")
def google_callback(code: str, state: str = "google_drive", db: Session = Depends(get_db)):
    # Determine source type from state
    source_type = "gmail" if state == "gmail" else "google_drive"
    scopes = GMAIL_SCOPES if source_type == "gmail" else GOOGLE_DRIVE_SCOPES

    token_response = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "code": code,
            "client_id": settings.GOOGLE_CLIENT_ID,
            "client_secret": settings.GOOGLE_CLIENT_SECRET,
            "redirect_uri": settings.GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        },
    )

    if not token_response.ok:
        raise HTTPException(
            status_code=400,
            detail=f"Token exchange failed: {token_response.text}",
        )

    tokens = token_response.json()

    userinfo_response = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    if not userinfo_response.ok:
        raise HTTPException(status_code=400, detail="Failed to fetch user info")

    user_email = userinfo_response.json().get("email")

    credential = Credential(
        source_type=source_type,
        credential_json={
            "google_tokens": {
                "token": tokens["access_token"],
                "refresh_token": tokens.get("refresh_token"),
                "token_uri": GOOGLE_TOKEN_URL,
                "client_id": settings.GOOGLE_CLIENT_ID,
                "client_secret": settings.GOOGLE_CLIENT_SECRET,
                "scopes": scopes,
            },
            "google_primary_admin": user_email,
            "authentication_method": "uploaded",
        },
    )
    db.add(credential)
    db.commit()
    db.refresh(credential)

    return {"credential_id": credential.id, "email": user_email, "source_type": source_type}