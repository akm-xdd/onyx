"""
Purely for backend testing, will be removed in production. From frontend we will handle the oauth flow.
"""

import httpx
import secrets
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

import redis
from core.config import settings
from core.db import get_db
from models.credential import Credential

router = APIRouter(prefix="/connectors/dropbox", tags=["dropbox-oauth"])

_redis = redis.from_url(settings.REDIS_URL)
STATE_TTL = 600  # 10 min

REDIRECT_URI = f"{settings.BACKEND_URL}/connectors/dropbox/oauth/callback"


@router.get("/oauth/start")
def start(user_email: str = Query(...), user_id: int = Query(...)):
    """UI calls this, redirects user to the returned auth_url."""
    state = secrets.token_urlsafe(32)
    _redis.setex(
        f"dropbox_oauth:{state}",
        STATE_TTL,
        f"{user_id}:{user_email}",
    )
    auth_url = (
        "https://www.dropbox.com/oauth2/authorize"
        f"?client_id={settings.DROPBOX_APP_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
        "&token_access_type=offline"
        f"&state={state}"
    )
    return {"auth_url": auth_url}


@router.get("/oauth/callback")
def callback(
    code: str = Query(...),
    state: str = Query(...),
    db: Session = Depends(get_db),
):
    """Dropbox redirects here after user consents."""
    stored = _redis.get(f"dropbox_oauth:{state}")
    if not stored:
        raise HTTPException(400, "Invalid or expired state")
    _redis.delete(f"dropbox_oauth:{state}")

    user_id_str, user_email = stored.decode().split(":", 1)
    user_id = int(user_id_str)

    # Exchange code for tokens
    resp = httpx.post(
        "https://api.dropboxapi.com/oauth2/token",
        data={
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI,
            "client_id": settings.DROPBOX_APP_KEY,
            "client_secret": settings.DROPBOX_APP_SECRET,
        },
        timeout=15,
    )
    if resp.status_code != 200:
        raise HTTPException(400, f"Token exchange failed: {resp.text}")

    data = resp.json()
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=data["expires_in"])

    credential = Credential(
        source_type="dropbox",
        credential_json={
            "dropbox_access_token": data["access_token"],
            "dropbox_refresh_token": data["refresh_token"],
            "dropbox_expires_at": expires_at.isoformat(),
            "account_id": data.get("account_id"),
        },
        user_id=user_id,
        user_email=user_email,
    )
    db.add(credential)
    db.commit()
    db.refresh(credential)

    # Redirect back to your UI with the credential_id
    return RedirectResponse(
        f"{settings.FRONTEND_URL}/connectors/dropbox/connected?credential_id={credential.id}"
    )
    

@router.get("/oauth/initiate")
def initiate(user_email: str = Query(...), user_id: int = Query(...)):
    """Browser-friendly: hit this URL directly and it redirects you to Dropbox."""
    state = secrets.token_urlsafe(32)
    _redis.setex(
        f"dropbox_oauth:{state}",
        STATE_TTL,
        f"{user_id}:{user_email}",
    )
    auth_url = (
        "https://www.dropbox.com/oauth2/authorize"
        f"?client_id={settings.DROPBOX_APP_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
        "&token_access_type=offline"
        f"&state={state}"
    )
    return RedirectResponse(auth_url)