"""
Google OAuth 2.0 + JWT authentication for DataIQ.

Setup:
  1. Go to https://console.cloud.google.com → APIs & Services → Credentials
  2. Create OAuth 2.0 Client ID (type: Web application)
  3. Add authorised redirect URI: http://localhost:8000/api/auth/google/callback
  4. Copy Client ID and Client Secret into backend/.env:
       GOOGLE_CLIENT_ID=...
       GOOGLE_CLIENT_SECRET=...
       JWT_SECRET=<any long random string>
       FRONTEND_URL=http://localhost:3000
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

try:
    import httpx
except ImportError:
    httpx = None  # Optional — only needed for Google OAuth

try:
    from jose import jwt, JWTError
    _JOSE_AVAILABLE = True
except ImportError:
    jwt = None  # type: ignore
    JWTError = Exception  # type: ignore
    _JOSE_AVAILABLE = False

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from db.database import get_db
from db.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])

# ── Config ────────────────────────────────────────────────────────────────────

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
FRONTEND_URL         = os.getenv("FRONTEND_URL", "http://localhost:3000")
JWT_SECRET           = os.getenv("JWT_SECRET", "dataiq-dev-secret-change-in-prod")
JWT_ALGORITHM        = "HS256"
JWT_EXPIRE_DAYS      = 30

GOOGLE_AUTH_URL     = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL    = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"


# ── JWT helpers ───────────────────────────────────────────────────────────────

def _create_jwt(user: User) -> str:
    if not _JOSE_AVAILABLE:
        raise HTTPException(status_code=503, detail="python-jose not installed — JWT unavailable")
    payload = {
        "sub":     user.id,
        "email":   user.email,
        "name":    user.name,
        "picture": user.picture,
        "role":    user.role,
        "exp":     datetime.utcnow() + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _decode_jwt(token: str) -> dict:
    if not _JOSE_AVAILABLE:
        raise HTTPException(status_code=503, detail="python-jose not installed — JWT unavailable")
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")


def get_current_user(
    authorization: Optional[str] = None,
    db: Session = Depends(get_db),
) -> Optional[User]:
    """Dependency — returns the authenticated User or None (for optional auth)."""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization[7:]
    payload = _decode_jwt(token)
    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user


def require_user(
    authorization: Optional[str] = None,
    db: Session = Depends(get_db),
) -> User:
    """Dependency — returns User or raises 401."""
    user = get_current_user(authorization, db)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/google")
def google_login():
    """Redirect to Google OAuth consent screen."""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=503,
            detail="Google OAuth not configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in backend/.env"
        )
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  f"{_backend_url()}/api/auth/google/callback",
        "response_type": "code",
        "scope":         "openid email profile",
        "access_type":   "offline",
        "prompt":        "select_account",
    }
    url = GOOGLE_AUTH_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    return RedirectResponse(url)


@router.get("/google/callback")
async def google_callback(
    code: str = Query(...),
    db: Session = Depends(get_db),
):
    """Exchange Google auth code for user info, issue JWT, redirect to frontend."""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=503, detail="Google OAuth not configured")

    backend_url = _backend_url()

    if httpx is None:
        raise HTTPException(status_code=503, detail="httpx not installed — Google OAuth unavailable")
    async with httpx.AsyncClient() as client:
        # Exchange code for tokens
        token_resp = await client.post(GOOGLE_TOKEN_URL, data={
            "code":          code,
            "client_id":     GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri":  f"{backend_url}/api/auth/google/callback",
            "grant_type":    "authorization_code",
        })
        if token_resp.status_code != 200:
            logger.error(f"Google token exchange failed: {token_resp.text}")
            return RedirectResponse(f"{FRONTEND_URL}?auth_error=token_exchange_failed")

        tokens = token_resp.json()
        access_token = tokens.get("access_token")

        # Fetch user info from Google
        userinfo_resp = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if userinfo_resp.status_code != 200:
            return RedirectResponse(f"{FRONTEND_URL}?auth_error=userinfo_failed")

        info = userinfo_resp.json()

    google_id = info.get("sub")
    email     = info.get("email", "")
    name      = info.get("name", email.split("@")[0])
    picture   = info.get("picture", "")

    # Upsert user
    user = db.query(User).filter(User.google_id == google_id).first()
    if not user:
        user = db.query(User).filter(User.email == email).first()

    if user:
        user.google_id  = google_id
        user.name       = name
        user.picture    = picture
        user.last_login = datetime.utcnow()
    else:
        # First user becomes admin
        is_first = db.query(User).count() == 0
        user = User(
            email      = email,
            google_id  = google_id,
            name       = name,
            picture    = picture,
            role       = "admin" if is_first else "viewer",
            last_login = datetime.utcnow(),
        )
        db.add(user)

    db.commit()
    db.refresh(user)

    jwt_token = _create_jwt(user)
    logger.info(f"User authenticated: {email} (role={user.role})")

    return RedirectResponse(f"{FRONTEND_URL}?token={jwt_token}")


@router.get("/me")
def get_me(db: Session = Depends(get_db), authorization: Optional[str] = None):
    """Return current user info."""
    from fastapi import Request
    user = get_current_user(authorization, db)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {
        "id":      user.id,
        "email":   user.email,
        "name":    user.name,
        "picture": user.picture,
        "role":    user.role,
    }


@router.post("/logout")
def logout():
    """Client-side logout — just returns 200 (JWT is stateless)."""
    return {"ok": True, "message": "Logged out. Please discard your token."}


@router.get("/status")
def auth_status():
    """Returns whether Google OAuth is configured."""
    configured = bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)
    return {
        "google_oauth_configured": configured,
        "message": "Ready" if configured else "Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in backend/.env",
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _backend_url() -> str:
    return os.getenv("BACKEND_URL", "http://localhost:8000")
