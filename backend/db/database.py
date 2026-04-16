"""
Database layer — SQLite for local dev, easily swapped for PostgreSQL in prod.
Uses SQLAlchemy 2.x synchronous engine with a thread-local session.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Allow DATABASE_URL env override for prod (e.g. postgresql://...)
# Default to user's home directory so SQLite avoids network-mounted filesystem issues
_default_db = os.path.join(os.path.expanduser("~"), "dataiq.db")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{_default_db}")

# SQLite needs check_same_thread=False when used in FastAPI (multi-threaded)
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    echo=False,        # set True to log SQL in dev
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base = declarative_base()


def get_db():
    """FastAPI dependency — yields a DB session, always closes on exit."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables if they don't exist. Called at app startup."""
    # Import models so Base knows about them before create_all
    from db import models  # noqa: F401
    Base.metadata.create_all(bind=engine)
