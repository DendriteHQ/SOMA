"""Run coordinator service entry point."""
from app.bootstrap import create_app


app = create_app("coordinator")
