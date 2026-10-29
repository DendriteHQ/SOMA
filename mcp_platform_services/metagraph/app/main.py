"""Metagraph service entry point."""
from app.bootstrap import create_app


app = create_app("metagraph")
