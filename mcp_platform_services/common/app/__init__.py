"""Shared namespace used by the independently deployable platform services."""
from pkgutil import extend_path


__path__ = extend_path(__path__, __name__)
