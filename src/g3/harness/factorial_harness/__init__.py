"""Staged N0 package overlay; resolve unchanged modules from the pinned R7.4 parent."""
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
