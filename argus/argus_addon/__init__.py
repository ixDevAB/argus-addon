import os
import re
from pathlib import Path


def _version() -> str:
    """The add-on's version: the one Home Assistant offers updates by, in config.yaml.

    The image carries config.yaml beside the package. Where the package runs without it, the
    demo home's container, ARGUS_ADDON_VERSION says which add-on it was built from.
    """
    named = os.environ.get("ARGUS_ADDON_VERSION", "").strip()
    if named:
        return named
    try:
        text = (Path(__file__).resolve().parent.parent / "config.yaml").read_text()
    except OSError:
        return "unknown"
    found = re.search(r'^version:\s*"?([^"\s]+)"?\s*$', text, re.MULTILINE)
    return found.group(1) if found else "unknown"


__version__ = _version()
