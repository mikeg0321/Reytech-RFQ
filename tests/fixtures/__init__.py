# Test fixtures package
import os

FIXTURES_DIR = os.path.dirname(__file__)

def fixture_path(filename):
    """Return absolute path to a fixture file."""
    return os.path.join(FIXTURES_DIR, filename)
