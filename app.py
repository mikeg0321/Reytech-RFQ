import os
import sys
from pathlib import Path

# Add src/ to the Python path for compatibility
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Import the Flask app from dashboard
from api.dashboard import app as application

if __name__ == "__main__":
    application.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
