#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Start Streamlit dashboard
subprocess.run([
    sys.executable, "-m", "streamlit", "run", 
    str(project_root / "dashboard" / "main_dashboard.py"),
    "--server.port", "8501",
    "--server.address", "localhost"
])
