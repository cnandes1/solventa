import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for service in ("profiling", "quoting", "gateway"):
    sys.path.insert(0, str(ROOT / service))
