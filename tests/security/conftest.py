import os
import sys
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

HEALTH_URLS = (
    "http://localhost:6100/health",
    "http://localhost:6200/health",
    "http://localhost:7500/health",
    "http://localhost:7002/health",
    "http://localhost:7001/health",
    "http://localhost:8080/health",
)


@pytest.fixture(scope="session")
def stack():
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("set RUN_INTEGRATION=1 with the stack running")
    for url in HEALTH_URLS:
        with urllib.request.urlopen(url, timeout=3) as response:
            assert response.status == 200, url
    import security_scenarios
    return security_scenarios
