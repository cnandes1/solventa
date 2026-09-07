import os
import urllib.request

import pytest


pytestmark = pytest.mark.integration


@pytest.mark.skipif(os.environ.get("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 with the stack running")
def test_stack_health_endpoints():
    for url in (
        "http://localhost:6000/health",
        "http://localhost:7500/health",
        "http://localhost:7002/health",
        "http://localhost:7001/health",
        "http://localhost:8080/health",
    ):
        with urllib.request.urlopen(url, timeout=3) as response:
            assert response.status == 200
