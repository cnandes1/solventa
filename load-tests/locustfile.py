import json
import os
import random
import urllib.request

from locust import HttpUser, between, task


CUSTOMERS = [item.strip() for item in os.environ.get("CUSTOMER_POOL", "C001,C002,C003").split(",") if item.strip()]
IDP_URL = os.environ.get("IDP_URL", "http://localhost:6100").rstrip("/")


def owner_token(customer_id: str) -> str:
    """Test-only JWT from the experiment IdP; the Gateway enforces AS-4 on /quotes."""
    body = json.dumps({"sub": customer_id, "scopes": ["quotes:read", "profiles:read", "profiles:refresh"]}).encode()
    request = urllib.request.Request(f"{IDP_URL}/tokens", data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read())["access_token"]


class QuotingUser(HttpUser):
    host = os.environ.get("TARGET_HOST", "http://localhost:8080")
    wait_time = between(0.05, 0.2)

    def on_start(self):
        self.headers = {customer_id: {"Authorization": f"Bearer {owner_token(customer_id)}"}
                        for customer_id in CUSTOMERS}

    @task(9)
    def quote_from_local_view(self):
        customer_id = random.choice(CUSTOMERS)
        with self.client.get(f"/quotes/{customer_id}", name="/quotes/[customer]", catch_response=True,
                             headers=self.headers[customer_id]) as response:
            if response.status_code != 200:
                response.failure(f"functional or technical failure: {response.status_code}")

    @task(1)
    def request_profile_refresh(self):
        customer_id = random.choice(CUSTOMERS)
        self.client.post(f"/quotes/{customer_id}/request-refresh", name="/quotes/[customer]/request-refresh",
                         headers=self.headers[customer_id])
