import os
import random

from locust import HttpUser, between, task


CUSTOMERS = [item.strip() for item in os.environ.get("CUSTOMER_POOL", "C001,C002,C003").split(",") if item.strip()]


class QuotingUser(HttpUser):
    host = os.environ.get("TARGET_HOST", "http://localhost:8080")
    wait_time = between(0.05, 0.2)

    @task(9)
    def quote_from_local_view(self):
        customer_id = random.choice(CUSTOMERS)
        with self.client.get(f"/quotes/{customer_id}", name="/quotes/[customer]", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"functional or technical failure: {response.status_code}")

    @task(1)
    def request_profile_refresh(self):
        customer_id = random.choice(CUSTOMERS)
        self.client.post(f"/quotes/{customer_id}/request-refresh", name="/quotes/[customer]/request-refresh")
