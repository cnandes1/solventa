import importlib.util
import time
from pathlib import Path

import jwt
import pytest

import security

ROOT = Path(__file__).resolve().parents[2]
SECRET = "TEST-ONLY-unit-idp-key-with-enough-length"


def load_service(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pdp = load_service("pdp_app", ROOT / "security" / "pdp" / "app.py")
idp = load_service("idp_app", ROOT / "security" / "idp" / "app.py")


class FakeRequest:
    def __init__(self, token=None, method="GET", header=None):
        self.method = method
        self.headers = {}
        if header is not None:
            self.headers["Authorization"] = header
        elif token is not None:
            self.headers["Authorization"] = f"Bearer {token}"


def make_token(sub="C001", tenant="solventa", scopes=("quotes:read",), exp_delta=3600,
               kid="unit-kid", secret=SECRET, aud="solventa-gateway"):
    now = int(time.time())
    claims = {"iss": "solventa-test-idp", "aud": aud, "sub": sub, "customerId": sub, "tenantId": tenant,
              "scopes": list(scopes), "iat": now, "exp": now + exp_delta}
    return jwt.encode(claims, secret, algorithm="HS256", headers={"kid": kid})


@pytest.fixture(autouse=True)
def configured(monkeypatch):
    monkeypatch.setattr(security, "IDP_KEYS", {"unit-kid": SECRET})
    monkeypatch.setattr(security, "log_event", lambda *args, **kwargs: None)


@pytest.fixture
def local_pdp(monkeypatch):
    calls = []

    def query(subject, resource, action):
        calls.append((subject, resource, action))
        return pdp.decide(subject, resource, action)

    monkeypatch.setattr(security, "query_pdp", query)
    return calls


def test_owner_with_scope_is_permitted(local_pdp):
    decision = security.authorize(FakeRequest(make_token()), "quotes/C001")
    assert (decision.status, decision.reason) == ("PERMIT", "OWNER")
    subject, resource, action = local_pdp[0]
    assert resource["owner"]["customerId"] == "C001" and action == "quotes:read"
    assert "token" not in subject


@pytest.mark.parametrize("request_, code, reason", [
    (FakeRequest(), 401, "MISSING_TOKEN"),
    (FakeRequest(header="Basic abc"), 401, "INVALID_TOKEN"),
    (FakeRequest("not-a-jwt"), 401, "INVALID_TOKEN"),
    (FakeRequest(make_token(exp_delta=-5)), 401, "TOKEN_EXPIRED"),
    (FakeRequest(make_token(secret="TEST-ONLY-another-key-with-enough-length")), 401, "INVALID_SIGNATURE"),
    (FakeRequest(make_token(kid="other-kid")), 401, "UNKNOWN_KEY_ID"),
    (FakeRequest(make_token(aud="other-audience")), 401, "INVALID_AUDIENCE"),
    (FakeRequest(make_token(sub="C002")), 403, "OWNERSHIP_MISMATCH"),
    (FakeRequest(make_token(scopes=("profiles:read",))), 403, "INSUFFICIENT_SCOPE"),
    (FakeRequest(make_token(tenant="tenant-otro")), 403, "TENANT_MISMATCH"),
])
def test_denials(local_pdp, request_, code, reason):
    decision = security.authorize(request_, "quotes/C001")
    assert (decision.status, decision.code, decision.reason) == ("DENY", code, reason)
    assert decision.body["reason"] == reason


def test_tampered_signature_character_is_rejected(local_pdp):
    header, payload, signature = make_token().split(".")
    index = len(signature) // 2
    forged = ".".join((header, payload, signature[:index] + ("A" if signature[index] != "A" else "B")
                       + signature[index + 1:]))
    assert security.authorize(FakeRequest(forged), "quotes/C001").reason == "INVALID_SIGNATURE"


def test_explicit_delegation_is_permitted(local_pdp):
    token = make_token(sub="C002", scopes=("quotes:read", "delegated:C001"))
    decision = security.authorize(FakeRequest(token), "quotes/C001")
    assert (decision.status, decision.reason) == ("PERMIT", "DELEGATION")


def test_delegation_does_not_replace_action_scope(local_pdp):
    token = make_token(sub="C002", scopes=("delegated:C001",))
    assert security.authorize(FakeRequest(token), "quotes/C001").reason == "INSUFFICIENT_SCOPE"


def test_unmapped_routes_and_methods_are_denied_by_default(local_pdp):
    token = make_token(scopes=("quotes:read", "profiles:read", "profiles:refresh"))
    assert security.authorize(FakeRequest(token), "unknown/C001").reason == "UNMAPPED_RESOURCE"
    assert security.authorize(FakeRequest(token, method="DELETE"), "quotes/C001").reason == "UNMAPPED_RESOURCE"
    assert not local_pdp


def test_route_actions():
    assert security.resolve_resource("POST", "quotes/C001/request-refresh")[1] == "profiles:refresh"
    assert security.resolve_resource("GET", "sync/quotes/C001")[1] == "quotes:read"
    assert security.resolve_resource("GET", "materialized-profiles/C003")[0]["owner"]["customerId"] == "C003"


def test_experiment_control_plane_is_out_of_scope(local_pdp):
    assert security.authorize(FakeRequest(), "admin/materializer").status == "PERMIT"


def test_pdp_unreachable_fails_closed(monkeypatch):
    monkeypatch.setattr(security, "PDP_URL", "http://127.0.0.1:9")
    monkeypatch.setattr(security, "PDP_TIMEOUT_MS", 200)
    decision = security.authorize(FakeRequest(make_token()), "quotes/C001")
    assert (decision.status, decision.code, decision.reason) == ("DENY", 503, "PDP_UNAVAILABLE")


@pytest.mark.parametrize("status, body", [(500, {"decision": "PERMIT"}), (200, {"decision": "MAYBE"}),
                                          (200, ["PERMIT"]), (200, None)])
def test_unexpected_pdp_answers_fail_closed(monkeypatch, status, body):
    class Response:
        status_code = status

        def json(self):
            if body is None:
                raise ValueError("not json")
            return body

    monkeypatch.setattr(security._session, "post", lambda *args, **kwargs: Response())
    assert security.query_pdp({}, {}, "quotes:read") == ("DENY", "PDP_UNAVAILABLE")


def test_pdp_rejects_malformed_requests():
    client = pdp.app.test_client()
    assert client.post("/decisions", data="x").get_json()["decision"] == "DENY"
    body = client.post("/decisions", json={"subject": {"customerId": "C001"}}).get_json()
    assert body == {"decision": "DENY", "reason": "MALFORMED_REQUEST"}


def test_idp_tokens_are_accepted_by_the_gateway(monkeypatch, local_pdp):
    monkeypatch.setattr(idp, "IDP_SIGNING_KEY", SECRET)
    monkeypatch.setattr(idp, "IDP_KEY_ID", "unit-kid")
    monkeypatch.setattr(idp, "log_event", lambda *args, **kwargs: None)
    response = idp.app.test_client().post("/tokens", json={"sub": "C001", "scopes": ["quotes:read"]})
    token = response.get_json()["access_token"]
    assert security.authorize(FakeRequest(token), "quotes/C001").status == "PERMIT"
