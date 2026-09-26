from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_each_quoting_replica_has_an_independent_group_and_volume():
    compose = (ROOT / "docker-compose.yaml").read_text()
    assert "MATERIALIZER_GROUP: quoting-a-materializer" in compose
    assert "MATERIALIZER_GROUP: quoting-b-materializer" in compose
    assert "quoting-a-data:/data" in compose
    assert "quoting-b-data:/data" in compose


def test_business_and_control_redis_are_separate():
    compose = (ROOT / "docker-compose.yaml").read_text()
    assert "redis-business:" in compose
    assert "redis-control:" in compose
    assert "appendonly yes" in compose


def test_eda_refresh_route_does_not_call_profiling_http():
    source = (ROOT / "quoting" / "app.py").read_text()
    refresh_source = source.split('def request_refresh(customer_id):', 1)[1].split('@app.get("/sync/quotes', 1)[0]
    assert "requests." not in refresh_source
    assert "STREAM_REFRESH_REQUESTS" in refresh_source


def test_gateway_has_single_controlled_failover():
    source = (ROOT / "gateway" / "app.py").read_text()
    assert 'choose_active({primary_id})' in source
    assert "gateway_failover_success" in source


def test_gateway_authorizes_before_routing():
    source = (ROOT / "gateway" / "app.py").read_text()
    proxy_source = source.split("def proxy(subpath):", 1)[1]
    assert proxy_source.index("security.authorize(") < proxy_source.index("choose_active()")


def test_quoting_verifies_before_applying_events():
    source = (ROOT / "quoting" / "app.py").read_text()
    process_source = source.split("def process_message(", 1)[1].split("def recover_pending(", 1)[0]
    assert process_source.index("verify_event(") < process_source.index("repository.apply_event(")


def test_business_bus_enforces_acl_and_services_authenticate():
    compose = (ROOT / "docker-compose.yaml").read_text()
    acl = (ROOT / "redis" / "users.acl").read_text()
    assert "--aclfile /usr/local/etc/redis/users.acl" in compose
    assert "redis://profile_producer:" in compose and "redis://profile_consumer:" in compose
    assert "user default on nopass resetkeys resetchannels -@all +ping" in acl
    assert "(~profile-updated -@all +xadd)" in acl


def test_test_secrets_are_marked_and_not_in_service_code():
    compose = (ROOT / "docker-compose.yaml").read_text()
    for line in compose.splitlines():
        if any(name in line for name in ("HMAC_KEY:", "IDP_SIGNING_KEY:", "KNOWN_KEYS_JSON:", "IDP_KEYS_JSON:")):
            assert "TEST-ONLY" in line
    for path in ("gateway/app.py", "gateway/security.py", "profiling/app.py", "quoting/app.py",
                 "security/idp/app.py", "security/pdp/app.py"):
        assert "TEST-ONLY" not in (ROOT / path).read_text(), path
