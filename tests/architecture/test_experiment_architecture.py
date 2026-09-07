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
