import pytest

from circuit_breaker import CircuitBreaker, CircuitOpenError


def test_circuit_opens_and_allows_only_one_half_open_probe():
    now = [0.0]
    circuit = CircuitBreaker(2, 5, clock=lambda: now[0])

    circuit.acquire_permission()
    circuit.record_failure()
    circuit.acquire_permission()
    circuit.record_failure()
    assert circuit.snapshot()["state"] == "OPEN"

    with pytest.raises(CircuitOpenError):
        circuit.acquire_permission()

    now[0] = 6.0
    circuit.acquire_permission()
    assert circuit.snapshot()["state"] == "HALF_OPEN"
    with pytest.raises(CircuitOpenError):
        circuit.acquire_permission()

    circuit.record_success()
    assert circuit.snapshot()["state"] == "CLOSED"


def test_reset_returns_experiment_to_closed_state():
    circuit = CircuitBreaker(1, 10)
    circuit.acquire_permission()
    circuit.record_failure()
    assert circuit.snapshot()["state"] == "OPEN"
    circuit.reset()
    assert circuit.snapshot()["state"] == "CLOSED"
    assert circuit.snapshot()["failures"] == 0
