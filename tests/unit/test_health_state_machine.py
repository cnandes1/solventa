from state import InstanceState


def make_state(now):
    return InstanceState("quoting-b", "http://b", 2, 2, 1, 2, clock=lambda: now[0])


def test_required_transition_is_down_shadow_active():
    now = [0.0]
    state = make_state(now)
    assert state.observe_health(False) is None
    assert state.observe_health(False) == ("ACTIVE", "DOWN")
    assert state.state == "DOWN"

    now[0] = 1
    assert state.observe_health(True) == ("DOWN", "SHADOW")
    assert state.state == "SHADOW"

    now[0] = 2
    state.observe_health(True)
    state.observe_shadow_validation(True)
    assert state.state == "SHADOW"

    now[0] = 3.1
    assert state.observe_health(True) == ("SHADOW", "ACTIVE")


def test_shadow_mismatch_prevents_promotion():
    now = [0.0]
    state = make_state(now)
    state.observe_health(False)
    state.observe_health(False)
    state.observe_health(True)
    now[0] = 3
    state.observe_health(True)
    state.observe_shadow_validation(False)
    assert state.state == "SHADOW"
