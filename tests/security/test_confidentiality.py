import pytest

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("scenario", [f"SEC-C{i}" for i in range(10)])
def test_scenario(stack, scenario):
    result = stack.execute(scenario)
    assert result["accepted"], result
