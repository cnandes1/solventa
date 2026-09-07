from voting import Vote, decide_votes


def test_outlier_does_not_contaminate_consensus():
    result = decide_votes([Vote("A", 40), Vote("B", 40), Vote("C", 90)], tolerance=2)
    assert result["decision"] == "CONSENSUS"
    assert result["finalScore"] == 40
    assert result["outlierStrategies"] == ["C"]
    assert result["discrepancyDetected"] is True


def test_two_votes_can_decide_after_third_is_missing():
    result = decide_votes([Vote("A", 40), Vote("B", 40)], tolerance=2)
    assert result["decision"] == "CONSENSUS"
    assert result["finalScore"] == 40
    assert result["missingStrategies"] == ["C"]
    assert result["voteIncomplete"] is True


def test_different_correlations_are_not_part_of_consensus_state():
    result = decide_votes([Vote("A", 10), Vote("B", 50), Vote("C", 90)], tolerance=2)
    assert result["decision"] == "NO_CONSENSUS"
    assert result["finalScore"] is None
