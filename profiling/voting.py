from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Vote:
    strategy_id: str
    score: float | None
    status: str = "SUCCESS"
    error: str | None = None


def decide_votes(
    votes: list[Vote],
    expected_strategies: tuple[str, ...] = ("A", "B", "C"),
    tolerance: float = 2.0,
    minimum_consensus: int = 2,
) -> dict:
    """Select the largest cluster whose scores fit within the tolerance.

    Ties are deterministic: lower spread, then lower average, then strategy ids.
    """
    successful = [v for v in votes if v.status == "SUCCESS" and v.score is not None]
    candidates: list[list[Vote]] = []
    for seed in successful:
        cluster = [v for v in successful if abs(float(v.score) - float(seed.score)) <= tolerance]
        unique = {v.strategy_id: v for v in cluster}
        candidates.append(list(unique.values()))

    def rank(cluster: list[Vote]):
        scores = [float(v.score) for v in cluster]
        spread = max(scores) - min(scores) if scores else float("inf")
        average = sum(scores) / len(scores) if scores else float("inf")
        return (-len(cluster), spread, average, tuple(sorted(v.strategy_id for v in cluster)))

    consensus = sorted(candidates, key=rank)[0] if candidates else []
    consensus_ids = {v.strategy_id for v in consensus}
    received_ids = {v.strategy_id for v in votes}
    missing = [sid for sid in expected_strategies if sid not in received_ids]
    outliers = [v.strategy_id for v in successful if v.strategy_id not in consensus_ids]
    errors = [v.strategy_id for v in votes if v.status != "SUCCESS" or v.score is None]
    valid = len(consensus) >= minimum_consensus
    final_score = (
        round(sum(float(v.score) for v in consensus) / len(consensus), 2)
        if valid
        else None
    )
    return {
        "decision": "CONSENSUS" if valid else "NO_CONSENSUS",
        "finalScore": final_score,
        "consensusStrategies": sorted(consensus_ids),
        "outlierStrategies": sorted(outliers),
        "missingStrategies": missing,
        "errorStrategies": sorted(errors),
        "voteIncomplete": bool(missing),
        "discrepancyDetected": bool(outliers),
        "responsesReceived": len(received_ids),
    }
