from run_policy import evaluate_run_failure_reason


def test_run_policy_allows_dry_run():
    reason = evaluate_run_failure_reason(
        {"fetched": 0, "inserted": 0, "embedded": 0, "store_errors": 0, "failure_rate": 1.0},
        dry_run=True,
        allow_partial_success=False,
        max_failure_rate=0.2,
    )
    assert reason is None


def test_run_policy_fails_when_nothing_upserted():
    reason = evaluate_run_failure_reason(
        {"fetched": 100, "inserted": 0, "embedded": 100, "store_errors": 0, "failure_rate": 0.1},
        dry_run=False,
        allow_partial_success=False,
        max_failure_rate=0.2,
    )
    assert "upserted=0" in reason


def test_run_policy_fails_on_store_errors_when_partial_not_allowed():
    reason = evaluate_run_failure_reason(
        {"fetched": 100, "inserted": 80, "embedded": 100, "store_errors": 1, "failure_rate": 0.2},
        dry_run=False,
        allow_partial_success=False,
        max_failure_rate=0.5,
    )
    assert "retailer(s) failed" in reason


def test_run_policy_passes_on_healthy_stats():
    reason = evaluate_run_failure_reason(
        {"fetched": 100, "inserted": 92, "embedded": 100, "store_errors": 0, "failure_rate": 0.08},
        dry_run=False,
        allow_partial_success=False,
        max_failure_rate=0.2,
    )
    assert reason is None


def test_run_policy_fails_on_high_failure_rate():
    reason = evaluate_run_failure_reason(
        {"fetched": 100, "inserted": 80, "embedded": 100, "store_errors": 0, "failure_rate": 0.41},
        dry_run=False,
        allow_partial_success=True,
        max_failure_rate=0.35,
    )
    assert "Failure rate too high" in reason
