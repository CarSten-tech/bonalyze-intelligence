from typing import Dict, Optional


def evaluate_run_failure_reason(
    stats: Dict[str, int | float],
    dry_run: bool,
    allow_partial_success: bool,
    max_failure_rate: float,
) -> Optional[str]:
    if dry_run:
        return None

    fetched = int(stats.get("fetched", 0))
    upserted = int(stats.get("inserted", 0))
    embedded = int(stats.get("embedded", 0))
    store_errors = int(stats.get("store_errors", 0))
    failure_rate = float(stats.get("failure_rate", 0.0))

    if fetched == 0:
        return "No offers fetched. Treating run as failed."
    if embedded == 0:
        return "No embeddings were generated. Treating run as failed."
    if upserted == 0:
        return "Offers were fetched but upserted=0. Treating run as failed."
    if store_errors > 0 and not allow_partial_success:
        return f"{store_errors} retailer(s) failed sync/prune."
    if failure_rate > max_failure_rate:
        return (
            f"Failure rate too high ({failure_rate:.2%}) "
            f"above allowed threshold ({max_failure_rate:.2%})."
        )
    return None
