import requests
from requests.models import Response
from postgrest.exceptions import APIError

from scraper import _is_retryable_request_exception
from data_sync import _is_retryable_sync_exception


def _http_error(status_code: int) -> requests.HTTPError:
    response = Response()
    response.status_code = status_code
    return requests.HTTPError(response=response)


def test_request_retry_policy_retries_transient_http():
    assert _is_retryable_request_exception(_http_error(429)) is True
    assert _is_retryable_request_exception(_http_error(503)) is True


def test_request_retry_policy_skips_permanent_http():
    assert _is_retryable_request_exception(_http_error(400)) is False
    assert _is_retryable_request_exception(_http_error(404)) is False


def test_sync_retry_policy_retries_transient_api_errors():
    err = APIError({"message": "transient", "code": "500", "hint": None, "details": None})
    setattr(err, "status_code", 503)
    assert _is_retryable_sync_exception(err) is True


def test_sync_retry_policy_skips_constraint_errors():
    err = APIError(
        {
            "message": "null value in column violates not-null constraint",
            "code": "23502",
            "hint": None,
            "details": None,
        }
    )
    setattr(err, "status_code", 400)
    assert _is_retryable_sync_exception(err) is False
