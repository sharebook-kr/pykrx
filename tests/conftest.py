import urllib.parse
from collections.abc import Iterable

import pytest
import vcr

IGNORED_DATE_KEYS = {
    "strtDd",
    "endDd",
    "trdDd",
    "fromdate",
    "todate",
    "startDt",
    "endDt",
    "stDt",
    "enDt",
    "date",
    "count",  # Naver fchart parameter that drifts with current date
}


def _filter_pairs(pairs: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    """Remove volatile date parameters so VCR matches stable requests."""

    return sorted((k, v) for k, v in pairs if k not in IGNORED_DATE_KEYS)


def uri_without_dates(r1, r2):
    """Compare URIs ignoring date-like query params (prevents re-record)."""

    def _normalize(uri: str):
        parsed = urllib.parse.urlsplit(uri)
        filtered = _filter_pairs(
            urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        )
        rebuilt_query = urllib.parse.urlencode(filtered)
        return parsed.scheme, parsed.netloc, parsed.path, rebuilt_query

    norm1 = _normalize(r1.uri)
    norm2 = _normalize(r2.uri)
    result = norm1 == norm2

    # Debug logging
    if not result:
        import sys

        print("\n[VCR URI Matcher Debug]", file=sys.stderr)
        print(f"  Request URI: {r1.uri}", file=sys.stderr)
        print(f"  Cassette URI: {r2.uri}", file=sys.stderr)
        print(f"  Normalized Request: {norm1}", file=sys.stderr)
        print(f"  Normalized Cassette: {norm2}", file=sys.stderr)
        print(f"  Match: {result}\n", file=sys.stderr)

    return result


def form_body_matcher(r1, r2):
    """Order-insensitive form body matcher for application/x-www-form-urlencoded.

    - Parses body into key/value pairs so parameter order differences do not cause mismatches.
    - Falls back to direct equality when parsing fails (e.g., non-form bodies).
    """

    def _normalize(body):
        if body is None:
            return None
        if isinstance(body, bytes):
            body = body.decode()
        parsed = urllib.parse.parse_qsl(body, keep_blank_values=True)
        return set(_filter_pairs(parsed))

    b1 = _normalize(r1.body)
    b2 = _normalize(r2.body)

    # If both bodies are None or parsing failed, compare raw bodies
    if b1 is None or b2 is None:
        return r1.body == r2.body

    # Allow either body to be a superset after volatile keys are stripped
    return b1 == b2 or b1.issuperset(b2) or b2.issuperset(b1)


def before_record_request(request):
    """Scrub volatile query params from requests before recording."""
    parsed = urllib.parse.urlsplit(request.uri)
    filtered = _filter_pairs(urllib.parse.parse_qsl(parsed.query))
    clean_query = urllib.parse.urlencode(filtered)
    request.uri = urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, clean_query, parsed.fragment)
    )
    return request


custom_vcr = vcr.VCR(
    cassette_library_dir="tests/cassettes",
    record_mode="once",
    # Use custom matcher names that ignore volatile params
    match_on=["uri_ignore_dates", "method", "body_ignore_dates"],
    before_record_request=before_record_request,
)
# Register custom matchers with unique names
custom_vcr.register_matcher("uri_ignore_dates", uri_without_dates)
custom_vcr.register_matcher("body_ignore_dates", form_body_matcher)


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "cassette_library_dir": "tests/cassettes",
        "record_mode": "once",
        "match_on": ["uri_ignore_dates", "method", "body_ignore_dates"],
    }


@pytest.fixture
def use_cassette(vcr_config, request):
    """
    Pytest fixture that conditionally wraps a test in a vcrpy cassette.
    If the requesting test is marked with @pytest.mark.cassette('<name>'),
    this fixture creates a vcr.VCR(**vcr_config) and uses its use_cassette
    context manager with the provided cassette name for the duration of the
    test. If no such marker is present, the fixture yields without applying
    any cassette.
    Parameters
    ----------
    vcr_config : dict
        Keyword arguments forwarded to vcr.VCR(...) when creating the VCR
        instance (e.g., serializer, cassette_library_dir, record_mode).
    request : _pytest.fixtures.FixtureRequest
        Pytest request object used to inspect the test for a 'cassette' marker.
    Yields
    ------
    None
        Control is yielded to the test body; teardown occurs after the test
        completes and after the cassette context (if any) exits.
    Notes
    -----
    The cassette name is taken from the first positional argument of the
    'cassette' marker. Any exceptions raised by the test propagate normally.
    """
    """자동으로 cassette을 적용하는 fixture"""
    marker = request.node.get_closest_marker("cassette")
    if not marker:
        yield
        return

    cassette_name = marker.args[0]
    vcr_instance = vcr.VCR(**vcr_config)
    with vcr_instance.use_cassette(cassette_name):
        yield
