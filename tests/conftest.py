import urllib.parse
from collections.abc import Iterable
from contextlib import ExitStack
from pathlib import Path

import pytest
import vcr as vcrpy
import yaml

from pykrx.website.comm import webio as _webio

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
    return norm1 == norm2


def form_body_matcher(r1, r2):
    """Order-insensitive form body matcher for application/x-www-form-urlencoded.

    - Parses body into key/value pairs so parameter order differences do not cause mismatches.
    - Falls back to direct equality when parsing fails (e.g., non-form bodies).
    """

    def _normalize(body):
        if body is None:
            return None
        if isinstance(body, dict) and "string" in body:
            body = body["string"]
        if isinstance(body, bytes):
            try:
                body = body.decode("utf-8")
            except UnicodeDecodeError:
                # Fallback for binary bodies that aren't form data
                return body

        # If body is not a string at this point (e.g. binary data that failed decode),
        # return it as is, or empty set if it's empty
        if not isinstance(body, str):
            return body

        try:
            parsed = urllib.parse.parse_qsl(body, keep_blank_values=True)
            return set(_filter_pairs(parsed))
        except Exception:
            return body

    b1 = _normalize(r1.body)
    b2 = _normalize(r2.body)

    # If both bodies are None or parsing failed, compare raw bodies
    if b1 is None or b2 is None:
        return r1.body == r2.body

    # Strict equality check - parameters must match exactly after filtering
    try:
        return b1 == b2
    except Exception:
        return r1.body == r2.body


def before_record_request(request):
    """Scrub volatile query params from requests before recording."""

    # Prevent caching of requests that are already in common cassettes
    # This simulates "read-only" inheritance from common cassettes when using new_episodes mode.
    # If we don't filter these, new_episodes will re-record them into the test-specific cassette.
    if request.body:
        body_str = request.body
        if isinstance(body_str, bytes):
            body_str = body_str.decode("utf-8", "ignore")

        # BLD codes found in common cassettes
        COMMON_BLDS = [
            "finder_stkisu",  # finder_init.yaml
            "finder_listdelisu",  # finder_init.yaml
            "MDCSTAT04601",  # etx_ticker_init.yaml
            "MDCSTAT06701",  # index_kind_init.yaml
            "MDCSTAT08501",  # index_kind_init.yaml
        ]

        for bld in COMMON_BLDS:
            if bld in body_str:
                return None  # Do not record this request

    parsed = urllib.parse.urlsplit(request.uri)
    filtered = _filter_pairs(urllib.parse.parse_qsl(parsed.query))
    clean_query = urllib.parse.urlencode(filtered)
    request.uri = urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, clean_query, parsed.fragment)
    )
    return request


# Get absolute path to cassettes directory
CASSETTE_DIR = str(Path(__file__).parent / "cassettes")
COMMON_CASSETTE_DIR = str(Path(__file__).parent / "cassettes" / "common")

# Register custom matchers globally for ALL VCR instances
# This needs to be done at module level before pytest-vcr creates VCR instances
_global_vcr = vcrpy.VCR()
_global_vcr.register_matcher("uri_ignore_dates", uri_without_dates)
_global_vcr.register_matcher("body_ignore_dates", form_body_matcher)


@pytest.fixture(scope="session", autouse=True)
def init_singletons(tmp_path_factory):
    """세션 시작 시 singleton을 common cassette로 미리 초기화.

    VCR은 중첩 카세트에서 가장 안쪽(innermost) 카세트만 사용하므로
    모든 common cassette를 하나의 YAML 파일로 합쳐서 단일 컨텍스트를 사용한다.
    singleton이 미리 초기화되면 개별 테스트에서 HTTP 요청이 불필요하다.
    """
    # Merge all common cassette interactions into one file
    all_interactions = []
    for fname in ["etx_ticker_init.yaml", "finder_init.yaml", "index_kind_init.yaml"]:
        path = Path(COMMON_CASSETTE_DIR) / fname
        if path.exists():
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
            all_interactions.extend(data.get("interactions", []))

    combined_path = tmp_path_factory.mktemp("vcr") / "combined_init.yaml"
    combined_path.write_text(
        yaml.dump({"interactions": all_interactions, "version": 1}, allow_unicode=True),
        encoding="utf-8",
    )

    _vcr = vcrpy.VCR()
    _vcr.register_matcher("uri_ignore_dates", uri_without_dates)
    _vcr.register_matcher("body_ignore_dates", form_body_matcher)

    # Use plain requests inside VCR context so cassettes (recorded without auth)
    # match correctly on all platforms including Windows.
    original_session = _webio.get_session()
    _webio.set_session(None)
    try:
        with _vcr.use_cassette(
            str(combined_path),
            record_mode="none",
            allow_playback_repeats=True,
            match_on=["uri_ignore_dates", "method", "body_ignore_dates"],
        ):
            from pykrx.website.krx.etx.ticker import EtxTicker
            from pykrx.website.krx.market.ticker import StockTicker

            EtxTicker()
            StockTicker()
    finally:
        _webio.set_session(original_session)


@pytest.fixture(scope="module")
def vcr_cassette_dir():
    """pytest-vcr: cassette directory location"""
    return CASSETTE_DIR


@pytest.fixture(scope="function")
def vcr(vcr):
    """pytest-vcr: Override VCR instance to register custom matchers and load shared cassettes."""
    vcr.register_matcher("uri_ignore_dates", uri_without_dates)
    vcr.register_matcher("body_ignore_dates", form_body_matcher)

    # 공통 ticker 초기화 호출을 재사용해 cassette 크기를 줄인다.
    common_cassettes = [
        Path(COMMON_CASSETTE_DIR) / "etx_ticker_init.yaml",
        Path(COMMON_CASSETTE_DIR) / "finder_init.yaml",
        Path(COMMON_CASSETTE_DIR) / "index_kind_init.yaml",
    ]

    stack = ExitStack()
    try:
        for cassette_path in common_cassettes:
            if cassette_path.exists():
                stack.enter_context(
                    vcr.use_cassette(
                        str(cassette_path),
                        record_mode="none",  # 공통 cassette은 읽기 전용으로 사용
                        allow_playback_repeats=True,  # 중복 요청 허용
                        match_on=["uri_ignore_dates", "method", "body_ignore_dates"],
                    )
                )
        yield vcr
    finally:
        stack.close()


@pytest.fixture(scope="module")
def vcr_config():
    """pytest-vcr: VCR configuration with custom matchers"""
    return {
        "record_mode": "once",  # [User Requirement] External requests must be 0
        "match_on": ["uri_ignore_dates", "method", "body_ignore_dates"],
        "before_record_request": before_record_request,
        "decode_compressed_response": False,  # 응답을 압축된 상태(gzip)로 저장해 파일 크기 감소
    }
