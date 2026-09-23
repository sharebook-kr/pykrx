"""Unit tests for :func:`pykrx.website.comm.util.dataframe_empty_handler`.

These are pure unit tests (no network / no VCR cassette). They pin the
behaviour that KRX empty responses -- which surface as exceptions such as
``ValueError: Length mismatch`` while assigning column names to an empty
frame -- are handled gracefully by returning an empty ``DataFrame`` and
emitting a suppressible warning, rather than crashing or printing to stdout.

See issues #150 and #294.
"""

import json
import logging

import pandas as pd

from pykrx.website.comm.util import dataframe_empty_handler


def test_returns_empty_dataframe_on_column_length_mismatch(caplog):
    @dataframe_empty_handler
    def fetch_with_empty_response():
        # Reproduces the #294 scenario: KRX returned nothing, so assigning
        # the expected column names to the empty frame raises a ValueError
        # ("Length mismatch: Expected axis has 0 elements ...").
        df = pd.DataFrame()
        df.columns = ["a", "b", "c"]
        return df

    with caplog.at_level(logging.WARNING):
        result = fetch_with_empty_response()

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    # A suppressible warning is emitted (instead of printing to stdout).
    assert any("returned no data" in record.getMessage() for record in caplog.records)


def test_passes_through_non_empty_results():
    @dataframe_empty_handler
    def fetch_ok():
        return pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    result = fetch_ok()

    assert list(result["a"]) == [1, 2]
    assert list(result["b"]) == [3, 4]


def test_handles_keyerror_and_jsondecodeerror():
    @dataframe_empty_handler
    def raises_keyerror():
        raise KeyError("missing")

    @dataframe_empty_handler
    def raises_jsondecodeerror():
        raise json.JSONDecodeError("bad", "", 0)

    assert raises_keyerror().empty
    assert raises_jsondecodeerror().empty
