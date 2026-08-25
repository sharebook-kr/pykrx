import logging

import pandas as pd

from pykrx.website.comm.util import dataframe_empty_handler


def test_dataframe_empty_handler_logs_without_error(caplog):
    @dataframe_empty_handler
    def raises():
        raise KeyError("missing")

    with caplog.at_level(logging.INFO):
        df = raises()

    assert isinstance(df, pd.DataFrame)
    assert df.empty
    # logging.info(args, kwargs) used to pass args as the format string and
    # kwargs as its single positional arg, so formatting the record raised
    # "TypeError: not all arguments converted during string formatting".
    for record in caplog.records:
        record.getMessage()
