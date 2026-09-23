import json
import logging

from pandas import DataFrame

logger = logging.getLogger(__name__)


def dataframe_empty_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (
            AttributeError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as e:
            # KRX intermittently returns an empty payload (e.g. under request
            # load or rate limiting). Building the DataFrame then fails --
            # most commonly a "Length mismatch" ValueError when column names
            # are assigned to an empty frame. Treat this as an empty result:
            # emit a single, suppressible warning (instead of printing to
            # stdout) and return an empty DataFrame so callers do not crash.
            # See issues #150 and #294.
            logger.warning(
                "%s returned no data (%s: %s); returning an empty DataFrame "
                "[args=%r kwargs=%r]",
                func.__name__,
                type(e).__name__,
                e,
                args,
                kwargs,
            )
            return DataFrame()

    return wrapper


def singleton(class_):
    class class_w(class_):
        _instance = None

        def __new__(class_, *args, **kwargs):
            if class_w._instance is None:
                class_w._instance = super().__new__(class_, *args, **kwargs)
                class_w._instance._sealed = False
            return class_w._instance

        def __init__(self, *args, **kwargs):
            if self._sealed:
                return
            super().__init__(*args, **kwargs)
            self._sealed = True

    class_w.__name__ = class_.__name__
    return class_w
