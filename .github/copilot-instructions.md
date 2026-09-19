# PyKrx Copilot Instructions

## Project Overview
PyKrx is a Python library for scraping financial data from Korean exchange markets (KRX, Naver). It provides clean APIs for stock, bond, ETF, and futures data with pandas DataFrame outputs.

## Architecture

### Three-Layer Structure
1. **Public API Layer** (`pykrx/stock/stock_api.py`, `pykrx/bond/bond.py`)
   - User-facing functions like `get_market_ohlcv_by_date()`, `get_market_ticker_list()`
   - Handle date formatting, parameter validation via decorators
   - Use `@market_valid_check` decorator for market parameter validation

2. **Wrap Layer** (`pykrx/website/krx/*/wrap.py`)
   - Transform raw data into clean DataFrames
   - Convert Korean column names to standardized format
   - Apply data type conversions and cleaning

3. **Core Layer** (`pykrx/website/krx/*/core.py`)
   - Each class inherits from `KrxWebIo` or `KrxFutureIo`
   - Implement `bld` property (BLD path for KRX API endpoint)
   - Implement `fetch()` method for data retrieval
   - Use descriptive Korean class names (e.g., `개별종목시세`, `전종목등락률`)

### Key Base Classes
- **`KrxWebIo`** (`pykrx/website/krx/krxio.py`): POST requests to KRX, automatic pagination for >730 day ranges
- **`KrxFutureIo`**: GET requests for future/resource bundle data
- **`Get`/`Post`** (`pykrx/website/comm/webio.py`): Base HTTP clients with standard headers

## Development Workflow

### Setup & Tools
```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Code formatting (auto-fix)
ruff check --fix .
ruff format .

# Run tests
pytest -v                    # All tests
pytest tests/test_*.py -v    # Specific test file
pytest -m "not slow"         # Skip slow tests
```

### Pre-commit Hooks
- Ruff linting/formatting enforced on commit
- Configure via `pyproject.toml` [tool.ruff] section

### Testing Patterns
- Tests in `tests/test_*_api.py` match API modules
- Use real dates in tests (e.g., "20210118") - data may change over time
- Compare actual data patterns (e.g., `temp.sum() == 5`) rather than exact values
- All test methods start with `test_`

## Code Conventions

### API Design
```python
# Standard function signature pattern
def get_market_ohlcv_by_date(
    fromdate: str, todate: str, ticker: str, adjusted: bool = True
) -> DataFrame:
    """
    Args:
        fromdate (str): YYYYMMDD format
        todate   (str): YYYYMMDD format
        ticker   (str): Stock ticker (6-digit code)
        adjusted (bool): Return adjusted price

    Returns:
        DataFrame: Index as datetime, Korean column names
    """
```

### Core Class Pattern
```python
class 개별종목시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01701"

    def fetch(self, strtDd: str, endDd: str, isuCd: str, adjStkPrc: int) -> DataFrame:
        # KrxWebIo.read() handles POST with bld parameter injection
        result = self.read(strtDd=strtDd, endDd=endDd, isuCd=isuCd, adjStkPrc=adjStkPrc)
        return DataFrame(result["output"])
```

### Ticker Handling
- Tickers are 6-digit strings (e.g., "005930" for Samsung)
- Convert to ISIN codes via `get_stock_ticker_isin()` before KRX API calls
- Market codes: "KOSPI", "KOSDAQ", "KONEX", "ALL"

### Date Handling
- Input format: "YYYYMMDD" strings
- Output: pandas DatetimeIndex
- Use `get_nearest_business_day_in_a_week()` when date is None

### Deprecation
- Mark old APIs with `@deprecated` decorator from `deprecated` library
- Include version and reason parameters

## Common Tasks

### Adding New Data Source
1. Create core class in `pykrx/website/krx/{domain}/core.py` inheriting `KrxWebIo`
2. Implement `bld` property (find from KRX website network requests)
3. Implement `fetch()` with proper parameters
4. Add wrapper function in `wrap.py` for data transformation
5. Expose public API in `pykrx/stock/stock_api.py`
6. Add tests in `tests/test_*_api.py`

### Working with KRX API
- Inspect network requests at http://data.krx.co.kr for BLD paths
- POST data to `http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd`
- Response format: `{'output': [...], 'block1': [...]}` - check both keys
- Rate limit: 1 request per second for >730 day ranges (handled automatically)

### Debugging Web Scraping
- Headers include Referer and User-Agent - don't remove
- Check `result['output']` and `result['block1']` for response structure
- KRX may return Korean error messages in JSON

## Dependencies
- **pandas**: Primary data structure for all outputs
- **requests**: HTTP client for web scraping
- **multipledispatch**: Function overloading in stock_api.py
- **deprecated**: API deprecation warnings
- **matplotlib** + font handling: Korean text rendering (NanumBarunGothic.ttf)

## File Organization
- `pykrx/stock/`, `pykrx/bond/`: Domain-specific public APIs
- `pykrx/website/krx/{market,etx,bond,items,future}/`: KRX data sources
- `pykrx/website/naver/`: Naver Finance data source
- `tests/`: Unit tests organized by API domain

## Directory Responsibilities
- `pykrx/stock`, `pykrx/bond`: Public API layer — contains user-facing functions, handles parameter validation (including decorators), date formatting, and other common pre-processing. These modules call the `wrap` layer and expose results as `pandas.DataFrame` to consumers.
- `pykrx/website/comm`: Network/HTTP common layer — provides `Get`/`Post` clients and is responsible for common headers, `requests.Session` reuse, timeout and retry policies, and exception handling for HTTP interactions.
- `pykrx/website/krx/*/core.py`: Data-source network layer — `core.py` modules communicate directly with KRX APIs. Each core class typically inherits from `KrxWebIo` or `KrxFutureIo`, defines the `bld` endpoint identifier, and performs POST/GET requests returning raw JSON/dict responses.
- `pykrx/website/krx/*/wrap.py`: Data cleaning and interface layer — transforms raw responses from `core` into `pandas.DataFrame`, normalizes column names, casts types, sets indices, handles missing values, and provides a clean, stable interface for library consumers.
- `pykrx/website/naver`: Naver Finance-specific scraping and parsing logic — contains HTML parsing, selector management, and adaptation code for site-structure changes.
- `pykrx/website/path_bld_information.json`: BLD endpoint mapping registry — centralizes endpoint identifiers and descriptions so `core` classes can reference them instead of hardcoding values.
- `tests/`: Unit and integration tests — tests should mock network calls to ensure deterministic, network-independent test runs.
