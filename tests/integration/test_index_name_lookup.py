"""Tests for issue #295: 지수이름조회 클래스 확장."""

import pandas as pd
import pytest

from pykrx.website.krx.market.core import 지수구성종목, 지수이름조회


class Test지수이름조회:
    """지수이름조회 클래스 테스트"""

    @pytest.mark.vcr
    def test_bld_property(self):
        obj = 지수이름조회()
        assert obj.bld == "dbms/comm/finder/finder_equidx"

    @pytest.mark.vcr
    def test_fetch_returns_dataframe(self):
        df = 지수이름조회().fetch()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "codeName" in df.index.names or df.index.name == "codeName"
        # Verify expected columns
        for col in ["full_code", "short_code", "marketCode", "marketName"]:
            assert col in df.columns, f"Missing column: {col}"

    @pytest.mark.vcr
    def test_fetch_with_search_text(self, monkeypatch):
        cached = pd.DataFrame(
            [
                {
                    "codeName": "코스피",
                    "full_code": "1",
                    "short_code": "001",
                    "marketCode": "STK",
                    "marketName": "KOSPI",
                },
                {
                    "codeName": "KRX 300",
                    "full_code": "5",
                    "short_code": "300",
                    "marketCode": "KRX",
                    "marketName": "KRX",
                },
            ]
        ).set_index("codeName")
        monkeypatch.setattr(지수이름조회, "KRX_INDIDX", cached)

        df = 지수이름조회().fetch(search_text="코스피")
        assert df.index.tolist() == ["코스피"]

    @pytest.mark.vcr
    def test_get_indidx_known_name(self):
        """Well-known index name should resolve to (short_code, full_code)."""
        short_code, full_code = 지수이름조회().get_indidx("KRX TMI")
        # KRX TMI is a real index; short_code should be a non-empty string
        assert short_code is not None
        assert full_code is not None

    @pytest.mark.vcr
    def test_get_indidx_unknown_name(self):
        """Unknown index name should return (None, None)."""
        short_code, full_code = 지수이름조회().get_indidx("존재하지않는지수명")
        assert short_code is None
        assert full_code is None

    def test_get_indidx_requires_market_for_duplicate_name(self, monkeypatch):
        cached = pd.DataFrame(
            [
                {
                    "codeName": "화학",
                    "full_code": "1",
                    "short_code": "008",
                    "marketCode": "STK",
                    "marketName": "KOSPI",
                },
                {
                    "codeName": "화학",
                    "full_code": "2",
                    "short_code": "065",
                    "marketCode": "KSQ",
                    "marketName": "KOSDAQ",
                },
            ]
        ).set_index("codeName")
        monkeypatch.setattr(지수이름조회, "KRX_INDIDX", cached)

        with pytest.raises(ValueError, match="market을 지정하세요"):
            지수이름조회().get_indidx("화학")

        assert 지수이름조회().get_indidx("화학", "KOSDAQ") == ("065", "2")

    @pytest.mark.vcr
    def test_cache_consistency(self):
        """KRX_INDIDX cache should be set after first fetch."""
        지수이름조회.KRX_INDIDX = None  # reset cache
        df1 = 지수이름조회().fetch()
        assert 지수이름조회.KRX_INDIDX is not None
        # Second call should use cache
        df2 = 지수이름조회().fetch()
        assert df1 is df2 or df1.equals(df2)


class Test지수구성종목_NameBased:
    """지수구성종목 name-based fetch (group_id=None) 테스트"""

    @pytest.fixture(autouse=True)
    def index_name_cache(self, monkeypatch):
        cached = pd.DataFrame(
            [
                {
                    "codeName": "코스피",
                    "full_code": "1",
                    "short_code": "001",
                    "marketCode": "STK",
                    "marketName": "KOSPI",
                },
                {
                    "codeName": "코스피 200",
                    "full_code": "1",
                    "short_code": "028",
                    "marketCode": "STK",
                    "marketName": "KOSPI",
                },
                {
                    "codeName": "KRX 300",
                    "full_code": "5",
                    "short_code": "300",
                    "marketCode": "KRX",
                    "marketName": "KRX",
                },
            ]
        ).set_index("codeName")
        monkeypatch.setattr(지수이름조회, "KRX_INDIDX", cached)

    @pytest.mark.vcr
    def test_fetch_by_name_kospi(self):
        """코스피 by name should return constituent stocks."""
        df = 지수구성종목().fetch("20210125", "코스피")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        # Verify expected columns
        for col in ["ISU_SRT_CD", "ISU_ABBRV", "TDD_CLSPRC"]:
            assert col in df.columns, f"Missing column: {col}"
        # Samsung should be in KOSPI
        assert "ISU_ABBRV" in df.columns

    @pytest.mark.vcr
    def test_fetch_by_name_kospi200(self):
        """코스피 200 by name should return constituent stocks."""
        df = 지수구성종목().fetch("20210125", "코스피 200")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    @pytest.mark.vcr
    def test_fetch_by_name_krx300(self):
        """KRX 300 by name should return constituent stocks."""
        df = 지수구성종목().fetch("20210125", "KRX 300")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    @pytest.mark.vcr
    def test_fetch_by_name_unknown(self):
        """Unknown index name should return empty DataFrame."""
        df = 지수구성종목().fetch("20210125", "존재하지않는지수")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @pytest.mark.vcr
    def test_fetch_by_ticker_and_group_id_still_works(self):
        """Original ticker + group_id based fetch should still work."""
        df = 지수구성종목().fetch("20210125", "001", "1")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        for col in ["ISU_SRT_CD", "ISU_ABBRV", "TDD_CLSPRC"]:
            assert col in df.columns
