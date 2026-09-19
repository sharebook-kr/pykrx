**기여자 안내**

이 문서는 로컬 개발자 및 기여자를 위한 최소한의 안내입니다. 변경을 제출하기 전에 아래 지침을 확인하세요.

1) 지원하는 Python

- 프로젝트 최소 요구: `Python >= 3.10` (pyproject.toml 기준)
- 권장 개발 버전: `3.11`

2) 로컬 개발 환경 설정 (권장)

```bash
python3.10 -m venv .venv
source .venv/bin/activate  # macOS / Linux
# Windows (PowerShell): .venv\Scripts\Activate.ps1
pip install -e .[dev]
pre-commit install
```

3) 테스트 실행

**테스트 구조**
- `tests/integration/`: VCR cassette를 사용한 통합 테스트 (기본)
- `tests/live/`: 실제 서버 interface 점검 테스트 (CI에서 제외)
- `tests/cassettes/`: HTTP 요청/응답이 녹화된 YAML 파일들

**기본 테스트 실행**
```bash
pytest -v                           # integration 테스트 실행
pytest tests/integration/ -v        # 명시적으로 integration만 실행
pytest tests/live/ -v               # live 테스트 (실제 네트워크 호출)
```

**VCR Cassette 사용법**

Integration 테스트는 `vcrpy`를 사용하여 HTTP 요청을 녹화/재생합니다:

```python
import pytest
from pykrx import stock


class TestStockApi:
    @pytest.mark.cassette("stock/ohlcv_20210104.yaml")
    def test_get_ohlcv(self, use_cassette):
        df = stock.get_market_ohlcv_by_date("20210104", "20210108", "005930")
        assert len(df) > 0
```

- 첫 실행: 실제 HTTP 요청 발생 → `tests/cassettes/`에 녹화
- 이후 실행: 녹화된 cassette 재생 (네트워크 불필요)
- Cassette 재녹화: 해당 YAML 파일을 삭제하고 다시 테스트 실행

**Cassette 파일은 Git에 커밋되어야 합니다** (CI에서 네트워크 없이 실행하기 위해)

4) 포맷팅 및 린트

- 이 프로젝트는 `ruff`를 린트·포맷 도구로 사용합니다. 기본 명령:

```bash
ruff check --config pyproject.toml .            # 린트 검사
ruff check --fix --config pyproject.toml .     # 자동 수정(가능한 규칙)
ruff format --config pyproject.toml .          # 포맷 적용
```

5) CI 매트릭스

- CI는 Python 3.10 ~ 3.14 범위에서 린트/테스트를 수행합니다 (`.github/workflows/ci.yml` 확인).
- PR을 제출하기 전에 가능하면 로컬에서 `ruff`와 테스트를 실행해 주세요. CI가 자동으로 실행되지만, 로컬 확인은 빠른 피드백과 불필요한 CI 실행을 줄이는 데 도움이 됩니다.

6) 의존성 충돌 주의

- 일부 외부 패키지(예: `tts 0.22.0`)는 오래된 `pandas`를 요구할 수 있어 프로젝트의 의존성과 충돌합니다.
- 그러한 패키지를 실험하려면 별도의 가상환경을 사용하세요. 프로젝트의 `pyproject.toml`을 임의로 낮추지 마세요 — 변경 시에는 CI 통과를 전제로 PR로 제안해야 합니다.

7) Matplotlib 관련

- `pykrx/__init__.py`에서 폰트 설정을 수행합니다. 로컬에서 GUI/폰트 관련 문제가 있으면 비대화형 백엔드(`Agg`)를 사용하세요:

```python
import matplotlib

matplotlib.use("Agg")
```

8) 브랜치 및 PR 규칙

- 기능 단위로 토픽 브랜치를 생성하세요 (기본 브랜치는 `develop` 또는 `main`).
- 한 PR에는 하나의 주제(기능/버그픽스)를 포함하세요.
- 변경사항은 설명이 분명한 PR 제목과 본문을 포함해야 합니다. 관련 이슈를 링크해 주세요.
- 버그 수정이나 기능 추가에는 가능한 경우 테스트를 추가하세요.

9) 버전 관리

- 패키지 버전은 `setuptools_scm`으로 관리합니다. 직접 `__version__`을 수동으로 변경하지 마세요.

- 메인 브랜치에 태그를 푸시하면 PyPI에 자동으로 배포됩니다 (태그 형식: `vX.Y.Z`).
	태그를 만들고 푸시하기 전에 변경사항과 버전 번호를 반드시 확인하세요. 실수로 배포를 방지하려면 태그 푸시 전에 리뷰/CI 상태를 확인하거나, 긴급 차단이 필요할 경우 리포지토리 시크릿과 워크플로우 조건을 조정하세요.

10) 보고 및 커뮤니케이션

- 설계 변경, 호환성 문제 등 논의가 필요하면 이슈를 열고 CI 로그를 첨부하세요.

---

## 폴더별 역할
- `pykrx/stock`, `pykrx/bond`: Public API 계층 — 사용자에게 제공되는 함수들을 포함합니다. 입력 파라미터 검증, 날짜 포맷팅, 버전/호환성 체크와 같은 공통 전처리를 수행하고, 내부적으로 `wrap` 계층을 호출하여 `pandas.DataFrame`을 반환합니다.
- `pykrx/website/comm`: 네트워크/HTTP 공통 계층 — `Get`/`Post` 클라이언트를 제공하며, 공통 헤더 설정, `requests.Session` 기반 세션 재사용, 타임아웃, 재시도(Retry) 정책 및 예외 처리를 구현합니다.
- `pykrx/website/krx/*/core.py`: 데이터 소스별 네트워크 요청 책임 — 각 도메인의 `core.py`는 KRX API와 직접 통신합니다. `bld`(엔드포인트 식별자)를 정의하고, 필요한 파라미터로 POST/GET 요청을 수행하여 원시 JSON/사전 응답을 반환합니다.
- `pykrx/website/krx/*/wrap.py`: 데이터 정제 및 인터페이스 책임 — `core`에서 받은 원시 응답을 `pandas.DataFrame`으로 변환하고 컬럼명 통합, 타입 변환, 인덱스 설정, 누락값 처리 등 라이브러리 소비자에게 제공할 깨끗한 인터페이스를 만듭니다.
- `pykrx/website/naver`: Naver Finance 전용 스크레이핑 및 파싱 로직을 포함합니다. HTML 구조 변화에 대응하는 선택자 및 파서 유지보수를 합니다.
- `pykrx/website/path_bld_information.json`: KRX BLD 엔드포인트 매핑 레지스트리로 사용됩니다. 엔드포인트 식별자와 설명을 중앙에서 관리하여 `core` 클래스들이 참조하도록 권장합니다.
- `tests/`: 단위 및 통합 테스트. 네트워크 호출은 모킹하여 테스트의 안정성과 재현성을 확보하세요.

원하시면 이 파일을 `CONTRIBUTING.md`로 이름을 변경하여 GitHub에서 자동으로 노출되도록 변경해 드립니다.
