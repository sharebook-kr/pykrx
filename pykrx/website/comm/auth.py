import os
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
LOGIN_JSP = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
LOGIN_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Global session reference (set by webio.py)
_auth_session: Optional["KRXSession"] = None


@dataclass
class KRXSession:
    """KRX 인증 세션 관리 클래스

    JSESSIONID 쿠키를 저장하고 만료 시간을 추적하여
    자동 재로그인 및 헤더 추가를 관리합니다.
    """

    session: requests.Session = field(default_factory=requests.Session)
    login_time: float = field(default_factory=time.time)
    expiry_time: float = field(
        default_factory=lambda: time.time() + 3600
    )  # 1 시간 만료
    is_authenticated: bool = False
    cookies: dict = field(default_factory=dict)

    def is_valid(self, buffer_seconds: int = 300) -> bool:
        """세션이 유효한지 확인 (버퍼 시간 포함)"""
        return self.is_authenticated and time.time() < (
            self.expiry_time - buffer_seconds
        )

    def refresh(self, login_id: str, login_pw: str) -> bool:
        """세션 갱신 (재로그인)"""
        try:
            self.session.close()
        except Exception as e:
            print(f"Session close error: {e}")
            pass

        self.session = requests.Session()
        warmup_krx_session(self.session)

        success = login_krx(login_id, login_pw, self.session)

        if success:
            self.login_time = time.time()
            self.expiry_time = time.time() + 3600  # 1 시간 만료
            self.is_authenticated = True

            # 쿠키 추출 및 저장
            for cookie in self.session.cookies:
                self.cookies[cookie.name] = {
                    "value": cookie.value,
                    "domain": cookie.domain,
                    "path": cookie.path,
                    "secure": cookie.secure,
                    "expires": cookie.expires or 0,
                }

        return success

    def get_headers(self) -> dict:
        """현재 세션에 적합한 헤더 반환"""
        return {
            "User-Agent": USER_AGENT,
            "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
            "X-Requested-With": "XMLHttpRequest",
            "Cookie": "; ".join(
                [f"{name}={info['value']}" for name, info in self.cookies.items()]
            )
            if self.cookies
            else "",
        }

    def get(self, url: str, headers: dict = None, params: dict = None, **kwargs):
        """GET 요청 전송"""
        if headers is None:
            headers = self.get_headers()
        else:
            # 기본 헤더와 병합
            default_headers = self.get_headers()
            default_headers.update(headers)
            headers = default_headers

        return self.session.get(url, headers=headers, params=params, **kwargs)

    def post(self, url: str, headers: dict = None, data: dict = None, **kwargs):
        """POST 요청 전송"""
        if headers is None:
            headers = self.get_headers()
        else:
            # 기본 헤더와 병합
            default_headers = self.get_headers()
            default_headers.update(headers)
            headers = default_headers

        return self.session.post(url, headers=headers, data=data, **kwargs)


def set_auth_session(session: KRXSession | None) -> None:
    """Set the global auth session (called by webio.py)."""
    global _auth_session
    _auth_session = session


def warmup_krx_session(session: requests.Session) -> None:
    session.get(LOGIN_PAGE, headers={"User-Agent": USER_AGENT}, timeout=15)
    session.get(
        LOGIN_JSP,
        headers={"User-Agent": USER_AGENT, "Referer": LOGIN_PAGE},
        timeout=15,
    )


def login_krx(
    login_id: str, login_pw: str, session: requests.Session | None = None
) -> bool:
    """
    KRX <http://data.krx.co.kr|data.krx.co.kr> 로그인 후 세션 쿠키(JSESSIONID) 를 갱신합니다.

    로그인 흐름:
      1. GET MDCCOMS001.cmd  → 초기 JSESSIONID 발급
      2. GET login.jsp       → iframe 세션 초기화
      3. POST MDCCOMS001D1.cmd → 실제 로그인
      4. CD011(중복 로그인) → skipDup=Y 추가 후 재전송
    """
    if session is None:
        session = requests.Session()

    warmup_krx_session(session)

    payload = {
        "mbrNm": "",
        "telNo": "",
        "di": "",
        "certType": "",
        "mbrId": login_id,
        "pw": login_pw,
    }
    headers = {"User-Agent": USER_AGENT, "Referer": LOGIN_PAGE}

    resp = session.post(LOGIN_URL, data=payload, headers=headers, timeout=15)
    data = resp.json()
    error_code = data.get("_error_code", "")
    error_message = data.get("_error_message", "")

    # CD010: 패스워드 변경 필요
    if error_code == "CD010":
        print("⚠️ KRX 비밀번호 변경이 필요합니다.")
        print(f"   오류 메시지: {error_message}")
        print("   https://www.krx.co.kr 에서 비밀번호를 변경한 후 다시 시도하세요.")
        return False

    # CD011: 중복 로그인 (skipDup 처리)
    if error_code == "CD011":
        payload["skipDup"] = "Y"
        resp = session.post(LOGIN_URL, data=payload, headers=headers, timeout=15)
        data = resp.json()
        error_code = data.get("_error_code", "")
        error_message = data.get("_error_message", "")

    return error_code == "CD001"  # CD001 = 정상


def build_krx_session(
    login_id: str = os.getenv("KRX_ID"), login_pw: str = os.getenv("KRX_PW")
) -> KRXSession | None:
    """
    KRX 로그인 세션을 생성하고 반환합니다.

    환경 변수 KRX_ID, KRX_PW 가 설정되어 있으면 자동으로 로그인합니다.
    로그인 성공 시 KRXSession 객체를 반환하며, 실패 시 None 을 반환합니다.
    """
    if not (login_id and login_pw):
        print("KRX 로그인 실패: KRX_ID 또는 KRX_PW 환경 변수가 설정되지 않았습니다.")
        return None

    print("KRX 로그인 시도...")
    print(f"  로그인 ID: {login_id}")

    krxs = KRXSession()
    success = krxs.refresh(login_id, login_pw)

    if success:
        print("KRX 로그인 완료.")
        print(
            f"  로그인 시간: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(krxs.login_time))}"
        )
        print(
            f"  만료 시간: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(krxs.expiry_time))}"
        )
        return krxs
    else:
        print("KRX 로그인 실패: 자격 증명을 확인하세요.")
        return None


def get_auth_session() -> KRXSession | None:
    """
    현재 활성화된 KRX 세션을 반환합니다.

    환경 변수 KRX_ID, KRX_PW 가 설정되어 있지 않으면 None 을 반환합니다.
    세션이 만료되었을 경우 자동으로 재로그인을 시도합니다.
    """
    global _auth_session

    if _auth_session is None:
        # 환경 변수에서 다시 시도
        login_id = os.getenv("KRX_ID")
        login_pw = os.getenv("KRX_PW")
        if login_id and login_pw:
            _auth_session = build_krx_session(login_id, login_pw)
        return _auth_session

    # 세션 만료 확인 및 재로그인
    if not _auth_session.is_valid():
        login_id = os.getenv("KRX_ID")
        login_pw = os.getenv("KRX_PW")
        if login_id and login_pw:
            print("KRX 세션 만료, 재로그인 시도...")
            if _auth_session.refresh(login_id, login_pw):
                print("KRX 세션 갱신 완료.")
            else:
                print("KRX 세션 갱신 실패.")
                return None
        else:
            return None

    return _auth_session
