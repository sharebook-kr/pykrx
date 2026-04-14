from abc import abstractmethod

import requests

from pykrx.website.comm.auth import (
    build_krx_session,
    get_auth_session,
    set_auth_session,
)

# Initialize session at module load time
_session = build_krx_session()
# Set the auth session for get_auth_session() to work
set_auth_session(_session)


def set_session(session) -> requests.Session | None:
    """Set the global session (deprecated, use KRXSession)"""
    global _session
    _session = session


def get_session() -> requests.Session | None:
    """Get the current KRX session with automatic refresh if expired."""
    return get_auth_session()


class Get:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
            "X-Requested-With": "XMLHttpRequest",
        }

    def read(self, **params):
        krxs = get_session()

        if krxs is None:
            # 세션이 없으면 새 요청 생성
            session = requests.Session()
            resp = session.get(self.url, headers=self.headers, params=params)
        else:
            # KRXSession 의 헤더 사용 (쿠키 포함)
            headers = krxs.get_headers()
            # 커스텀 헤더 병합
            for key, value in self.headers.items():
                headers[key] = value

            resp = krxs.session.get(self.url, headers=headers, params=params)

        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


class Post:
    def __init__(self, headers=None):
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
            "X-Requested-With": "XMLHttpRequest",
        }
        if headers is not None:
            self.headers.update(headers)

    def read(self, **params):
        krxs = get_session()

        if krxs is None:
            # 세션이 없으면 새 요청 생성
            session = requests.Session()
            resp = session.post(self.url, headers=self.headers, data=params)
        else:
            # KRXSession 의 헤더 사용 (쿠키 포함)
            headers = krxs.get_headers()
            # 커스텀 헤더 병합
            for key, value in self.headers.items():
                headers[key] = value

            resp = krxs.session.post(self.url, headers=headers, data=params)

        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError
