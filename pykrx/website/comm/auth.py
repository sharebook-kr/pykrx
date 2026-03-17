import requests


def login_krx(session: requests.Session, login_id: str, login_pw: str) -> bool:
    login_page = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
    login_jsp = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
    login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    session.get(login_page, headers={"User-Agent": user_agent}, timeout=15)
    session.get(login_jsp, headers={"User-Agent": user_agent, "Referer": login_page}, timeout=15)

    payload = {
        "mbrNm": "",
        "telNo": "",
        "di": "",
        "certType": "",
        "mbrId": login_id,
        "pw": login_pw,
    }
    headers = {"User-Agent": user_agent, "Referer": login_page}

    resp = session.post(login_url, data=payload, headers=headers, timeout=15)
    data = resp.json()
    error_code = data.get("_error_code", "")

    if error_code == "CD011":
        payload["skipDup"] = "Y"
        resp = session.post(login_url, data=payload, headers=headers, timeout=15)
        data = resp.json()
        error_code = data.get("_error_code", "")

    return error_code == "CD001"
