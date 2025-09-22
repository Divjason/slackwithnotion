import os, json, time
from datetime import datetime, timezone
from dateutil import parser as dtparser
import requests
from typing import List, Dict, Any

STATE_PATH = "state/notion_state.json"
os.makedirs("state", exist_ok=True)

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

DB_IDS = [s.strip() for s in os.environ.get("NOTION_DATABASE_IDS", "").split(",") if s.strip()]
PAGE_IDS = [s.strip() for s in os.environ.get("NOTION_PAGE_IDS", "").split(",") if s.strip()]

NOTION_BASE = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_PATH):
        return {"last_checked": None, "seen_ids": []}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: Dict[str, Any]):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def slack_post(text: str, blocks: List[Dict[str, Any]] = None):
    payload = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
    resp.raise_for_status()

def query_new_db_pages(db_id: str, after_iso: str = None) -> List[Dict[str, Any]]:
    url = f"{NOTION_BASE}/databases/{db_id}/query"
    payload: Dict[str, Any] = {"page_size": 50, "sorts": [{"timestamp": "created_time", "direction": "ascending"}]}
    if after_iso:
        payload["filter"] = {
            "timestamp": "created_time",
            "created_time": {"after": after_iso}
        }
    results = []
    while True:
        resp = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
    return results

def list_new_blocks(page_id: str, after_dt: datetime = None) -> List[Dict[str, Any]]:
    """페이지 본문 블록 중 새로 생성된 것만 추출"""
    url = f"{NOTION_BASE}/blocks/{page_id}/children?page_size=100"
    out = []
    while True:
        resp = requests.get(url, headers=NOTION_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for blk in data.get("results", []):
            ctime = dtparser.parse(blk.get("created_time"))
            if after_dt is None or ctime > after_dt:
                out.append(blk)
        if not data.get("has_more"):
            break
        url = f"{NOTION_BASE}/blocks/{page_id}/children?start_cursor={data.get('next_cursor')}&page_size=100"
    return out

def page_title(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    # 첫 번째 title 속성 자동 탐색
    for v in props.values():
        if v.get("type") == "title":
            rich = v.get("title", [])
            if rich:
                return "".join([r.get("plain_text", "") for r in rich])[:120] or "(제목 없음)"
    return "(제목 없음)"

def page_url(page: Dict[str, Any]) -> str:
    return page.get("url", "https://www.notion.so")

def format_page_block(page):
    title = page_title(page)
    url = page_url(page)
    created = page.get("created_time", "")
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*새 페이지가 생성되었습니다*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*제목:*\n<{url}|{title}>"},
            {"type": "mrkdwn", "text": f"*생성시각 (UTC):*\n{created}"},
        ]},
        {"type": "divider"}
    ]

def format_block_item(block):
    btype = block.get("type")
    created = block.get("created_time", "")
    txt = ""
    data = block.get(btype, {})
    if isinstance(data, dict):
        if "rich_text" in data and data["rich_text"]:
            txt = "".join([r.get("plain_text", "") for r in data["rich_text"]])[:200]
        elif "text" in data and data["text"]:
            txt = "".join([r.get("plain_text", "") for r in data["text"]])[:200]
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*페이지 내 새 블록 추가*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*블록 타입:*\n`{btype}`"},
            {"type": "mrkdwn", "text": f"*생성시각 (UTC):*\n{created}"}
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*내용 미리보기:*\n{txt or '(텍스트 없음)'}"}},
        {"type": "divider"}
    ]

def main():
    state = load_state()
    last_checked = state.get("last_checked")
    seen_ids = set(state.get("seen_ids", []))

    # 첫 실행 시: 최근 30분만 훑고 시작(중복 폭주 방지)
    if not last_checked:
        # last_checked_dt = datetime.now(timezone.utc) - (60 * 30).__rshift__(0)  # dummy
        # last_checked_dt = datetime.now(timezone.utc)  # 실사용: 이제부터
        # last_checked_iso = last_checked_dt.isoformat()
        last_checked_iso = datetime.now(timezone.utc).isoformat()
    else:
        last_checked_iso = last_checked

    any_posted = False

    # 1) DB 신규 페이지
    for dbid in DB_IDS:
        pages = query_new_db_pages(dbid, after_iso=last_checked_iso)
        for p in pages:
            pid = p.get("id")
            if pid in seen_ids:
                continue
            blocks = format_page_block(p)
            slack_post("Notion 새 페이지", blocks)
            seen_ids.add(pid)
            any_posted = True

    # 2) 특정 페이지의 새 블록
    if PAGE_IDS:
        after_dt = dtparser.parse(last_checked_iso) if last_checked_iso else None
        for pgid in PAGE_IDS:
            blks = list_new_blocks(pgid, after_dt=after_dt)
            for b in blks:
                bid = b.get("id")
                if bid in seen_ids:
                    continue
                blocks = format_block_item(b)
                slack_post("Notion 새 블록", blocks)
                seen_ids.add(bid)
                any_posted = True

    # 상태 갱신
    state["last_checked"] = now_iso()
    state["seen_ids"] = list(seen_ids)
    save_state(state)

if __name__ == "__main__":
    main()
