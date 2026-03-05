from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
SEARCH_CATEGORIES_URL = "https://api.twitch.tv/helix/search/categories"
GAMES_URL = "https://api.twitch.tv/helix/games"
STREAMS_URL = "https://api.twitch.tv/helix/streams"


def _http_json(url: str, timeout: int, *, headers: dict[str, str] | None = None, data: bytes | None = None) -> dict[str, Any]:
    req = Request(url, headers=headers or {}, data=data)
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def normalize_name(value: str) -> str:
    return " ".join(value.casefold().split())


def load_games(path: Path) -> list[tuple[int, str, str, str | None]]:
    games: list[tuple[int, str, str, str | None]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                raise ValueError(f"Expected at least 2 tab-separated columns in {path}: {line!r}")
            appid_raw, game_name = parts[0], parts[1]
            twitch_lookup_name = parts[2] if len(parts) >= 3 and parts[2] else game_name
            twitch_category_id = parts[3] if len(parts) >= 4 and parts[3] else None
            games.append((int(appid_raw), game_name, twitch_lookup_name, twitch_category_id))
    return games


def fetch_app_access_token(client_id: str, client_secret: str, timeout: int) -> str:
    payload = urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    response = _http_json(TOKEN_URL, timeout, headers={"Content-Type": "application/x-www-form-urlencoded"}, data=payload)
    token = response.get("access_token")
    if not token:
        raise RuntimeError("Twitch token response did not contain access_token")
    return token


def build_api_headers(client_id: str, access_token: str) -> dict[str, str]:
    return {
        "Client-Id": client_id,
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "codex-twitch-viewers-fetcher",
    }


def search_category(lookup_name: str, timeout: int, headers: dict[str, str]) -> dict[str, Any] | None:
    query = urlencode({"query": lookup_name, "first": 20})
    payload = _http_json(f"{SEARCH_CATEGORIES_URL}?{query}", timeout, headers=headers)
    categories = payload.get("data", [])
    if not categories:
        return None

    lookup_name_norm = normalize_name(lookup_name)
    for category in categories:
        category_name = str(category.get("name", ""))
        if normalize_name(category_name) == lookup_name_norm:
            return category
    return None


def get_category_by_id(category_id: str, timeout: int, headers: dict[str, str]) -> dict[str, Any] | None:
    payload = _http_json(f"{GAMES_URL}?{urlencode({'id': category_id})}", timeout, headers=headers)
    categories = payload.get("data", [])
    if not categories:
        return None
    return categories[0]


def fetch_category_viewers(
    category_id: str,
    timeout: int,
    headers: dict[str, str],
    max_pages: int | None,
    sleep_seconds: float,
) -> tuple[int, int, int, bool]:
    total_viewers = 0
    total_streams = 0
    pages_fetched = 0
    cursor: str | None = None

    while True:
        params = {"game_id": category_id, "first": 100}
        if cursor:
            params["after"] = cursor
        payload = _http_json(f"{STREAMS_URL}?{urlencode(params)}", timeout, headers=headers)
        streams = payload.get("data", [])
        total_viewers += sum(int(stream.get("viewer_count", 0)) for stream in streams)
        total_streams += len(streams)
        pages_fetched += 1

        cursor = payload.get("pagination", {}).get("cursor")
        if not cursor or not streams:
            return total_viewers, total_streams, pages_fetched, False
        if max_pages is not None and pages_fetched >= max_pages:
            return total_viewers, total_streams, pages_fetched, True
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Twitch live viewers for a fixed game list.")
    parser.add_argument("--games-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--client-id", type=str, required=True)
    parser.add_argument("--client-secret", type=str, required=True)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--meta-source-file", type=str, default=None)
    parser.add_argument("--meta-ingested-at", type=str, default=None)
    parser.add_argument("--meta-dt", type=str, default=None)
    parser.add_argument("--meta-hour", type=str, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    extra_fields = {
        key: value
        for key, value in {
            "source_file": args.meta_source_file,
            "ingested_at": args.meta_ingested_at,
            "dt": args.meta_dt,
            "hour": args.meta_hour,
        }.items()
        if value is not None
    }

    if not args.client_id.strip() or not args.client_secret.strip():
        raise RuntimeError("Twitch credentials are required via TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET")

    access_token = fetch_app_access_token(args.client_id, args.client_secret, args.timeout)
    headers = build_api_headers(args.client_id, access_token)

    max_pages = args.max_pages if args.max_pages and args.max_pages > 0 else None

    records: list[dict[str, Any]] = []
    for appid, game_name, twitch_lookup_name, twitch_category_id in load_games(args.games_file):
        category = None
        if twitch_category_id is not None:
            category = get_category_by_id(category_id=twitch_category_id, timeout=args.timeout, headers=headers)
        if category is None:
            category = search_category(lookup_name=twitch_lookup_name, timeout=args.timeout, headers=headers)
        if category is None:
            records.append(
                {
                    "appid": appid,
                    "game_name": game_name,
                    "twitch_lookup_name": twitch_lookup_name,
                    "configured_twitch_category_id": twitch_category_id,
                    "twitch_category_id": None,
                    "twitch_category_name": None,
                    "approx_total_viewers": None,
                    "live_channels": 0,
                    "pages_fetched": 0,
                    "is_partial": False,
                    **extra_fields,
                }
            )
            continue

        viewers, streams, pages_fetched, is_partial = fetch_category_viewers(
            category_id=str(category["id"]),
            timeout=args.timeout,
            headers=headers,
            max_pages=max_pages,
            sleep_seconds=args.sleep,
        )
        records.append(
            {
                "appid": appid,
                "game_name": game_name,
                "twitch_lookup_name": twitch_lookup_name,
                "configured_twitch_category_id": twitch_category_id,
                "twitch_category_id": str(category["id"]),
                "twitch_category_name": category.get("name"),
                "approx_total_viewers": viewers,
                "live_channels": streams,
                "pages_fetched": pages_fetched,
                "is_partial": is_partial,
                **extra_fields,
            }
        )
        if args.sleep > 0:
            time.sleep(args.sleep)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")

    print(f"Wrote {len(records)} twitch viewer rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
