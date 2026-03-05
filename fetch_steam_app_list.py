"""Fetch Steam app list and (optionally) app types to JSON/NDJSON.

- ISteamApps/GetAppList (legacy, keyless) or IStoreService/GetAppList (key)
- appdetails (store) to resolve type per appid, with incremental checkpoint
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL_LEGACY = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
API_URL_STORE = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
API_URL_APPDETAILS = "https://store.steampowered.com/api/appdetails"


def _get_json(url: str, timeout: int) -> Dict[str, Any]:
    req = Request(url, headers={"User-Agent": "codex-fetcher"})
    with urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data)


def get_app_type(appid: int, timeout: int = 30) -> str | None:
    """Return type (game/application/dlc/...) for given appid via appdetails."""
    query = urlencode({"appids": appid})
    try:
        payload = _get_json(f"{API_URL_APPDETAILS}?{query}", timeout)
        info = payload.get(str(appid), {})
        data = info.get("data") or {}
        return data.get("type")
    except Exception:
        return None


def _json_text(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def get_app_details(appid: int, timeout: int = 30) -> Dict[str, Any] | None:
    """Return a normalized appdetails payload for given appid."""
    query = urlencode({"appids": appid})
    try:
        payload = _get_json(f"{API_URL_APPDETAILS}?{query}", timeout)
        info = payload.get(str(appid), {})
        if not info.get("success"):
            return None

        data = info.get("data") or {}
        price_overview = data.get("price_overview") or {}
        platforms = data.get("platforms") or {}
        metacritic = data.get("metacritic") or {}
        recommendations = data.get("recommendations") or {}
        release_date = data.get("release_date") or {}

        return {
            "appid": appid,
            "name": data.get("name"),
            "type": data.get("type"),
            "is_free": data.get("is_free"),
            "required_age": data.get("required_age"),
            "short_description": data.get("short_description"),
            "about_the_game": data.get("about_the_game"),
            "supported_languages": data.get("supported_languages"),
            "developers": _json_text(data.get("developers")),
            "publishers": _json_text(data.get("publishers")),
            "website": data.get("website"),
            "platform_windows": platforms.get("windows"),
            "platform_mac": platforms.get("mac"),
            "platform_linux": platforms.get("linux"),
            "metacritic_score": metacritic.get("score"),
            "recommendations_total": recommendations.get("total"),
            "release_date": release_date.get("date"),
            "coming_soon": release_date.get("coming_soon"),
            "price_currency": price_overview.get("currency"),
            "price_initial": price_overview.get("initial"),
            "price_final": price_overview.get("final"),
            "categories_json": _json_text(data.get("categories")),
            "genres_json": _json_text(data.get("genres")),
        }
    except Exception:
        return None


def fetch_app_list(api_key: str | None, retries: int, timeout: int) -> List[Dict[str, Any]]:
    """Return the list of all Steam apps, trying legacy then StoreService."""
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            payload = _get_json(API_URL_LEGACY, timeout)
            return payload.get("applist", {}).get("apps", [])
        except HTTPError as exc:
            last_error = exc
            break
        except (URLError, ValueError, TimeoutError) as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(1.5 ** attempt)

    if api_key is None:
        raise RuntimeError(
            "?? ??????? ???????? ?????? ????? ISteamApps, ? ???? ?? ??????. "
            "??????? --api-key ??? IStoreService/GetAppList."
        ) from last_error

    apps: List[Dict[str, Any]] = []
    last_appid = 0

    while True:
        url = f"{API_URL_STORE}?key={api_key}&max_results=50000&last_appid={last_appid}"
        try:
            payload = _get_json(url, timeout)
        except (HTTPError, URLError, ValueError, TimeoutError) as exc:
            raise RuntimeError("?????? ??? ??????? IStoreService/GetAppList") from exc

        chunk = payload.get("response", {}).get("apps", [])
        apps.extend(chunk)

        if payload.get("response", {}).get("have_more_results"):
            last_appid = payload["response"].get("last_appid", 0)
            if not last_appid:
                raise RuntimeError("API ?? ?????? last_appid ??? ??????????? ?????????")
            continue
        break

    return apps


def write_json(apps: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(apps, handle, indent=2, ensure_ascii=True)


def write_ndjson(
    records: List[Dict[str, Any]], output_path: Path, extra_fields: Dict[str, Any] | None = None
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    extra_fields = extra_fields or {}
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            payload = dict(record)
            payload.update(extra_fields)
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the full Steam app catalog to a JSON file."
    )
    parser.add_argument("-o", "--output", type=Path, default=Path("steam_app_list.json"))
    parser.add_argument(
        "--output-ndjson",
        action="store_true",
        help="Write the main app list as NDJSON instead of a single JSON array.",
    )
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--api-key", type=str, default=None)
    parser.add_argument("--sort", action="store_true")
    parser.add_argument("--meta-source-file", type=str, default=None)
    parser.add_argument("--meta-ingested-at", type=str, default=None)
    parser.add_argument("--meta-dt", type=str, default=None)
    parser.add_argument("--meta-hour", type=str, default=None)
    parser.add_argument(
        "--types-output",
        type=Path,
        default=None,
        help="???? ??????, ????????? ???? (appid/name/type). ????? ???????????? --types-ndjson ??? ??????? ???????.",
    )
    parser.add_argument("--types-limit", type=int, default=0, help="0 = ???")
    parser.add_argument(
        "--types-ndjson",
        action="store_true",
        help="?????? ???? ? NDJSON (???? ?????? ?? ??????).",
    )
    parser.add_argument(
        "--types-sleep",
        type=float,
        default=0.1,
        help="????? ????? ????????? ?????, ???.",
    )
    parser.add_argument(
        "--types-checkpoint",
        type=Path,
        default=None,
        help="???????? last_appid ??? ??????????????? ???????? ????? (?????? NDJSON).",
    )
    parser.add_argument(
        "--details-output",
        type=Path,
        default=None,
        help="Output path for full normalized appdetails records.",
    )
    parser.add_argument(
        "--details-limit",
        type=int,
        default=0,
        help="0 = all appdetails records.",
    )
    parser.add_argument(
        "--details-ndjson",
        action="store_true",
        help="Write normalized appdetails as NDJSON.",
    )
    parser.add_argument(
        "--details-sleep",
        type=float,
        default=0.0,
        help="Delay between appdetails requests, seconds.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
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

    apps = fetch_app_list(api_key=args.api_key, retries=args.retries, timeout=args.timeout)
    if args.sort:
        apps = sorted(apps, key=lambda item: int(item.get("appid", 0)))

    if args.output_ndjson:
        write_ndjson(apps, args.output, extra_fields=extra_fields)
    else:
        write_json(apps, args.output)
    print(f"Wrote {len(apps):,} entries to {args.output}")

    if args.types_output:
        if args.types_checkpoint and not args.types_ndjson:
            raise SystemExit("???????? ?????????????? ?????? ? --types-ndjson")

        limit = args.types_limit if args.types_limit > 0 else len(apps)
        out_path = args.types_output
        out_path.parent.mkdir(parents=True, exist_ok=True)

        last_appid = None
        if args.types_checkpoint and args.types_checkpoint.exists():
            try:
                with args.types_checkpoint.open("r", encoding="utf-8") as cp:
                    saved = json.load(cp)
                    last_appid = int(saved.get("last_appid", 0))
            except Exception:
                last_appid = None

        def iter_records():
            processed = 0
            for item in apps:
                appid = int(item.get("appid", 0))
                if last_appid and appid <= last_appid:
                    continue
                if limit and processed >= limit:
                    break
                type_val = get_app_type(appid, timeout=args.timeout)
                processed += 1
                payload = {"appid": appid, "name": item.get("name"), "type": type_val}
                if extra_fields:
                    payload.update(extra_fields)
                yield payload
                if args.types_sleep > 0:
                    time.sleep(args.types_sleep)

        if args.types_ndjson:
            mode = "a" if args.types_checkpoint else "w"
            written = 0
            with out_path.open(mode, encoding="utf-8") as handle:
                for rec in iter_records():
                    handle.write(json.dumps(rec, ensure_ascii=True) + "\n")
                    written += 1
        else:
            records = list(iter_records())
            written = len(records)
            with out_path.open("w", encoding="utf-8") as handle:
                json.dump(records, handle, indent=2, ensure_ascii=True)

        if args.types_checkpoint:
            max_appid = last_appid
            try:
                with out_path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        line = line.strip()
                        if not line:
                            continue
                        obj = json.loads(line)
                        aid = int(obj.get("appid", 0))
                        if max_appid is None or aid > max_appid:
                            max_appid = aid
            except Exception:
                pass

            if max_appid is not None:
                args.types_checkpoint.parent.mkdir(parents=True, exist_ok=True)
                with args.types_checkpoint.open("w", encoding="utf-8") as cp:
                    json.dump(
                        {"last_appid": max_appid, "updated_at": datetime.utcnow().isoformat()},
                        cp,
                        ensure_ascii=True,
                        indent=2,
                    )

        print(f"Wrote types for {written:,} apps to {out_path}")

    if args.details_output:
        limit = args.details_limit if args.details_limit > 0 else len(apps)
        out_path = args.details_output
        out_path.parent.mkdir(parents=True, exist_ok=True)

        def iter_details():
            processed = 0
            for item in apps:
                appid = int(item.get("appid", 0))
                if limit and processed >= limit:
                    break
                details = get_app_details(appid, timeout=args.timeout)
                processed += 1
                if details is None:
                    continue
                if extra_fields:
                    details.update(extra_fields)
                yield details
                if args.details_sleep > 0:
                    time.sleep(args.details_sleep)

        if args.details_ndjson:
            written = 0
            with out_path.open("w", encoding="utf-8") as handle:
                for rec in iter_details():
                    handle.write(json.dumps(rec, ensure_ascii=True) + "\n")
                    written += 1
        else:
            records = list(iter_details())
            written = len(records)
            with out_path.open("w", encoding="utf-8") as handle:
                json.dump(records, handle, indent=2, ensure_ascii=True)

        print(f"Wrote appdetails for {written:,} apps to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
