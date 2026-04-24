from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from importlib.resources import files
import json
from pathlib import Path
import time
from typing import Protocol
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

KST = ZoneInfo("Asia/Seoul")
LOOKBACK_DAYS = 14
MAX_FETCH_ATTEMPTS = 3


@dataclass(frozen=True)
class AssetConfig:
    section: str
    display_label: str
    symbol: str
    decimals: int
    sort_order: int


@dataclass(frozen=True)
class ReportItem:
    section: str
    label: str
    symbol: str
    decimals: int
    sort_order: int
    value: float | None
    change_pct: float | None
    direction: str
    as_of_date: str | None
    status: str

    def to_payload(self) -> dict[str, object]:
        return {
            "section": self.section,
            "label": self.label,
            "symbol": self.symbol,
            "value": self.value,
            "change_pct": self.change_pct,
            "direction": self.direction,
            "as_of_date": self.as_of_date,
            "status": self.status,
        }

    def to_view_model(self) -> dict[str, object]:
        return {
            "section": self.section,
            "label": self.label,
            "symbol": self.symbol,
            "value": self.value,
            "formatted_value": format_value(self.value, self.decimals),
            "change_pct": self.change_pct,
            "formatted_change_pct": format_change_pct(self.change_pct),
            "direction": self.direction,
            "change_indicator": direction_indicator(self.direction),
            "as_of_date": self.as_of_date,
            "as_of_date_display": f"기준일 {self.as_of_date}" if self.as_of_date else "기준일 없음",
            "status": self.status,
            "css_class": f"is-{self.direction}",
        }


@dataclass(frozen=True)
class GenerationResult:
    status: str
    message: str


class MarketDataFetcher(Protocol):
    def fetch(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        ...


class FinanceDataFetcher:
    def fetch(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        import FinanceDataReader as fdr

        return fdr.DataReader(symbol, start.isoformat(), end.isoformat())


def load_assets(config_path: Path) -> list[AssetConfig]:
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    assets = raw.get("assets", [])
    if not assets:
        raise ValueError(f"No assets found in config: {config_path}")

    return [
        AssetConfig(
            section=item["section"],
            display_label=item["display_label"],
            symbol=item["symbol"],
            decimals=int(item["decimals"]),
            sort_order=int(item["sort_order"]),
        )
        for item in assets
    ]


def generate_site(
    run_date: date,
    output_root: Path,
    config_path: Path,
    fetcher: MarketDataFetcher,
) -> GenerationResult:
    assets = load_assets(config_path)
    generated_at = datetime.now(KST).replace(microsecond=0).isoformat()
    items = collect_items(assets=assets, run_date=run_date, fetcher=fetcher)
    ok_items = [item for item in items if item.status == "ok"]

    if not ok_items:
        raise RuntimeError("No market data could be collected for any configured symbol.")

    payload = build_payload(run_date=run_date, generated_at=generated_at, items=items)
    latest_data_path = output_root / "latest" / "data.json"
    existing_signature = load_existing_signature(latest_data_path)
    has_new_market_data = existing_signature != payload["signature"]

    latest_dir = output_root / "latest"
    ensure_directory(latest_dir)

    archive_entries = collect_archive_entries(output_root)
    if has_new_market_data:
        archive_dir = output_root / "archive" / run_date.isoformat()
        ensure_directory(archive_dir)
        archive_payload = payload.copy()
        write_report_bundle(
            output_dir=archive_dir,
            payload=archive_payload,
            archive_entries=[],
            page_title=f"전일 시장 요약 - {run_date.isoformat()}",
            home_href="../../index.html",
            latest_href="../../latest/index.html",
        )
        archive_entries.insert(
            0,
            {
                "run_date": run_date.isoformat(),
                "latest_market_date": payload["summary"]["latest_market_date"],
                "href": f"archive/{run_date.isoformat()}/index.html",
            },
        )

    archive_entries = deduplicate_archive_entries(archive_entries)

    write_report_bundle(
        output_dir=latest_dir,
        payload=payload.copy(),
        archive_entries=[],
        page_title="전일 시장 요약 - Latest",
        home_href="../index.html",
        latest_href="index.html",
    )
    write_root_bundle(output_root=output_root, payload=payload, archive_entries=archive_entries)

    if not has_new_market_data:
        return GenerationResult(status="no_new_data", message="no new market data")

    return GenerationResult(
        status="generated",
        message=f"generated report for {run_date.isoformat()}",
    )


def collect_items(assets: list[AssetConfig], run_date: date, fetcher: MarketDataFetcher) -> list[ReportItem]:
    start = run_date - timedelta(days=LOOKBACK_DAYS)
    end = run_date - timedelta(days=1)
    items = []
    ordered_assets = order_assets(assets)

    for asset in ordered_assets:
        items.append(collect_item(asset=asset, start=start, end=end, fetcher=fetcher))

    return items


def collect_item(asset: AssetConfig, start: date, end: date, fetcher: MarketDataFetcher) -> ReportItem:
    try:
        frame = fetch_with_retries(fetcher=fetcher, symbol=asset.symbol, start=start, end=end)
        return build_report_item(asset=asset, frame=frame)
    except Exception:
        return ReportItem(
            section=asset.section,
            label=asset.display_label,
            symbol=asset.symbol,
            decimals=asset.decimals,
            sort_order=asset.sort_order,
            value=None,
            change_pct=None,
            direction="missing",
            as_of_date=None,
            status="missing",
        )


def build_report_item(asset: AssetConfig, frame: pd.DataFrame) -> ReportItem:
    if frame.empty or "Close" not in frame.columns:
        return missing_item(asset)

    close_series = frame["Close"].dropna()
    if len(close_series) < 2:
        return missing_item(asset)

    latest_value = float(close_series.iloc[-1])
    previous_value = float(close_series.iloc[-2])
    latest_index = close_series.index[-1]
    change_pct = 0.0 if previous_value == 0 else ((latest_value - previous_value) / previous_value) * 100

    return ReportItem(
        section=asset.section,
        label=asset.display_label,
        symbol=asset.symbol,
        decimals=asset.decimals,
        sort_order=asset.sort_order,
        value=latest_value,
        change_pct=round(change_pct, 4),
        direction=direction_from_change(change_pct),
        as_of_date=index_to_iso_date(latest_index),
        status="ok",
    )


def fetch_with_retries(fetcher: MarketDataFetcher, symbol: str, start: date, end: date) -> pd.DataFrame:
    last_error = None

    for attempt in range(MAX_FETCH_ATTEMPTS):
        try:
            return fetcher.fetch(symbol, start, end)
        except Exception as exc:
            last_error = exc
            if attempt < MAX_FETCH_ATTEMPTS - 1:
                time.sleep(2 ** attempt)

    if last_error is None:
        raise RuntimeError(f"Unable to fetch data for {symbol}")
    raise last_error


def missing_item(asset: AssetConfig) -> ReportItem:
    return ReportItem(
        section=asset.section,
        label=asset.display_label,
        symbol=asset.symbol,
        decimals=asset.decimals,
        sort_order=asset.sort_order,
        value=None,
        change_pct=None,
        direction="missing",
        as_of_date=None,
        status="missing",
    )


def build_payload(run_date: date, generated_at: str, items: list[ReportItem]) -> dict[str, object]:
    latest_market_date = max((item.as_of_date for item in items if item.as_of_date), default=None)
    grouped_sections = []
    section_order = []

    for item in items:
        if item.section not in section_order:
            section_order.append(item.section)

    for section in section_order:
        section_items = [item.to_view_model() for item in items if item.section == section]
        grouped_sections.append({"name": section, "items": section_items})

    payload_items = [item.to_payload() for item in items]
    signature = build_signature(items)

    return {
        "title": "전일 시장 요약",
        "run_date": run_date.isoformat(),
        "generated_at": generated_at,
        "summary": {
            "successful_items": len([item for item in items if item.status == "ok"]),
            "missing_items": len([item for item in items if item.status == "missing"]),
            "latest_market_date": latest_market_date,
            "note": "각 항목은 자산별 최신 거래일 기준으로 표시됩니다.",
        },
        "sections": grouped_sections,
        "items": payload_items,
        "signature": signature,
    }


def order_assets(assets: list[AssetConfig]) -> list[AssetConfig]:
    section_order = list(dict.fromkeys(asset.section for asset in assets))
    ordered_assets = []

    for section in section_order:
        ordered_assets.extend(
            sorted(
                [asset for asset in assets if asset.section == section],
                key=lambda asset: (asset.sort_order, asset.symbol),
            )
        )

    return ordered_assets


def build_signature(items: list[ReportItem]) -> list[dict[str, object]]:
    signature = []
    for item in items:
        signature.append(
            {
                "symbol": item.symbol,
                "status": item.status,
                "value": round(item.value, 8) if item.value is not None else None,
                "change_pct": round(item.change_pct, 8) if item.change_pct is not None else None,
                "as_of_date": item.as_of_date,
            }
        )
    return signature


def load_existing_signature(latest_data_path: Path) -> list[dict[str, object]] | None:
    if not latest_data_path.exists():
        return None

    with latest_data_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    return payload.get("signature")


def collect_archive_entries(output_root: Path) -> list[dict[str, str | None]]:
    entries = []
    archive_root = output_root / "archive"
    if not archive_root.exists():
        return entries

    for data_path in sorted(archive_root.glob("*/data.json"), reverse=True):
        with data_path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
        run_date = payload.get("run_date")
        entries.append(
            {
                "run_date": run_date,
                "latest_market_date": payload.get("summary", {}).get("latest_market_date"),
                "href": f"archive/{run_date}/index.html",
            }
        )

    return entries


def deduplicate_archive_entries(entries: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    seen = set()
    deduplicated = []
    for entry in entries:
        run_date = entry["run_date"]
        if run_date in seen:
            continue
        seen.add(run_date)
        deduplicated.append(entry)
    return deduplicated


def write_report_bundle(
    output_dir: Path,
    payload: dict[str, object],
    archive_entries: list[dict[str, str | None]],
    page_title: str,
    home_href: str,
    latest_href: str,
) -> None:
    html = render_report(
        payload=payload,
        page_title=page_title,
        archive_entries=archive_entries,
        home_href=home_href,
        latest_href=latest_href,
    )
    write_text(output_dir / "index.html", html)
    write_json(output_dir / "data.json", payload)


def write_root_bundle(output_root: Path, payload: dict[str, object], archive_entries: list[dict[str, str | None]]) -> None:
    root_payload = dict(payload)
    root_payload["archive_entries"] = archive_entries
    html = render_report(
        payload=root_payload,
        page_title="전일 시장 요약",
        archive_entries=archive_entries,
        home_href="index.html",
        latest_href="latest/index.html",
    )
    write_text(output_root / "index.html", html)
    write_json(output_root / "data.json", root_payload)


def render_report(
    payload: dict[str, object],
    page_title: str,
    archive_entries: list[dict[str, str | None]],
    home_href: str,
    latest_href: str,
) -> str:
    template_root = files("daily_finance_briefing").joinpath("templates")
    environment = Environment(
        loader=FileSystemLoader(str(template_root)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = environment.get_template("report.html.j2")
    return template.render(
        page_title=page_title,
        payload=payload,
        archive_entries=archive_entries,
        home_href=home_href,
        latest_href=latest_href,
    )


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str) -> None:
    ensure_directory(path.parent)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: dict[str, object]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def format_value(value: float | None, decimals: int) -> str:
    if value is None:
        return "데이터 없음"
    return f"{value:,.{decimals}f}"


def format_change_pct(change_pct: float | None) -> str:
    if change_pct is None:
        return "데이터 없음"
    return f"{abs(change_pct):.2f}%"


def direction_indicator(direction: str) -> str:
    return {
        "up": "▲",
        "down": "▼",
        "flat": "■",
        "missing": "-",
    }[direction]


def direction_from_change(change_pct: float) -> str:
    if change_pct > 0:
        return "up"
    if change_pct < 0:
        return "down"
    return "flat"


def index_to_iso_date(value: object) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
