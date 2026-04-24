from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pandas as pd
import pytest

from daily_finance_briefing.generator import generate_site


class FakeFetcher:
    def __init__(self, responses: dict[str, pd.DataFrame | Exception]) -> None:
        self.responses = responses

    def fetch(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        response = self.responses[symbol]
        if isinstance(response, Exception):
            raise response
        return response


def write_config(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_generate_site_builds_latest_and_archive(tmp_path: Path) -> None:
    config_path = tmp_path / "assets.yml"
    output_root = tmp_path / "site"
    write_config(
        config_path,
        """
assets:
  - section: Domestic
    display_label: KOSPI
    symbol: KS11
    decimals: 2
    sort_order: 10
""".strip(),
    )
    frame = pd.DataFrame(
        {"Close": [100.0, None, 110.0]},
        index=pd.to_datetime(["2026-04-21", "2026-04-22", "2026-04-23"]),
    )

    result = generate_site(
        run_date=date(2026, 4, 24),
        output_root=output_root,
        config_path=config_path,
        fetcher=FakeFetcher({"KS11": frame}),
    )

    assert result.status == "generated"
    latest_payload = json.loads((output_root / "latest" / "data.json").read_text(encoding="utf-8"))
    archive_payload = json.loads((output_root / "archive" / "2026-04-24" / "data.json").read_text(encoding="utf-8"))
    latest_html = (output_root / "latest" / "index.html").read_text(encoding="utf-8")

    assert latest_payload["items"][0]["value"] == 110.0
    assert latest_payload["items"][0]["change_pct"] == 10.0
    assert latest_payload["items"][0]["as_of_date"] == "2026-04-23"
    assert archive_payload["run_date"] == "2026-04-24"
    assert "110.00" in latest_html
    assert "▲ 10.00%" in latest_html


def test_generate_site_marks_missing_symbols_but_continues(tmp_path: Path) -> None:
    config_path = tmp_path / "assets.yml"
    output_root = tmp_path / "site"
    write_config(
        config_path,
        """
assets:
  - section: Domestic
    display_label: KOSPI
    symbol: KS11
    decimals: 2
    sort_order: 10
  - section: Global
    display_label: NASDAQ
    symbol: IXIC
    decimals: 2
    sort_order: 20
""".strip(),
    )
    frame = pd.DataFrame(
        {"Close": [200.0, 190.0]},
        index=pd.to_datetime(["2026-04-22", "2026-04-23"]),
    )

    result = generate_site(
        run_date=date(2026, 4, 24),
        output_root=output_root,
        config_path=config_path,
        fetcher=FakeFetcher({"KS11": frame, "IXIC": RuntimeError("boom")}),
    )

    assert result.status == "generated"
    payload = json.loads((output_root / "latest" / "data.json").read_text(encoding="utf-8"))
    html = (output_root / "latest" / "index.html").read_text(encoding="utf-8")

    assert payload["summary"]["missing_items"] == 1
    assert any(item["status"] == "missing" for item in payload["items"])
    assert "데이터 없음" in html


def test_generate_site_returns_no_new_data_for_same_signature(tmp_path: Path) -> None:
    config_path = tmp_path / "assets.yml"
    output_root = tmp_path / "site"
    write_config(
        config_path,
        """
assets:
  - section: Commodities
    display_label: Gold
    symbol: GC=F
    decimals: 2
    sort_order: 10
""".strip(),
    )
    frame = pd.DataFrame(
        {"Close": [300.0, 303.0]},
        index=pd.to_datetime(["2026-04-22", "2026-04-23"]),
    )
    fetcher = FakeFetcher({"GC=F": frame})

    first_result = generate_site(
        run_date=date(2026, 4, 24),
        output_root=output_root,
        config_path=config_path,
        fetcher=fetcher,
    )
    second_result = generate_site(
        run_date=date(2026, 4, 24),
        output_root=output_root,
        config_path=config_path,
        fetcher=fetcher,
    )

    assert first_result.status == "generated"
    assert second_result.status == "no_new_data"
    archive_dirs = list((output_root / "archive").iterdir())
    assert len(archive_dirs) == 1


def test_generate_site_fails_when_all_symbols_are_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "assets.yml"
    output_root = tmp_path / "site"
    write_config(
        config_path,
        """
assets:
  - section: FX
    display_label: KRW/USD
    symbol: USD/KRW
    decimals: 2
    sort_order: 10
""".strip(),
    )

    with pytest.raises(RuntimeError):
        generate_site(
            run_date=date(2026, 4, 24),
            output_root=output_root,
            config_path=config_path,
            fetcher=FakeFetcher({"USD/KRW": RuntimeError("missing")}),
        )
