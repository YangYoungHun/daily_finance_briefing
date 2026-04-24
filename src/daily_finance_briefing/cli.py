from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from .generator import FinanceDataFetcher, generate_site

KST = ZoneInfo("Asia/Seoul")


def _parse_run_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a daily finance briefing site.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate the static site.")
    generate_parser.add_argument("--run-date", type=_parse_run_date, help="KST run date in YYYY-MM-DD format.")
    generate_parser.add_argument("--output", type=Path, default=Path("site"), help="Output directory for the static site.")
    generate_parser.add_argument("--config", type=Path, default=Path("config/assets.yml"), help="Path to the asset configuration YAML.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command != "generate":
        parser.error(f"Unsupported command: {args.command}")

    run_date = args.run_date or datetime.now(KST).date()
    try:
        result = generate_site(
            run_date=run_date,
            output_root=args.output,
            config_path=args.config,
            fetcher=FinanceDataFetcher(),
        )
    except Exception as exc:
        print(f"generation failed: {exc}", file=sys.stderr)
        return 1

    print(result.message)
    return 0
