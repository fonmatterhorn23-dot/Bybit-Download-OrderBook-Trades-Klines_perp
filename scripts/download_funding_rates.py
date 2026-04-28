#!/usr/bin/env python3
"""
Загрузчик исторических ставок финансирования Bybit (Funding Rates).
Использует публичный API Bybit, не требует API-ключа.
Сохраняет CSV с полями: symbol, fundingRate, fundingRateTimestamp (ISO-8601).
"""

import csv
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

import requests


API_URL = "https://api.bybit.com/v5/market/funding/history"
PAGE_LIMIT = 200
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3


def log(msg: str) -> None:
    """Логирование с временной меткой."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def parse_symbols_file(symbols_file: str) -> list[str]:
    """Читаем список торговых пар из файла."""
    symbols = []
    text = Path(symbols_file).read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        symbols.extend(
            part.strip().upper()
            for part in line.replace(",", " ").split()
            if part.strip()
        )
    return symbols


def resolve_symbols(symbol: Optional[str], symbols_arg: Optional[str], symbols_file: Optional[str]) -> List[str]:
    """Собираем уникальный список символов."""
    candidates = []
    if symbols_file:
        candidates.extend(parse_symbols_file(symbols_file))
    if symbols_arg:
        candidates.extend(part.strip().upper() for part in symbols_arg.split(",") if part.strip())
    if symbol:
        candidates.append(symbol.upper())
    if not candidates:
        candidates.append("BTCUSDT")
    seen = set()
    symbols = []
    for c in candidates:
        if c and c not in seen:
            symbols.append(c)
            seen.add(c)
    return symbols


def fetch_funding_history(symbol: str, start_ms: int, end_ms: int, max_retries: int = MAX_RETRIES) -> List[Dict]:
    """
    Получает полную историю ставок для символа за период [start_ms, end_ms].
    Автоматически обрабатывает пагинацию (limit=200).
    Возвращает список словарей с ключами: symbol, fundingRate, fundingRateTimestamp.
    """
    all_rows = []
    current_start = start_ms

    for attempt in range(max_retries):
        try:
            while True:
                params = {
                    "category": "linear",
                    "symbol": symbol,
                    "limit": PAGE_LIMIT,
                    "startTime": current_start,
                    "endTime": end_ms,
                }
                resp = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                if data.get("retCode") != 0:
                    log(f"  API error for {symbol}: {data.get('retMsg', 'Unknown')}")
                    return all_rows  # return what we have so far

                batch = data["result"]["list"]
                if not batch:
                    break

                all_rows.extend(batch)

                if len(batch) < PAGE_LIMIT:
                    break

                # Сдвигаем startTime на timestamp последней записи + 1 мс
                last_ts = int(batch[-1]["fundingRateTimestamp"])
                current_start = last_ts + 1

            return all_rows

        except requests.exceptions.RequestException as e:
            log(f"  Request error for {symbol}, attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(2)
        except Exception as e:
            log(f"  Unexpected error for {symbol}, attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(2)

    log(f"  Failed to fetch full data for {symbol} after {max_retries} retries")
    return all_rows  # return partial data


def download_symbol(symbol: str, start: datetime, end: datetime, output_dir: Path) -> Dict[str, int]:
    """
    Загружает историю ставок для одного символа и сохраняет в CSV.
    Возвращает статистику.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{symbol}_funding_rates.csv"

    # Преобразуем даты в миллисекунды UTC
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    log(f"  Processing {symbol} ...")
    rows = fetch_funding_history(symbol, start_ms, end_ms)

    if not rows:
        log(f"  No data for {symbol} in this period.")
        return {"downloaded": 0, "errors": 0}

    # Пишем CSV
    try:
        with open(out_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["symbol", "fundingRate", "fundingRateTimestamp"])
            for r in rows:
                # fundingRateTimestamp может быть строкой или числом
                ts = int(r["fundingRateTimestamp"])
                iso_ts = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
                writer.writerow([r["symbol"], r["fundingRate"], iso_ts])

        file_size_mb = out_file.stat().st_size / 1024 / 1024
        log(f"  ✓ {symbol}: saved {len(rows)} rows ({file_size_mb:.2f} MB)")
        return {"downloaded": len(rows), "errors": 0}
    except Exception as e:
        log(f"  ✗ Failed to save CSV for {symbol}: {e}")
        return {"downloaded": 0, "errors": len(rows)}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Загружаем исторические ставки финансирования Bybit (USDT-маржированные бессрочные контракты)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python download_funding_rates.py BTCUSDT --start-date 2026-01-01 --end-date 2026-04-20
  python download_funding_rates.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --start-date 2026-01-01 --end-date 2026-04-20
  python download_funding_rates.py --symbols-file pairs.txt --start-date 2026-01-01 --end-date 2026-04-20 --workers 5
        """,
    )
    parser.add_argument("symbol", nargs="?", default=None, help="Торговая пара")
    parser.add_argument("--symbols", type=str, default=None, help="Список пар через запятую")
    parser.add_argument("--symbols-file", type=str, default=None, help="Файл со списком пар")
    parser.add_argument("--start-date", type=str, required=True, help="Начальная дата (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True, help="Конечная дата (YYYY-MM-DD)")
    parser.add_argument("--output-dir", type=str, default="data/funding_rates", help="Директория для CSV")
    parser.add_argument("--workers", type=int, default=3, help="Количество параллельных запросов (символов)")

    args = parser.parse_args()

    try:
        symbols = resolve_symbols(args.symbol, args.symbols, args.symbols_file)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")
    if end < start:
        parser.error("end-date must be >= start-date")

    output_dir = Path(args.output_dir)

    print("=" * 60)
    print("Bybit Funding Rates Downloader")
    print("=" * 60)
    print(f"Symbols:      {', '.join(symbols)}")
    print(f"Period:       {args.start_date} to {args.end_date}")
    print(f"Output dir:   {output_dir}")
    print(f"Workers:      {args.workers}")
    print("=" * 60)

    total_downloaded = 0
    total_errors = 0

    if args.workers <= 1:
        # Последовательная обработка
        for symbol in symbols:
            stats = download_symbol(symbol, start, end, output_dir)
            total_downloaded += stats["downloaded"]
            total_errors += stats["errors"]
    else:
        # Параллельная обработка (осторожно с rate limit)
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(download_symbol, sym, start, end, output_dir): sym for sym in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    stats = future.result()
                    total_downloaded += stats["downloaded"]
                    total_errors += stats["errors"]
                except Exception as e:
                    log(f"  ✗ {symbol} unexpected failure: {e}")
                    total_errors += 1

    print("\n" + "=" * 60)
    print(f"Итого: {total_downloaded} записей скачано, ошибок в строках: {total_errors}")
    print("=" * 60)


if __name__ == "__main__":
    main()