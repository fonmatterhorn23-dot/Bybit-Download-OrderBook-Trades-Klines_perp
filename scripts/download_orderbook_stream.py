#!/usr/bin/env python3
"""
Загрузчик Order Book данных Bybit со Streaming конвертацией в Parquet.
Скачиваем → Конвертируем → Удаляем ZIP.
Экономим место на диске в 2-3 раза по сравнению с хранением ZIP.
"""

import os
import json
import time
import random
import shutil
import zipfile
import argparse
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Iterator

import requests
import pyarrow as pa
import pyarrow.parquet as pq
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_DEPTH_BY_MARKET = {
    "spot": 200,
    "linear": 200,
    "inverse": 200,
}
MAX_SYMBOLS = 30
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_CHUNK_SIZE_MB = 1
DEFAULT_COMPRESSION_LEVEL = 6
MANIFEST_VERSION = 1
THREAD_LOCAL = threading.local()
PARQUET_SCHEMA = pa.schema([
    ("ts", pa.int64()),
    ("cts", pa.int64()),
    ("type", pa.string()),
    ("u", pa.int64()),
    ("seq", pa.int64()),
    ("bids", pa.string()),
    ("asks", pa.string()),
])


@dataclass(frozen=True)
class DownloadTask:
    symbol: str
    market: str
    depth: int
    date: str
    url: str
    zip_filename: str
    output_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class ConversionStats:
    records: int
    parse_errors: int
    parquet_rows: int
    parquet_bytes: int


@dataclass(frozen=True)
class TaskResult:
    ok: bool
    status: str
    records: int = 0
    parquet_mb: float = 0.0
    zip_mb: float = 0.0
    elapsed_seconds: float = 0.0
    parse_errors: int = 0


@dataclass
class QueueStats:
    success: int = 0
    failed: int = 0
    not_found: int = 0
    skipped: int = 0
    reprocess: int = 0
    total_mb: float = 0.0
    disk_full: bool = False


@dataclass(frozen=True)
class PreparedTasks:
    tasks: list[DownloadTask]
    skipped: int
    reprocess: int


def get_disk_free_gb(path: Path) -> float:
    """
    Получаем свободное место на диске в ГБ.

    params:
        path: Путь для проверки
    return:
        Свободное место в ГБ
    """
    stat = shutil.disk_usage(path)
    return stat.free / (1024 ** 3)


def parse_record(data: dict[str, object]) -> dict[str, object]:
    """
    Парсим одну запись Order Book в плоскую структуру.

    params:
        data: Сырая JSON запись
    return:
        Плоский словарь
    """
    payload = data["data"]
    if not isinstance(payload, dict):
        raise ValueError("Order book payload must be an object")

    return {
        "ts": data["ts"],
        "cts": data.get("cts"),
        "type": data["type"],
        "u": payload["u"],
        "seq": payload["seq"],
        "bids": json.dumps(payload.get("b", [])),
        "asks": json.dumps(payload.get("a", [])),
    }


def log(msg: str) -> None:
    """
    Логируем с timestamp.

    params:
        msg: Сообщение
    return:
        None
    """
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def parse_symbols_file(symbols_file: str) -> list[str]:
    """
    Читаем список торговых пар из файла.

    params:
        symbols_file: Путь к файлу
    return:
        Список символов
    """
    symbols = []
    text = Path(symbols_file).read_text(encoding="utf-8")

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        symbols.extend(part.strip().upper() for part in line.replace(",", " ").split() if part.strip())

    return symbols


def resolve_symbols(symbol: str | None, symbols_arg: str | None, symbols_file: str | None) -> list[str]:
    """
    Собираем список символов из positional arg, --symbols и --symbols-file.

    params:
        symbol: Одна торговая пара
        symbols_arg: Список пар через запятую
        symbols_file: Файл со списком пар
    return:
        Список уникальных символов
    """
    candidates = []

    if symbols_file:
        candidates.extend(parse_symbols_file(symbols_file))
    if symbols_arg:
        candidates.extend(part.strip().upper() for part in symbols_arg.split(",") if part.strip())
    if symbol:
        candidates.append(symbol.upper())
    if not candidates:
        candidates.append("BTCUSDT")

    symbols = []
    seen = set()
    for candidate in candidates:
        normalized = candidate.strip().upper()
        if normalized and normalized not in seen:
            symbols.append(normalized)
            seen.add(normalized)

    if len(symbols) > MAX_SYMBOLS:
        raise ValueError(f"Too many symbols: {len(symbols)}. Maximum supported is {MAX_SYMBOLS}.")

    return symbols


def build_orderbook_url(market: str, symbol: str, date: datetime, depth: int) -> tuple[str, str]:
    """
    Собираем URL исторического архива Order Book.

    params:
        market: spot, linear или inverse
        symbol: Торговая пара
        date: Дата архива
        depth: Глубина стакана
    return:
        Кортеж (url, filename)
    """
    date_str = date.strftime("%Y-%m-%d")
    filename = f"{date_str}_{symbol}_ob{depth}.data.zip"
    url = f"https://quote-saver.bycsi.com/orderbook/{market}/{symbol}/{filename}"
    return url, filename


def daterange(start: datetime, end: datetime) -> Iterator[datetime]:
    """
    Генерируем даты от start до end включительно.

    params:
        start: Начальная дата
        end: Конечная дата
    return:
        Итератор дат
    """
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def safe_unlink(path: Path | None) -> None:
    """
    Удаляем файл если он существует.

    params:
        path: Путь к файлу
    return:
        None
    """
    if path and path.exists():
        try:
            path.unlink()
        except Exception:
            pass


def build_manifest_path(output_path: Path) -> Path:
    """
    Строим путь к manifest файлу.

    params:
        output_path: Путь к Parquet
    return:
        Путь к manifest
    """
    return output_path.with_suffix(".manifest.json")


def build_task(symbol: str, market: str, depth: int, date: datetime, output_dir: Path) -> DownloadTask:
    """
    Строим одну задачу загрузки.

    params:
        symbol: Торговая пара
        market: Рынок
        depth: Глубина стакана
        date: Дата
        output_dir: Базовая директория Parquet
    return:
        Словарь задачи
    """
    date_str = date.strftime("%Y-%m-%d")
    url, zip_filename = build_orderbook_url(market, symbol, date, depth)
    symbol_dir = output_dir / market / symbol
    output_path = symbol_dir / f"{date_str}_{symbol}_ob{depth}.parquet"
    manifest_path = build_manifest_path(output_path)
    return DownloadTask(
        symbol=symbol,
        market=market,
        depth=depth,
        date=date_str,
        url=url,
        zip_filename=zip_filename,
        output_path=output_path,
        manifest_path=manifest_path,
    )


def get_session() -> requests.Session:
    """
    Возвращаем thread-local HTTP session.

    params:
        None
    return:
        requests.Session
    """
    try:
        return THREAD_LOCAL.session
    except AttributeError:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"User-Agent": "bybit-orderbook-stream/2.0"})
        THREAD_LOCAL.session = session
        return session


def validate_existing_output(task: DownloadTask) -> tuple[bool, str]:
    """
    Проверяем существующие Parquet и manifest перед пропуском.

    params:
        task: Словарь задачи
    return:
        Кортеж (валиден ли файл, причина)
    """
    output_path = task.output_path
    manifest_path = task.manifest_path

    if not output_path.exists():
        return False, "missing_parquet"
    if not manifest_path.exists():
        return False, "missing_manifest"

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False, "invalid_manifest_json"

    required_matches = {
        "status": "success",
        "symbol": task.symbol,
        "market": task.market,
        "date": task.date,
        "depth": task.depth,
        "url": task.url,
    }
    for key, expected in required_matches.items():
        if manifest.get(key) != expected:
            return False, f"manifest_mismatch_{key}"

    if manifest.get("manifest_version") != MANIFEST_VERSION:
        return False, "manifest_version"

    actual_size = output_path.stat().st_size
    if actual_size <= 0:
        return False, "empty_parquet"
    if manifest.get("parquet_bytes") != actual_size:
        return False, "parquet_size_mismatch"
    if manifest.get("zip_bytes", 0) <= 0:
        return False, "missing_zip_size"

    try:
        parquet_rows = pq.ParquetFile(output_path).metadata.num_rows
    except Exception:
        return False, "parquet_read_failed"

    if parquet_rows <= 0:
        return False, "zero_rows"
    if manifest.get("records") != parquet_rows:
        return False, "row_count_mismatch"
    if manifest.get("parse_errors", 0) != 0 and not manifest.get("allow_parse_errors", False):
        return False, "parse_errors_present"

    return True, "ok"


def write_manifest_atomic(manifest_path: Path, manifest: dict[str, object]) -> None:
    """
    Пишем manifest атомарно.

    params:
        manifest_path: Целевой путь
        manifest: Данные manifest
    return:
        None
    """
    temp_manifest = manifest_path.with_name(f"{manifest_path.name}.tmp")
    temp_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_manifest, manifest_path)


def convert_zip_to_parquet(
    zip_path: Path,
    parquet_temp_path: Path,
    batch_size: int,
    compression_level: int,
    allow_parse_errors: bool,
) -> ConversionStats:
    """
    Конвертируем ZIP в Parquet инкрементально по row groups.

    params:
        zip_path: Временный ZIP
        parquet_temp_path: Временный Parquet путь
        batch_size: Размер батча
        compression_level: Уровень ZSTD
        allow_parse_errors: Разрешить ли ошибки парсинга
    return:
        Статистика конвертации
    """
    writer = None
    current_batch: list[dict[str, object]] = []
    total_records = 0
    parse_errors = 0

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            data_file = zf.namelist()[0]
            with zf.open(data_file) as f:
                for line in f:
                    try:
                        data = json.loads(line.decode("utf-8").strip())
                        current_batch.append(parse_record(data))
                    except Exception:
                        parse_errors += 1
                        if not allow_parse_errors:
                            raise ValueError("JSON parse error encountered")
                        continue

                    if len(current_batch) >= batch_size:
                        table = pa.Table.from_pylist(current_batch, schema=PARQUET_SCHEMA)
                        if writer is None:
                            writer = pq.ParquetWriter(
                                parquet_temp_path,
                                PARQUET_SCHEMA,
                                compression="zstd",
                                compression_level=compression_level,
                            )
                        writer.write_table(table)
                        total_records += len(current_batch)
                        current_batch = []

                if current_batch:
                    table = pa.Table.from_pylist(current_batch, schema=PARQUET_SCHEMA)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            parquet_temp_path,
                            PARQUET_SCHEMA,
                            compression="zstd",
                            compression_level=compression_level,
                        )
                    writer.write_table(table)
                    total_records += len(current_batch)
                    current_batch = []
    finally:
        if writer is not None:
            writer.close()

    if total_records == 0:
        raise ValueError("No valid rows were written to parquet")

    parquet_file = pq.ParquetFile(parquet_temp_path)
    parquet_rows = parquet_file.metadata.num_rows
    if parquet_rows != total_records:
        raise ValueError(f"Parquet validation failed: {parquet_rows} != {total_records}")

    parquet_bytes = parquet_temp_path.stat().st_size
    return ConversionStats(
        records=total_records,
        parse_errors=parse_errors,
        parquet_rows=parquet_rows,
        parquet_bytes=parquet_bytes,
    )


def process_task(
    task: DownloadTask,
    worker_id: int,
    batch_size: int,
    max_retries: int,
    stagger_delay: float,
    chunk_size_bytes: int,
    compression_level: int,
    allow_parse_errors: bool,
) -> TaskResult:
    """
    Выполняем одну задачу: скачивание, конвертация, atomic rename, manifest.

    params:
        task: Словарь задачи
        worker_id: Номер воркера для логов
        batch_size: Размер батча конвертации
        max_retries: Количество попыток
        stagger_delay: Случайная стартовая задержка
        chunk_size_bytes: Размер сетевого чанка
        compression_level: Уровень ZSTD
        allow_parse_errors: Разрешить ошибки парсинга
    return:
        Кортеж (успех, статус, статистика)
    """
    output_path = task.output_path
    manifest_path = task.manifest_path
    task_label = f"{task.symbol} {task.date}"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if stagger_delay > 0:
        delay = random.uniform(0, stagger_delay)
        log(f"  [W{worker_id}] {task_label} wait {delay:.1f}s")
        time.sleep(delay)

    session = get_session()
    temp_zip_path = None
    temp_output_path = output_path.with_name(f"{output_path.name}.tmp")

    for attempt in range(max_retries):
        try:
            start_time = time.time()
            log(f"  [W{worker_id}] {task_label} downloading...")

            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip", dir=output_path.parent) as temp_zip:
                temp_zip_path = Path(temp_zip.name)

            downloaded = 0
            with session.get(task.url, stream=True, timeout=120) as response:
                if response.status_code == 404:
                    log(f"  [W{worker_id}] {task_label} not found")
                    safe_unlink(temp_zip_path)
                    return TaskResult(ok=False, status="not_found")
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))
                with open(temp_zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=chunk_size_bytes):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                if total_size > 0 and downloaded != total_size:
                    raise IOError(f"Incomplete download: {downloaded}/{total_size}")

            download_time = time.time() - start_time
            zip_bytes = temp_zip_path.stat().st_size
            zip_mb = zip_bytes / 1024 / 1024
            log(f"  [W{worker_id}] {task_label} downloaded {zip_mb:.1f}MB in {download_time:.1f}s")

            safe_unlink(temp_output_path)
            convert_start = time.time()
            convert_stats = convert_zip_to_parquet(
                temp_zip_path,
                temp_output_path,
                batch_size=batch_size,
                compression_level=compression_level,
                allow_parse_errors=allow_parse_errors,
            )
            convert_time = time.time() - convert_start

            os.replace(temp_output_path, output_path)
            parquet_bytes = output_path.stat().st_size
            parquet_mb = parquet_bytes / 1024 / 1024

            manifest = {
                "manifest_version": MANIFEST_VERSION,
                "status": "success",
                "symbol": task.symbol,
                "market": task.market,
                "date": task.date,
                "depth": task.depth,
                "url": task.url,
                "zip_filename": task.zip_filename,
                "zip_bytes": zip_bytes,
                "parquet_bytes": parquet_bytes,
                "records": convert_stats.records,
                "parquet_rows": convert_stats.parquet_rows,
                "parse_errors": convert_stats.parse_errors,
                "allow_parse_errors": allow_parse_errors,
                "compression": "zstd",
                "compression_level": compression_level,
                "batch_size": batch_size,
                "chunk_size_bytes": chunk_size_bytes,
                "download_seconds": round(download_time, 3),
                "convert_seconds": round(convert_time, 3),
                "completed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            write_manifest_atomic(manifest_path, manifest)

            total_time = time.time() - start_time
            safe_unlink(temp_zip_path)
            log(
                f"  [W{worker_id}] {task_label} ✓ {parquet_mb:.1f}MB "
                f"({convert_stats.records:,} rows) in {total_time:.1f}s"
            )
            return TaskResult(
                ok=True,
                status="success",
                records=convert_stats.records,
                parquet_mb=parquet_mb,
                zip_mb=zip_mb,
                elapsed_seconds=total_time,
                parse_errors=convert_stats.parse_errors,
            )

        except requests.exceptions.Timeout:
            log(f"  [W{worker_id}] {task_label} timeout, retry {attempt + 1}/{max_retries}")
            time.sleep(5)
        except Exception as exc:
            log(f"  [W{worker_id}] {task_label} error: {str(exc)[:120]}, retry {attempt + 1}/{max_retries}")
            time.sleep(2)
        finally:
            safe_unlink(temp_zip_path)
            safe_unlink(temp_output_path)

    log(f"  [W{worker_id}] {task_label} ✗ failed after {max_retries} attempts")
    return TaskResult(ok=False, status="failed")


def prepare_tasks(
    symbols: list[str],
    market: str,
    depth: int,
    start: datetime,
    end: datetime,
    output_dir: Path,
    dry_run: bool,
    verify_existing: bool,
) -> PreparedTasks:
    """
    Готовим глобальный список задач по всем символам и датам.

    params:
        symbols: Список символов
        market: Рынок
        depth: Глубина стакана
        start: Начальная дата
        end: Конечная дата
        output_dir: Базовая директория
        dry_run: Только печатать задачи
        verify_existing: Проверять существующие parquet/manifest
    return:
        Словарь с задачами и счётчиками
    """
    tasks = []
    skipped = 0
    reprocess = 0

    for symbol in symbols:
        symbol_dir = output_dir / market / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        for date in daterange(start, end):
            task = build_task(symbol, market, depth, date, output_dir)

            if dry_run:
                print(f"  {task.url}")
                print(f"    → {task.output_path}")
                print(f"    → {task.manifest_path}")
                continue

            if task.output_path.exists():
                if verify_existing:
                    valid, reason = validate_existing_output(task)
                    if valid:
                        skipped += 1
                        continue
                    reprocess += 1
                    log(f"Reprocessing invalid existing output for {symbol} {task.date} ({reason})")
                else:
                    skipped += 1
                    continue

            tasks.append(task)

    return PreparedTasks(tasks=tasks, skipped=skipped, reprocess=reprocess)


def run_global_queue(
    tasks: list[DownloadTask],
    workers: int,
    output_dir: Path,
    min_disk_gb: float,
    batch_size: int,
    max_retries: int,
    stagger_delay: float,
    chunk_size_bytes: int,
    compression_level: int,
    allow_parse_errors: bool,
    skipped: int,
    reprocess: int,
) -> QueueStats:
    """
    Запускаем глобальную очередь задач по всем символам и датам.

    params:
        tasks: Список задач
        workers: Максимум параллельных задач
        output_dir: Базовая директория вывода
        min_disk_gb: Минимальный свободный диск
        batch_size: Размер батча
        max_retries: Количество retry
        stagger_delay: Случайная стартовая задержка
        chunk_size_bytes: Размер chunk при скачивании
        compression_level: Уровень ZSTD
        allow_parse_errors: Разрешить ошибки парсинга
        skipped: Уже пропущенные готовые файлы
        reprocess: Число невалидных существующих файлов
    return:
        Статистика выполнения
    """
    if not tasks:
        return QueueStats(skipped=skipped, reprocess=reprocess)

    free_gb = get_disk_free_gb(output_dir)
    log(f"Disk free: {free_gb:.1f} GB, Min required: {min_disk_gb} GB")
    if free_gb < min_disk_gb:
        log(f"⚠ Not enough disk space! Required: {min_disk_gb} GB")
        return QueueStats(skipped=skipped, reprocess=reprocess, disk_full=True)

    stats = QueueStats(skipped=skipped, reprocess=reprocess)

    log(f"Starting global queue: {len(tasks)} tasks, {workers} workers")
    task_iter = iter(tasks)
    submitted = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[Future[TaskResult], DownloadTask] = {}

        def submit_next() -> bool:
            nonlocal submitted
            if get_disk_free_gb(output_dir) < min_disk_gb:
                stats.disk_full = True
                return False

            try:
                task = next(task_iter)
            except StopIteration:
                return False

            worker_id = (submitted % workers) + 1
            future = executor.submit(
                process_task,
                task,
                worker_id,
                batch_size,
                max_retries,
                stagger_delay,
                chunk_size_bytes,
                compression_level,
                allow_parse_errors,
            )
            futures[future] = task
            submitted += 1
            return True

        while len(futures) < workers and submit_next():
            pass

        while futures:
            future = next(as_completed(futures))
            task = futures.pop(future)
            result = future.result()

            if result.ok:
                stats.success += 1
                stats.total_mb += result.parquet_mb
            elif result.status == "not_found":
                stats.not_found += 1
            else:
                stats.failed += 1

            completed = stats.success + stats.failed + stats.not_found
            remaining = len(tasks) - completed
            log(
                f"Progress: {completed}/{len(tasks)} done, "
                f"{remaining} remaining, {stats.total_mb:.1f} MB written"
            )

            if get_disk_free_gb(output_dir) < min_disk_gb:
                log(f"⚠ Stopping: disk space low ({get_disk_free_gb(output_dir):.1f} GB)")
                stats.disk_full = True
                executor.shutdown(wait=False, cancel_futures=True)
                futures.clear()
                break

            submit_next()

    return stats


def main() -> None:
    """
    Точка входа.

    params:
        None
    return:
        None
    """
    parser = argparse.ArgumentParser(
        description="Скачиваем Order Book из исторических архивов и сразу конвертируем в Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Скачать и конвертировать BTCUSDT USDT perpetuals за 5 дней
  python download_orderbook_stream.py BTCUSDT --start-date 2025-05-01 --end-date 2025-05-05

  # Глобальная очередь по нескольким символам
  python download_orderbook_stream.py --symbols BTCUSDT,ETHUSDT --market linear --start-date 2025-05-01 --end-date 2025-05-31 --workers 5
  python download_orderbook_stream.py --symbols-file pairs.txt --market linear --start-date 2025-05-01 --end-date 2025-05-31 --workers 5

  # Inverse perpetuals
  python download_orderbook_stream.py BTCUSD --market inverse --start-date 2025-05-01 --end-date 2025-05-05

  # Spot
  python download_orderbook_stream.py BTCUSDT --market spot --start-date 2025-05-01 --end-date 2025-05-05

  # Быстрая запись с более низким сжатием и крупными сетевыми чанками
  python download_orderbook_stream.py BTCUSDT --start-date 2025-05-01 --end-date 2025-05-31 --compression-level 3 --chunk-size-mb 4
        """
    )
    parser.add_argument("symbol", nargs="?", default=None,
                        help="Торговая пара (можно использовать --symbols)")
    parser.add_argument("--symbols", type=str, default=None,
                        help="Список пар через запятую: BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--symbols-file", type=str, default=None,
                        help="Файл со списком пар (по одной в строке или через запятую, максимум 30)")
    parser.add_argument("--market", type=str, choices=["linear", "inverse", "spot"],
                        default="linear",
                        help="Рынок Bybit: linear/inverse (derivatives) или spot")
    parser.add_argument("--depth", type=int, default=None,
                        help="Глубина стакана. По умолчанию: 500 для derivatives, 200 для spot")
    parser.add_argument("--start-date", type=str, required=True,
                        help="Начальная дата (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True,
                        help="Конечная дата (YYYY-MM-DD)")
    parser.add_argument("--output-dir", type=str, default="data/parquet/orderbook",
                        help="Директория для Parquet файлов")
    parser.add_argument("--workers", type=int, default=3,
                        help="Количество параллельных обработок во всей очереди")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help="Записей на один Parquet row group")
    parser.add_argument("--compression-level", type=int, default=DEFAULT_COMPRESSION_LEVEL,
                        help="Уровень ZSTD сжатия Parquet (быстрее: 3-6, плотнее: 9+)")
    parser.add_argument("--chunk-size-mb", type=int, default=DEFAULT_CHUNK_SIZE_MB,
                        help="Размер сетевого чанка при скачивании в МБ")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Количество попыток для одной задачи")
    parser.add_argument("--min-disk", type=float, default=50.0,
                        help="Минимальное свободное место на диске (ГБ)")
    parser.add_argument("--stagger", type=float, default=5.0,
                        help="Макс случайная задержка старта воркеров (сек)")
    parser.add_argument("--allow-parse-errors", action="store_true",
                        help="Не падать на единичных ошибках JSON парсинга")
    parser.add_argument("--no-verify-existing", action="store_true",
                        help="Не проверять существующие parquet/manifest перед пропуском")
    parser.add_argument("--dry-run", action="store_true",
                        help="Показать URL и пути без скачивания")

    args = parser.parse_args()

    try:
        symbols = resolve_symbols(args.symbol, args.symbols, args.symbols_file)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    depth = args.depth or DEFAULT_DEPTH_BY_MARKET[args.market]
    output_dir = Path(args.output_dir)
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1
    chunk_size_bytes = args.chunk_size_mb * 1024 * 1024

    if args.batch_size <= 0:
        parser.error("--batch-size must be > 0")
    if args.chunk_size_mb <= 0:
        parser.error("--chunk-size-mb must be > 0")
    if args.workers <= 0:
        parser.error("--workers must be > 0")

    print("=" * 60)
    print("Bybit Order Book Stream Downloader")
    print("=" * 60)
    print(f"Symbols:           {', '.join(symbols)}")
    print(f"Market:            {args.market}")
    print(f"Depth:             ob{depth}")
    print(f"Period:            {args.start_date} to {args.end_date} ({total_days} days)")
    print(f"Output:            {output_dir}")
    print(f"Workers:           {args.workers}")
    print(f"Batch size:        {args.batch_size}")
    print(f"Compression:       zstd level {args.compression_level}")
    print(f"Chunk size:        {args.chunk_size_mb} MB")
    print(f"Stagger:           {args.stagger}s")
    print(f"Min disk:          {args.min_disk} GB")
    print(f"Verify existing:   {not args.no_verify_existing}")
    print("=" * 60)

    start_time = time.time()
    prepared = prepare_tasks(
        symbols=symbols,
        market=args.market,
        depth=depth,
        start=start,
        end=end,
        output_dir=output_dir,
        dry_run=args.dry_run,
        verify_existing=not args.no_verify_existing,
    )

    if args.dry_run:
        return

    log(
        f"Planned tasks: {len(prepared.tasks)}, "
        f"skipped valid: {prepared.skipped}, "
        f"reprocess invalid: {prepared.reprocess}"
    )

    stats = run_global_queue(
        tasks=prepared.tasks,
        workers=args.workers,
        output_dir=output_dir,
        min_disk_gb=args.min_disk,
        batch_size=args.batch_size,
        max_retries=args.max_retries,
        stagger_delay=args.stagger,
        chunk_size_bytes=chunk_size_bytes,
        compression_level=args.compression_level,
        allow_parse_errors=args.allow_parse_errors,
        skipped=prepared.skipped,
        reprocess=prepared.reprocess,
    )

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    log(f"FINISHED in {elapsed/60:.1f} minutes")
    print(f"  Success:        {stats.success}")
    print(f"  Failed:         {stats.failed}")
    print(f"  Not found:      {stats.not_found}")
    print(f"  Skipped valid:  {stats.skipped}")
    print(f"  Reprocessed:    {stats.reprocess}")
    print(f"  Written:        {stats.total_mb:.1f} MB")
    print(f"  Disk free:      {get_disk_free_gb(output_dir):.1f} GB")
    if stats.disk_full:
        print("  Stopped:        disk threshold reached")
    print("=" * 60)


if __name__ == "__main__":
    main()
