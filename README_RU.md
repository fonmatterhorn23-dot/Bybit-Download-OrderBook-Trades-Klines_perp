# Bybit Market Data Downloader

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Bybit](https://img.shields.io/badge/Exchange-Bybit-orange.svg)](https://www.bybit.com/)

CLI-инструменты для скачивания исторических данных **Derivatives** и Spot с Bybit. API ключи не нужны.

[Русская версия](README_RU.md) | [English](README.md)

---

## 🚀 Возможности

- **📊 Order Book** — Derivatives (linear/inverse, 500 уровней) и Spot (200 уровней)
- **💹 Trades** — Тиковые данные сделок
- **📈 Klines** — Spot и Futures через API v5
- **🗜️ Parquet Streaming** — Скачивание и конвертация за один шаг, экономия ~22% места
- **🔒 Атомарная запись** — Защита от прерываний
- **🔄 Retry** — Устойчивость к сбоям сети
- **💾 Защита диска** — Автоостановка при нехватке места

## 📦 Установка

```bash
git clone https://github.com/nssanta/Bybit-Download-OrderBook-Trades-Klines.git
cd Bybit-Download-OrderBook-Trades-Klines
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 📖 Использование

### Order Book (Streaming — Рекомендуется)
Скачивание и конвертация в Parquet за один шаг. Экономит ~22% места.

```bash
# Один derivatives символ (USDT perpetuals, рекомендуется: 3 воркера, 10с задержка)
python scripts/download_orderbook_stream.py BTCUSDT --start-date 2025-05-01 --end-date 2025-05-31 --workers 3 --stagger 10

# Несколько derivatives символов
python scripts/download_orderbook_stream.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --market linear --start-date 2025-05-01 --end-date 2025-05-31 --workers 3

# Много символов из файла (до 30, глобальная очередь по всем symbol/date задачам)
python scripts/download_orderbook_stream.py --symbols-file pairs.txt --market linear --start-date 2025-05-01 --end-date 2025-05-31 --workers 5

# Inverse perpetuals
python scripts/download_orderbook_stream.py BTCUSD --market inverse --start-date 2025-05-01 --end-date 2025-05-31 --workers 3

# Spot market
python scripts/download_orderbook_stream.py BTCUSDT --market spot --start-date 2025-05-01 --end-date 2025-05-31 --workers 3

# С порогом свободного места (остановка если < 100 ГБ)
python scripts/download_orderbook_stream.py BTCUSDT --start-date 2025-05-01 --end-date 2025-12-31 --min-disk 100

# Быстрые backfill: ниже ZSTD уровень и крупнее сетевые чанки
python scripts/download_orderbook_stream.py --symbols-file pairs.txt --market linear --start-date 2025-05-01 --end-date 2025-05-31 --workers 5 --compression-level 3 --chunk-size-mb 4
```

**Флаги:**
- `--workers N` — глобальные параллельные загрузки по всем symbol/date задачам (рекомендуется: 3-5, больше может вызвать таймауты)
- `--stagger N` — случайная задержка 0-N секунд перед стартом каждого воркера
- `--min-disk N` — остановка если места на диске меньше N ГБ
- `--market linear|inverse|spot` — архив рынка для скачивания (`linear` по умолчанию для USDT perpetuals)
- `--depth N` — глубина стакана (`500` для derivatives, `200` для spot по умолчанию)
- `--symbols-file PATH` — чтение до 30 символов из текстового файла, по одному в строке или через запятую/пробел; комментарии `#` игнорируются
- `--batch-size N` — размер Parquet row group (`50000` по умолчанию)
- `--compression-level N` — уровень ZSTD для Parquet (`6` по умолчанию; `3` быстрее для backfill, выше — меньше файлы)
- `--chunk-size-mb N` — размер HTTP чанка в МБ (`1` по умолчанию)
- `--max-retries N` — число попыток на одну symbol/date задачу (`3` по умолчанию)
- `--allow-parse-errors` — продолжать запись при ошибках отдельных JSON строк (по умолчанию fail-fast)
- `--no-verify-existing` — пропускать существующие Parquet без проверки manifest и row count

Streaming downloader пишет Parquet атомарно и создаёт `.manifest.json` рядом с каждым файлом. При resume существующие файлы пропускаются только если manifest, размер файла, row count, symbol, market, date, depth и URL совпадают.

### Order Book (Legacy — только ZIP)
Скачивание ZIP архивов без конвертации.

```bash
python scripts/download_orderbook.py BTCUSDT --market linear --start-date 2025-05-01 --end-date 2025-05-31
python scripts/download_orderbook.py --symbols-file pairs.txt --market linear --start-date 2025-05-01 --end-date 2025-05-31
```

### Конвертация ZIP в Parquet
Конвертация ранее скачанных ZIP архивов.

```bash
python scripts/convert_to_parquet.py --input data/raw/orderbook/linear/BTCUSDT --output data/parquet/orderbook/linear/BTCUSDT
```

### Скачать Trades
```bash
python scripts/download_trades.py BTCUSDT --start-date 2025-05-01 --end-date 2025-05-31
```

### Klines (API)
Скачивание Spot или Futures свечей через API.

```bash
# Spot Market
python scripts/download_klines.py BTCUSDT --source spot --start-date 2025-01-01 --end-date 2025-01-31 --interval 1

# Futures Market
python scripts/download_klines.py BTCUSDT --source linear --start-date 2025-01-01 --end-date 2025-01-31 --interval 60
```

## 📁 Структура данных

```
data/
├── raw/
│   ├── orderbook/linear/BTCUSDT/  # Derivatives ZIP архивы (legacy)
│   ├── orderbook/inverse/BTCUSD/  # Inverse derivatives ZIP архивы (legacy)
│   ├── orderbook/spot/BTCUSDT/    # Spot ZIP архивы (legacy)
│   └── trades/BTCUSDT/         # CSV.gz файлы
├── parquet/
│   └── orderbook/linear/BTCUSDT/  # Parquet файлы (рекомендуется)
└── klines/
    ├── spot/BTCUSDT/           # Spot свечи
    └── futures/BTCUSDT/        # Futures свечи
```

## 📋 Форматы и размеры данных

| Тип | Источник | Сырой формат | Parquet | Размер/день |
|-----|----------|--------------|---------|-------------|
| Order Book | quote-saver.bycsi.com | ZIP (JSON, зависит от рынка/глубины) | ZSTD | **65-200+ МБ** |
| Trades | public.bybit.com/spot | CSV.gz | — | ~5-50 МБ |
| Klines | Bybit API v5 | — | ZSTD | ~1-5 МБ |

### Схема Order Book Parquet

| Колонка | Тип | Описание |
|---------|-----|----------|
| ts | int64 | Серверный timestamp (мс) |
| cts | int64 | Клиентский timestamp (мс) |
| type | string | `snapshot` или `delta` |
| u | int64 | Update ID |
| seq | int64 | Порядковый номер |
| bids | string | JSON массив `[["price", "qty"], ...]` |
| asks | string | JSON массив `[["price", "qty"], ...]` |

## ⏰ Доступность данных

| Тип данных | Доступно с |
|------------|------------|
| Order Book | Май 2025 |
| Trades | 2020 |

## ⚠️ Важные замечания

- **Используй Streaming для Order Book**: `download_orderbook_stream.py` экономит место.
- **Рынок по умолчанию — derivatives**: `linear` скачивает USDT perpetuals со страницы исторических данных Bybit derivatives. Используй `--market spot`, только если нужны spot архивы.
- **Внимание к размерам**: Order Book данные большие! Derivatives `ob500` архивы больше, чем spot `ob200`.
- **Проверяй здоровье диска**: Установи `smartmontools` и запускай `sudo smartctl -a /dev/sdX`.

## 🔧 Проверка здоровья диска (Linux)

```bash
# Установка smartmontools
sudo apt install smartmontools

# Проверка здоровья диска
sudo smartctl -a /dev/sda
```

## 📄 Лицензия

MIT
