# Phase 3: Processing - Xử lý dữ liệu

## ✅ Hoàn thành tất cả nhiệm vụ Phase 3

### 📋 Tổng quan

| Nhiệm vụ | File | Trạng thái |
|----------|------|-----------|
| 1. Clean & Transform | `spark_processing.py` | ✅ DONE |
| 2. Batch → Parquet/Delta | `spark_processing.py` | ✅ DONE |
| 3. Technical Indicators | `technical_indicators.py` | ✅ DONE |
| 4. Orderbook Statistics | `spark_processing.py` | ✅ DONE |

---

## 1️⃣ Clean & Transform

### Features:
- ✅ Convert timestamp (ms → datetime)
- ✅ Validate JSON (filter null data)
- ✅ Cast types (string → double)
- ✅ Validate values (price > 0, quantity > 0)
- ✅ Add partition columns (Year, Month, Day, Hour)
- ✅ Calculate derived fields (TradeValue, Side)

### Code:
```python
trades_cleaned_df = (
    trades_raw_df
    .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
    .filter(col("data").isNotNull())  # ✅ Validate JSON
    .select(...)
    .filter(col("Price") > 0)  # ✅ Validate price
    .filter(col("Quantity") > 0)  # ✅ Validate quantity
)
```

---

## 2️⃣ Batch → Parquet (Partitioned)

### Output Structure:
```
/data/processed/
├── trades/
│   ├── Symbol=BTCUSDT/
│   │   ├── Year=2024/
│   │   │   ├── Month=11/
│   │   │   │   ├── Day=15/
│   │   │   │   │   ├── Hour=10/
│   │   │   │   │   │   └── part-00000.parquet
│   │   │   │   │   ├── Hour=11/
│   │   │   │   │   └── ...
├── tickers/
│   └── Symbol=BTCUSDT/Year=2024/Month=11/Day=15/...
├── orderbook_stats/
│   └── Symbol=BTCUSDT/Year=2024/Month=11/Day=15/...
└── tickers_with_indicators/
    └── Symbol=BTCUSDT/Year=2024/Month=11/Day=15/...
```

### Partitioning:
- **Trades**: `Symbol`, `Year`, `Month`, `Day`, `Hour`
- **Tickers**: `Symbol`, `Year`, `Month`, `Day`
- **Orderbook**: `Symbol`, `Year`, `Month`, `Day`

---

## 3️⃣ Technical Indicators

### Indicators Implemented:

| Indicator | Function | Description |
|-----------|----------|-------------|
| **SMA** | `calculate_sma()` | Simple Moving Average (20, 50 periods) |
| **EMA** | `calculate_ema()` | Exponential Moving Average (12, 26 periods) |
| **RSI** | `calculate_rsi()` | Relative Strength Index (14 period) |
| **MACD** | `calculate_macd()` | MACD Line, Signal, Histogram |
| **Bollinger Bands** | `calculate_bollinger_bands()` | Upper, Middle, Lower bands |
| **Buy/Sell Pressure** | `calculate_buy_sell_pressure()` | From trades volume |

### Usage:
```python
# Run batch processing
spark-submit app/technical_indicators.py

# Or import functions
from technical_indicators import calculate_rsi, calculate_macd

df_with_rsi = calculate_rsi(tickers_df, period=14)
df_with_macd = calculate_macd(tickers_df)
```

### Output Columns:
```
Symbol, CloseTime, Open, High, Low, Close, Volume,
SMA_20, SMA_50,
EMA_12, EMA_26,
RSI_14,
MACD_Line, MACD_Signal, MACD_Histogram,
BB_Upper, BB_Middle, BB_Lower,
Year, Month, Day
```

---

## 4️⃣ Orderbook Statistics

### Metrics Calculated:

| Metric | Description |
|--------|-------------|
| **TotalQty** | Sum of quantities at each side (BID/ASK) |
| **AvgPrice** | Average price across all levels |
| **NumLevels** | Number of price levels |
| **OrderImbalance** | (BidValue - AskValue) / TotalValue |
| **ImbalanceLabel** | BUY_PRESSURE / SELL_PRESSURE / NEUTRAL |

### Window Aggregation:
- Window: **5 seconds**
- Watermark: **10 seconds**

### Code:
```python
orderbook_stats_df = (
    orderbook_df
    .withWatermark("EventTime", "10 seconds")
    .groupBy(
        window(col("EventTime"), "5 seconds"),
        col("Symbol"),
        col("Side")
    )
    .agg(
        _sum("Qty").alias("TotalQty"),
        avg("Price").alias("AvgPrice"),
        count("*").alias("NumLevels")
    )
)
```

---

## 🚀 Chạy Processing

### 1. Streaming Processing (Real-time)
```bash
# Trong docker-compose
sudo docker compose up spark-submit

# Hoặc thủ công
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  app/spark_processing.py
```

### 2. Batch Processing (Technical Indicators)
```bash
spark-submit \
  --master spark://spark-master:7077 \
  app/technical_indicators.py
```

---

## 📊 Monitoring

### Check Logs:
```bash
sudo docker compose logs -f spark-submit
```

### Check Output Files:
```bash
ls -lah data/processed/trades/
ls -lah data/processed/tickers_with_indicators/
```

### Spark UI:
- **Master**: http://localhost:8080
- **Worker**: http://localhost:8081

---

## 🎯 Summary

✅ **Phase 3 COMPLETE!**

- ✅ Clean & Transform với validation đầy đủ
- ✅ Batch processing → Parquet với partitioning
- ✅ 6 Technical Indicators (SMA, EMA, RSI, MACD, BB, Pressure)
- ✅ Orderbook statistics với window aggregation
- ✅ Ready for Phase 4 (Storage & Analytics)

---

## 📖 Next Steps

Phase 4: Storage & Analytics
- Ghi vào ClickHouse
- Query & Analysis
- Dashboard integration
