"""
Test Technical Indicators
Chạy từng indicator riêng biệt để kiểm tra
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from datetime import datetime, timedelta
from technical_indicators import (
    calculate_sma,
    calculate_ema,
    calculate_rsi,
    calculate_macd,
    calculate_bollinger_bands,
    calculate_buy_sell_pressure
)

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("TestTechnicalIndicators") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("🧪 TEST TECHNICAL INDICATORS")
print("=" * 80)

# ======================================================
# TẠO DỮ LIỆU MẪU
# ======================================================
print("\n📊 Creating sample data...")

# Tạo dữ liệu ticker giả
base_time = datetime(2024, 11, 15, 10, 0, 0)
data = []

for i in range(100):
    time = base_time + timedelta(hours=i)
    price = float(40000 + (i * 100) + ((i % 10) * 50))  # Giá dao động
    
    data.append({
        "Symbol": "BTCUSDT",
        "CloseTime": time,
        "Open": float(price - 50),
        "High": float(price + 100),
        "Low": float(price - 100),
        "Close": float(price),
        "BaseVolume": float(100.0 + i),
        "QuoteVolume": float((100.0 + i) * price),
        "Year": 2024,
        "Month": 11,
        "Day": 15
    })

schema = StructType([
    StructField("Symbol", StringType()),
    StructField("CloseTime", TimestampType()),
    StructField("Open", DoubleType()),
    StructField("High", DoubleType()),
    StructField("Low", DoubleType()),
    StructField("Close", DoubleType()),
    StructField("BaseVolume", DoubleType()),
    StructField("QuoteVolume", DoubleType()),
    StructField("Year", IntegerType()),
    StructField("Month", IntegerType()),
    StructField("Day", IntegerType())
])

df = spark.createDataFrame(data, schema)
print(f"✅ Created {df.count()} sample records")

# ======================================================
# TEST 1: SMA (Simple Moving Average)
# ======================================================
print("\n" + "=" * 80)
print("TEST 1: SMA (Simple Moving Average)")
print("=" * 80)

df_sma = calculate_sma(df, column="Close", period=20)
df_sma_result = df_sma.select("Symbol", "CloseTime", "Close", "SMA_20").orderBy("CloseTime")

print("\n📈 SMA Results (first 10 rows):")
df_sma_result.show(10, truncate=False)

sma_count = df_sma_result.filter(col("SMA_20").isNotNull()).count()
print(f"✅ SMA calculated for {sma_count} rows (expected >= {100-20+1})")

# ======================================================
# TEST 2: EMA (Exponential Moving Average)
# ======================================================
print("\n" + "=" * 80)
print("TEST 2: EMA (Exponential Moving Average)")
print("=" * 80)

df_ema = calculate_ema(df, column="Close", period=12)
df_ema_result = df_ema.select("Symbol", "CloseTime", "Close", "EMA_12").orderBy("CloseTime")

print("\n📈 EMA Results (first 10 rows):")
df_ema_result.show(10, truncate=False)

ema_count = df_ema_result.filter(col("EMA_12").isNotNull()).count()
print(f"✅ EMA calculated for {ema_count} rows")

# ======================================================
# TEST 3: RSI (Relative Strength Index)
# ======================================================
print("\n" + "=" * 80)
print("TEST 3: RSI (Relative Strength Index)")
print("=" * 80)

df_rsi = calculate_rsi(df, column="Close", period=14)
df_rsi_result = df_rsi.select("Symbol", "CloseTime", "Close", "RSI_14").orderBy("CloseTime")

print("\n📈 RSI Results (last 10 rows):")
df_rsi_result.tail(10)
df_rsi_result.orderBy(col("CloseTime").desc()).show(10, truncate=False)

rsi_stats = df_rsi_result.filter(col("RSI_14").isNotNull()).selectExpr(
    "min(RSI_14) as min_rsi",
    "max(RSI_14) as max_rsi",
    "avg(RSI_14) as avg_rsi"
)
print("\n📊 RSI Statistics:")
rsi_stats.show()

rsi_count = df_rsi_result.filter(col("RSI_14").isNotNull()).count()
print(f"✅ RSI calculated for {rsi_count} rows (should be 0-100)")

# ======================================================
# TEST 4: MACD
# ======================================================
print("\n" + "=" * 80)
print("TEST 4: MACD (Moving Average Convergence Divergence)")
print("=" * 80)

df_macd = calculate_macd(df, column="Close", fast=12, slow=26, signal=9)
df_macd_result = df_macd.select(
    "Symbol", "CloseTime", "Close", 
    "MACD_Line", "MACD_Signal", "MACD_Histogram"
).orderBy("CloseTime")

print("\n📈 MACD Results (last 10 rows):")
df_macd_result.orderBy(col("CloseTime").desc()).show(10, truncate=False)

macd_count = df_macd_result.filter(col("MACD_Line").isNotNull()).count()
print(f"✅ MACD calculated for {macd_count} rows")

# ======================================================
# TEST 5: Bollinger Bands
# ======================================================
print("\n" + "=" * 80)
print("TEST 5: Bollinger Bands")
print("=" * 80)

df_bb = calculate_bollinger_bands(df, column="Close", period=20, std_dev=2)
df_bb_result = df_bb.select(
    "Symbol", "CloseTime", "Close",
    "BB_Upper", "BB_Middle", "BB_Lower"
).orderBy("CloseTime")

print("\n📈 Bollinger Bands Results (last 10 rows):")
df_bb_result.orderBy(col("CloseTime").desc()).show(10, truncate=False)

bb_count = df_bb_result.filter(col("BB_Upper").isNotNull()).count()
print(f"✅ Bollinger Bands calculated for {bb_count} rows")

# ======================================================
# TEST 6: Buy/Sell Pressure (Trades data)
# ======================================================
print("\n" + "=" * 80)
print("TEST 6: Buy/Sell Pressure")
print("=" * 80)

# Tạo dữ liệu trades mẫu
trades_data = []
for i in range(50):
    time = base_time + timedelta(seconds=i*10)
    side = "BUY" if i % 2 == 0 else "SELL"
    price = float(40000 + (i * 10))
    qty = float(0.1 + (i * 0.01))
    
    trades_data.append({
        "Symbol": "BTCUSDT",
        "TradeTime": time,
        "Price": price,
        "Quantity": qty,
        "Side": side,
        "TradeValue": float(price * qty)
    })

trades_schema = StructType([
    StructField("Symbol", StringType()),
    StructField("TradeTime", TimestampType()),
    StructField("Price", DoubleType()),
    StructField("Quantity", DoubleType()),
    StructField("Side", StringType()),
    StructField("TradeValue", DoubleType())
])

trades_df = spark.createDataFrame(trades_data, trades_schema)
print(f"✅ Created {trades_df.count()} sample trades")

# Note: calculate_buy_sell_pressure sử dụng streaming watermark
# Nên cần test riêng trong streaming context
print("\n⚠️  Buy/Sell Pressure requires streaming context")
print("    Function available: calculate_buy_sell_pressure()")
print("    Test in spark_processing.py")

# ======================================================
# TEST SUMMARY - ALL INDICATORS TOGETHER
# ======================================================
print("\n" + "=" * 80)
print("🎯 FULL TEST: All Indicators Together")
print("=" * 80)

df_all = df
df_all = calculate_sma(df_all, period=20)
df_all = calculate_sma(df_all, period=50)
df_all = calculate_ema(df_all, period=12)
df_all = calculate_ema(df_all, period=26)
df_all = calculate_rsi(df_all, period=14)
df_all = calculate_macd(df_all)
df_all = calculate_bollinger_bands(df_all)

final_result = df_all.select(
    "Symbol", "CloseTime", "Close",
    "SMA_20", "SMA_50",
    "EMA_12", "EMA_26",
    "RSI_14",
    "MACD_Line", "MACD_Signal", "MACD_Histogram",
    "BB_Upper", "BB_Middle", "BB_Lower"
).orderBy("CloseTime")

print("\n📊 All Indicators (last 5 rows):")
final_result.orderBy(col("CloseTime").desc()).show(5, truncate=False)

# Kiểm tra null values
null_counts = {}
for column in final_result.columns:
    if column not in ["Symbol", "CloseTime", "Close"]:
        null_count = final_result.filter(col(column).isNull()).count()
        null_counts[column] = null_count

print("\n📈 Null Value Analysis:")
for indicator, null_count in sorted(null_counts.items()):
    status = "✅" if null_count < 50 else "⚠️"
    print(f"{status} {indicator}: {null_count} nulls / {final_result.count()} total")

# ======================================================
# FINAL SUMMARY
# ======================================================
print("\n" + "=" * 80)
print("✅ TEST COMPLETED!")
print("=" * 80)

test_results = {
    "SMA": sma_count > 0,
    "EMA": ema_count > 0,
    "RSI": rsi_count > 0,
    "MACD": macd_count > 0,
    "Bollinger Bands": bb_count > 0
}

print("\n📊 Test Results:")
for indicator, passed in test_results.items():
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"  {status} - {indicator}")

all_passed = all(test_results.values())
print("\n" + "=" * 80)
if all_passed:
    print("🎉 ALL TESTS PASSED!")
else:
    print("⚠️  SOME TESTS FAILED")
print("=" * 80)

spark.stop()
