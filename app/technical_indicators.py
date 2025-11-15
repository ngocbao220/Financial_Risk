from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, lag, when, lit
from pyspark.sql.window import Window

# ======================================================
# TECHNICAL INDICATORS CALCULATOR
# ======================================================

def calculate_sma(df, column="Close", period=20):
    """Simple Moving Average"""
    window_spec = Window.partitionBy("Symbol").orderBy("CloseTime").rowsBetween(-period + 1, 0)
    return df.withColumn(f"SMA_{period}", avg(col(column)).over(window_spec))

def calculate_ema(df, column="Close", period=12):
    """Exponential Moving Average (approximation)"""
    # EMA = Price * K + EMA(previous) * (1-K)
    # K = 2 / (period + 1)
    k = 2.0 / (period + 1)
    
    window_spec = Window.partitionBy("Symbol").orderBy("CloseTime")
    
    # Start with SMA as initial EMA
    df = calculate_sma(df, column, period)
    df = df.withColumn(f"EMA_{period}_prev", lag(f"SMA_{period}", 1).over(window_spec))
    
    df = df.withColumn(
        f"EMA_{period}",
        when(
            col(f"EMA_{period}_prev").isNotNull(),
            col(column) * k + col(f"EMA_{period}_prev") * (1 - k)
        ).otherwise(col(f"SMA_{period}"))
    )
    
    return df.drop(f"EMA_{period}_prev")

def calculate_rsi(df, column="Close", period=14):
    """Relative Strength Index"""
    window_spec = Window.partitionBy("Symbol").orderBy("CloseTime")
    
    # Calculate price changes
    df = df.withColumn("PriceChange", col(column) - lag(col(column), 1).over(window_spec))
    
    # Separate gains and losses
    df = df.withColumn("Gain", when(col("PriceChange") > 0, col("PriceChange")).otherwise(0))
    df = df.withColumn("Loss", when(col("PriceChange") < 0, -col("PriceChange")).otherwise(0))
    
    # Calculate average gain and loss
    gain_window = Window.partitionBy("Symbol").orderBy("CloseTime").rowsBetween(-period + 1, 0)
    df = df.withColumn("AvgGain", avg("Gain").over(gain_window))
    df = df.withColumn("AvgLoss", avg("Loss").over(gain_window))
    
    # Calculate RS and RSI
    df = df.withColumn("RS", col("AvgGain") / col("AvgLoss"))
    df = df.withColumn(
        f"RSI_{period}",
        100 - (100 / (1 + col("RS")))
    )
    
    return df.drop("PriceChange", "Gain", "Loss", "AvgGain", "AvgLoss", "RS")

def calculate_macd(df, column="Close", fast=12, slow=26, signal=9):
    """Moving Average Convergence Divergence"""
    # Calculate EMAs
    df = calculate_ema(df, column, fast)
    df = calculate_ema(df, column, slow)
    
    # MACD Line = EMA(fast) - EMA(slow)
    df = df.withColumn("MACD_Line", col(f"EMA_{fast}") - col(f"EMA_{slow}"))
    
    # Signal Line = EMA of MACD Line
    window_spec = Window.partitionBy("Symbol").orderBy("CloseTime").rowsBetween(-signal + 1, 0)
    df = df.withColumn("MACD_Signal", avg("MACD_Line").over(window_spec))
    
    # MACD Histogram = MACD Line - Signal Line
    df = df.withColumn("MACD_Histogram", col("MACD_Line") - col("MACD_Signal"))
    
    return df

def calculate_bollinger_bands(df, column="Close", period=20, std_dev=2):
    """Bollinger Bands"""
    window_spec = Window.partitionBy("Symbol").orderBy("CloseTime").rowsBetween(-period + 1, 0)
    
    df = df.withColumn("BB_Middle", avg(col(column)).over(window_spec))
    df = df.withColumn("BB_StdDev", stddev(col(column)).over(window_spec))
    
    df = df.withColumn("BB_Upper", col("BB_Middle") + (col("BB_StdDev") * std_dev))
    df = df.withColumn("BB_Lower", col("BB_Middle") - (col("BB_StdDev") * std_dev))
    
    return df.drop("BB_StdDev")

def calculate_buy_sell_pressure(trades_df):
    """Calculate Buy/Sell Pressure from Trades"""
    from pyspark.sql.functions import sum as _sum, window
    
    trades_with_window = trades_df.withWatermark("TradeTime", "1 minute")
    
    pressure_df = (
        trades_with_window
        .groupBy(
            window(col("TradeTime"), "1 minute"),
            col("Symbol")
        )
        .agg(
            _sum(when(col("Side") == "BUY", col("TradeValue")).otherwise(0)).alias("BuyVolume"),
            _sum(when(col("Side") == "SELL", col("TradeValue")).otherwise(0)).alias("SellVolume"),
            _sum("TradeValue").alias("TotalVolume")
        )
        .select(
            col("window.start").alias("WindowStart"),
            col("window.end").alias("WindowEnd"),
            col("Symbol"),
            col("BuyVolume"),
            col("SellVolume"),
            col("TotalVolume"),
            (col("BuyVolume") / col("TotalVolume") * 100).alias("BuyPressure_Pct"),
            (col("SellVolume") / col("TotalVolume") * 100).alias("SellPressure_Pct")
        )
    )
    
    return pressure_df

def calculate_order_imbalance(orderbook_df):
    """Calculate Order Imbalance from Orderbook"""
    from pyspark.sql.functions import sum as _sum
    
    # Aggregate bids and asks separately
    imbalance_df = (
        orderbook_df
        .groupBy("Symbol", "EventTime")
        .agg(
            _sum(when(col("Side") == "BID", col("Qty") * col("Price")).otherwise(0)).alias("BidValue"),
            _sum(when(col("Side") == "ASK", col("Qty") * col("Price")).otherwise(0)).alias("AskValue")
        )
        .withColumn("TotalValue", col("BidValue") + col("AskValue"))
        .withColumn(
            "OrderImbalance",
            (col("BidValue") - col("AskValue")) / col("TotalValue")
        )
        .withColumn(
            "ImbalanceLabel",
            when(col("OrderImbalance") > 0.1, "BUY_PRESSURE")
            .when(col("OrderImbalance") < -0.1, "SELL_PRESSURE")
            .otherwise("NEUTRAL")
        )
    )
    
    return imbalance_df

# ======================================================
# BATCH PROCESSING MAIN
# ======================================================
def process_batch_indicators(spark, parquet_path):
    """Process saved Parquet data and add technical indicators"""
    
    print("📊 Loading ticker data from Parquet...")
    tickers_df = spark.read.parquet(f"{parquet_path}/tickers")
    
    print("🔧 Calculating Technical Indicators...")
    
    # Calculate all indicators
    tickers_with_indicators = tickers_df
    tickers_with_indicators = calculate_sma(tickers_with_indicators, period=20)
    tickers_with_indicators = calculate_sma(tickers_with_indicators, period=50)
    tickers_with_indicators = calculate_ema(tickers_with_indicators, period=12)
    tickers_with_indicators = calculate_ema(tickers_with_indicators, period=26)
    tickers_with_indicators = calculate_rsi(tickers_with_indicators, period=14)
    tickers_with_indicators = calculate_macd(tickers_with_indicators)
    tickers_with_indicators = calculate_bollinger_bands(tickers_with_indicators)
    
    print("💾 Saving enriched data...")
    (
        tickers_with_indicators
        .write
        .mode("overwrite")
        .partitionBy("Symbol", "Year", "Month", "Day")
        .parquet(f"{parquet_path}/tickers_with_indicators")
    )
    
    print("✅ Technical indicators calculated and saved!")
    
    return tickers_with_indicators

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TechnicalIndicators") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    OUTPUT_PATH = "/data/processed"
    
    # Process batch
    result_df = process_batch_indicators(spark, OUTPUT_PATH)
    
    # Show sample
    result_df.select(
        "Symbol", "CloseTime", "Close", 
        "SMA_20", "RSI_14", "MACD_Line", "MACD_Signal",
        "BB_Upper", "BB_Lower"
    ).show(10, truncate=False)
    
    spark.stop()
