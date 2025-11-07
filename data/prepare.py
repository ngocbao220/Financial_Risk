import pandas as pd
SAVE_PATH = "aggregated_risk.csv"
df = pd.read_parquet("hf://datasets/sovai/corp_risks/corp_risks.parquet")
df.to_csv(SAVE_PATH)
