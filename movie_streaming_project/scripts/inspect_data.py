
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")

csv_files = sorted(RAW_DIR.glob("*.csv"))

for file in csv_files:
    print("\n" + "=" * 60)
    print(f"DATASET: {file.name}")
    print("=" * 60)

    df = pd.read_csv(file)

    print("\nFirst 5 records:")
    print(df.head())

    print("\nDataset shape:")
    print("Rows:", df.shape[0])
    print("Columns:", df.shape[1])

    print("\nColumn data types:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isnull().sum())

    print("\nDuplicate rows:")
    print(df.duplicated().sum())

    print("\nUnique values per column:")
    print(df.nunique())
