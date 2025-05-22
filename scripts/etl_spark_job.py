import pandas as pd
import os
import logging
from sklearn.preprocessing import StandardScaler

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/usr/local/airflow/data/logs/etl_spark_job.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('etl_spark_job')

BRONZE_PATH = "/usr/local/airflow/data/bronze"
SILVER_PATH = "/usr/local/airflow/data/silver/diabetes_processed.csv"

COLUMNS = [
    "Pregnancies", "Glucose", "BloodPressure", "SkinThickness",
    "Insulin", "BMI", "DiabetesPedigreeFunction", "Age", "Outcome"
]
NUMERIC_COLS = ["Glucose", "BloodPressure", "SkinThickness", "Insulin", "BMI"]


def load_data():
    """Load and concatenate all Pima CSV files from bronze zone."""
    logger.info("Loading data from bronze zone with pandas")
    pima_files = [f for f in os.listdir(BRONZE_PATH) if f.startswith("pima_") and f.endswith(".csv")]
    if not pima_files:
        logger.error("No pima diabetes data files found in bronze zone")
        return None
    logger.info(f"Found files: {pima_files}")
    dfs = []
    for f in pima_files:
        try:
            df = pd.read_csv(os.path.join(BRONZE_PATH, f), header=None, dtype=str)
            dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to read {f}: {e}")
    df = pd.concat(dfs, ignore_index=True)
    df.columns = COLUMNS
    # Convert columns to numeric, coerce errors to NaN
    for col in COLUMNS:
        if col != "DiabetesPedigreeFunction":
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info(f"Data loaded successfully. Row count: {len(df)}")
    return df


def clean_data(df):
    """Clean data: impute missing, drop duplicates, filter invalid."""
    logger.info("Cleaning data")
    logger.info(f"Row count before cleaning: {len(df)}")
    logger.info("Missing value statistics:")
    logger.info(df.isnull().sum())
    # Impute missing values with mean for numeric columns
    for col_name in NUMERIC_COLS:
        df[col_name] = df[col_name].fillna(df[col_name].mean())
    df = df.drop_duplicates()
    df = df[(df["Glucose"] > 0) & (df["BloodPressure"] > 0) & (df["BMI"] > 0)]
    logger.info(f"Row count after cleaning: {len(df)}")
    return df


def transform_features(df):
    """Feature engineering and scaling."""
    logger.info("Transforming features")
    # BMI Category
    df["BMI_Category"] = pd.cut(
        df["BMI"],
        bins=[-float('inf'), 18.5, 25, 30, float('inf')],
        labels=[0, 1, 2, 3]
    ).astype(int)
    feature_cols = [
        "Pregnancies", "Glucose", "BloodPressure", "SkinThickness", "Insulin",
        "BMI", "DiabetesPedigreeFunction", "Age", "BMI_Category"
    ]
    scaler = StandardScaler()
    df[feature_cols] = scaler.fit_transform(df[feature_cols])
    logger.info("Feature transformation and scaling complete")
    return df


def save_to_silver(df):
    """Save processed data to silver zone as CSV."""
    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)
    df.to_csv(SILVER_PATH, index=False)
    logger.info(f"Data saved to silver zone: {SILVER_PATH}")


def main():
    logger.info("Starting ETL process (pandas)")
    try:
        df = load_data()
        if df is None:
            logger.error("No data available for ETL")
            return
        df = clean_data(df)
        df = transform_features(df)
        save_to_silver(df)
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

