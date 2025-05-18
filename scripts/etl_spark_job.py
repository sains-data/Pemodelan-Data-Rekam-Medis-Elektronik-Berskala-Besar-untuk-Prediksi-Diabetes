from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.sql.functions import col, when, isnan, count, min, max, avg, stddev
import os
import logging

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/data/logs/etl_spark_job.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('etl_spark_job')

def create_spark_session():
    """Inisialisasi SparkSession dengan konfigurasi yang optimal"""
    logger.info("Inisialisasi Spark Session")
    return (SparkSession.builder
            .appName("ETL-Diabetes-Pipeline")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.default.parallelism", "10")
            .getOrCreate())

def load_data(spark):
    """Load data dari bronze zone"""
    logger.info("Loading data dari bronze zone")
    
    try:
        # Load dataset utama
        logger.info("Loading dataset Pima diabetes")
        pima = spark.read.csv("/data/bronze/pima_*.csv", 
                            header=False, 
                            inferSchema=True)
        
        # Rename kolom untuk kemudahan
        columns = ["Pregnancies", "Glucose", "BloodPressure", "SkinThickness", 
                "Insulin", "BMI", "DiabetesPedigreeFunction", "Age", "Outcome"]
        for i, col_name in enumerate(columns):
            pima = pima.withColumnRenamed(f"_c{i}", col_name)
        
        # Load data wearable jika tersedia
        wearable_path = "/data/bronze/wearable_*.csv"
        if os.path.exists(wearable_path.replace('*', '')):
            logger.info("Loading dataset wearable")
            wearable = spark.read.csv(wearable_path, 
                                    header=True, 
                                    inferSchema=True)
            # TODO: Join dengan data pima jika memungkinkan
        
        logger.info(f"Data loaded successfully. Row count: {pima.count()}")
        return pima
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def clean_data(df):
    """Membersihkan data"""
    logger.info("Membersihkan data")
    
    # Log statistik data awal
    logger.info(f"Row count before cleaning: {df.count()}")
    
    # Hitung missing values
    logger.info("Missing value statistics:")
    df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]).show()
    
    # Handle missing values dengan Imputer
    logger.info("Applying imputation for missing values")
    numeric_cols = ["Glucose", "BloodPressure", "SkinThickness", "Insulin", "BMI"]
    imputer = Imputer(
        inputCols=numeric_cols,
        outputCols=[f"{col}_imputed" for col in numeric_cols]
    )
    df = imputer.fit(df).transform(df)
    
    # Drop duplikasi
    df = df.dropDuplicates()
    
    # Filter data yang tidak valid
    df = df.filter((col("Glucose") > 0) & 
                 (col("BloodPressure") > 0) & 
                 (col("BMI") > 0))
    
    # Log statistik data akhir
    logger.info(f"Row count after cleaning: {df.count()}")
    
    return df

def transform_features(df):
    """Transformasi fitur untuk model ML"""
    logger.info("Transformasi fitur")
    
    # Feature engineering 
    # Contoh: BMI categories
    df = df.withColumn("BMI_Category", 
                      when(col("BMI") < 18.5, 0)
                      .when((col("BMI") >= 18.5) & (col("BMI") < 25), 1)
                      .when((col("BMI") >= 25) & (col("BMI") < 30), 2)
                      .otherwise(3))
    
    # Feature columns untuk vectorization
    feature_cols = ["Pregnancies", "Glucose_imputed", "BloodPressure_imputed", 
                   "SkinThickness_imputed", "Insulin_imputed", "BMI_imputed", 
                   "DiabetesPedigreeFunction", "Age", "BMI_Category"]
    
    # Vectorization
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    df = assembler.transform(df)
    
    # Standardization
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", 
                          withStd=True, withMean=True)
    df = scaler.fit(df).transform(df)
    
    return df

def save_to_silver(df):
    """Save processed data to silver zone"""
    logger.info("Saving data to silver zone")
    
    # Partition by year-month
    try:
        output_path = "/data/silver/diabetes_processed"
        
        # Save dataset 
        df.write.mode("overwrite").parquet(output_path)
        
        # Also save a sample for easy analysis
        df.sample(fraction=0.1, seed=42).write.mode("overwrite").csv(f"{output_path}_sample.csv")
        
        logger.info(f"Data saved to silver zone: {output_path}")
        
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        raise

def main():
    """Main ETL process"""
    logger.info("Starting ETL process")
    
    spark = create_spark_session()
    
    try:
        # Extract
        df = load_data(spark)
        
        # Transform
        df = clean_data(df)
        df = transform_features(df)
        
        # Load
        save_to_silver(df)
        
        logger.info("ETL process completed successfully")
        
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
