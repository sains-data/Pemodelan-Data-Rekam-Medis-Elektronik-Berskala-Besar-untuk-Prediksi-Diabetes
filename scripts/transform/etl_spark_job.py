#!/usr/bin/env python3
"""
ETL Spark Job for Diabetes Prediction Pipeline
Kelompok 8 RA - Big Data Project

This script handles data transformation from Bronze → Silver → Gold layers
implementing the medallion architecture for data lake processing.
"""

import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, mean, stddev, percentile_approx,
    regexp_replace, trim, lower, upper, to_timestamp, current_timestamp,
    lit, round as spark_round, coalesce, count, min as spark_min, max as spark_max,
    split, explode, year, month, dayofmonth, date_format, skewness, kurtosis, corr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    TimestampType, BooleanType, DateType
)
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DiabetesETLPipeline:
    """
    ETL Pipeline for diabetes prediction data processing
    Implements Bronze → Silver → Gold medallion architecture
    """
    
    def __init__(self, app_name="DiabetesETL"):
        """
        Initialize Spark session and ETL pipeline
        
        Args:
            app_name (str): Spark application name
        """
        try:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .getOrCreate()
            
            logger.info(f"Spark session created: {app_name}")
            self.bronze_schema = self._define_bronze_schema()
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def read_bronze_data(self, bronze_path):
        """
        Read data from Bronze layer
        
        Args:
            bronze_path (str): Path to bronze data
            
        Returns:
            pyspark.sql.DataFrame: Raw data
        """
        try:
            logger.info(f"Reading bronze data from {bronze_path}")
            df = self.spark.read.csv(bronze_path, header=True, inferSchema=True)
            logger.info(f"Loaded {df.count()} rows from bronze layer")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read bronze data: {e}")
            raise
    
    def clean_diabetes_data(self, df):
        """
        Clean and standardize diabetes dataset
        
        Args:
            df (pyspark.sql.DataFrame): Raw diabetes data
            
        Returns:
            pyspark.sql.DataFrame: Cleaned data
        """
        logger.info("Cleaning diabetes dataset...")
        
        # Handle missing values (zeros in medical measurements are often missing)
        df_cleaned = df.withColumn(
            "Glucose",
            when(col("Glucose") == 0, None).otherwise(col("Glucose"))
        ).withColumn(
            "BloodPressure",
            when(col("BloodPressure") == 0, None).otherwise(col("BloodPressure"))
        ).withColumn(
            "SkinThickness",
            when(col("SkinThickness") == 0, None).otherwise(col("SkinThickness"))
        ).withColumn(
            "Insulin",
            when(col("Insulin") == 0, None).otherwise(col("Insulin"))
        ).withColumn(
            "BMI",
            when(col("BMI") == 0, None).otherwise(col("BMI"))
        )
        
        # Calculate statistics for imputation
        glucose_mean = df_cleaned.select(mean(col("Glucose"))).collect()[0][0]
        bp_mean = df_cleaned.select(mean(col("BloodPressure"))).collect()[0][0]
        skin_mean = df_cleaned.select(mean(col("SkinThickness"))).collect()[0][0]
        insulin_median = df_cleaned.select(percentile_approx(col("Insulin"), 0.5)).collect()[0][0]
        bmi_mean = df_cleaned.select(mean(col("BMI"))).collect()[0][0]
        
        # Impute missing values
        df_imputed = df_cleaned.fillna({
            "Glucose": glucose_mean,
            "BloodPressure": bp_mean,
            "SkinThickness": skin_mean,
            "Insulin": insulin_median,
            "BMI": bmi_mean
        })
        
        # Add derived features
        df_enhanced = df_imputed.withColumn(
            "BMI_Category",
            when(col("BMI") < 18.5, "Underweight")
            .when(col("BMI") < 25, "Normal")
            .when(col("BMI") < 30, "Overweight")
            .otherwise("Obese")
        ).withColumn(
            "Age_Group",
            when(col("Age") < 30, "Young")
            .when(col("Age") < 50, "Middle")
            .otherwise("Senior")
        ).withColumn(
            "Glucose_Category",
            when(col("Glucose") < 100, "Normal")
            .when(col("Glucose") < 126, "Prediabetic")
            .otherwise("Diabetic")
        ).withColumn(
            "Pregnancy_Risk",
            when(col("Pregnancies") == 0, "No_Pregnancy")
            .when(col("Pregnancies") <= 2, "Low_Risk")
            .when(col("Pregnancies") <= 5, "Medium_Risk")
            .otherwise("High_Risk")
        )
        
        # Add processing metadata
        df_final = df_enhanced.withColumn("processed_timestamp", current_timestamp()) \
                             .withColumn("data_source", lit("diabetes_clinical"))
        
        logger.info(f"Diabetes data cleaning completed. Final count: {df_final.count()}")
        return df_final
    
    def clean_personal_health_data(self, df):
        """
        Clean and standardize personal health dataset
        
        Args:
            df (pyspark.sql.DataFrame): Raw personal health data
            
        Returns:
            pyspark.sql.DataFrame: Cleaned data
        """
        logger.info("Cleaning personal health dataset...")
        
        # Standardize gender values
        df_cleaned = df.withColumn(
            "Gender",
            when(lower(trim(col("Gender"))) == "male", "Male")
            .when(lower(trim(col("Gender"))) == "female", "Female")
            .otherwise("Other")
        )
        
        # Clean medical conditions
        df_cleaned = df_cleaned.withColumn(
            "Medical_Conditions",
            when(lower(trim(col("Medical_Conditions"))) == "none", None)
            .otherwise(col("Medical_Conditions"))
        )
        
        # Handle outliers in vital signs
        heart_rate_q1 = df_cleaned.select(percentile_approx(col("Heart_Rate"), 0.25)).collect()[0][0]
        heart_rate_q3 = df_cleaned.select(percentile_approx(col("Heart_Rate"), 0.75)).collect()[0][0]
        heart_rate_iqr = heart_rate_q3 - heart_rate_q1
        heart_rate_lower = heart_rate_q1 - 1.5 * heart_rate_iqr
        heart_rate_upper = heart_rate_q3 + 1.5 * heart_rate_iqr
        
        df_cleaned = df_cleaned.withColumn(
            "Heart_Rate",
            when((col("Heart_Rate") < heart_rate_lower) | (col("Heart_Rate") > heart_rate_upper), None)
            .otherwise(col("Heart_Rate"))
        )
        
        # Add derived features
        df_enhanced = df_cleaned.withColumn(
            "BMI_Calculated",
            spark_round(col("Weight") / ((col("Height") / 100) * (col("Height") / 100)), 2)
        ).withColumn(
            "Sleep_Efficiency",
            spark_round((col("Deep_Sleep_Duration") + col("REM_Sleep_Duration")) / col("Sleep_Duration") * 100, 2)
        ).withColumn(
            "Health_Risk_Category",
            when(col("Health_Score") >= 80, "Low_Risk")
            .when(col("Health_Score") >= 60, "Medium_Risk")
            .when(col("Health_Score") >= 40, "High_Risk")
            .otherwise("Critical_Risk")
        ).withColumn(
            "Timestamp_Parsed",
            to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
        
        # Fill missing values
        health_score_mean = df_enhanced.select(mean(col("Health_Score"))).collect()[0][0]
        heart_rate_mean = df_enhanced.select(mean(col("Heart_Rate"))).collect()[0][0]
        
        df_final = df_enhanced.fillna({
            "Health_Score": health_score_mean,
            "Heart_Rate": heart_rate_mean,
            "Stress_Level": "Unknown",
            "Mood": "Unknown"
        })
        
        # Add processing metadata
        df_final = df_final.withColumn("processed_timestamp", current_timestamp()) \
                          .withColumn("data_source", lit("personal_health_wearable"))
        
        logger.info(f"Personal health data cleaning completed. Final count: {df_final.count()}")
        return df_final
    
    def write_silver_data(self, df, silver_path, partition_columns=None):
        """
        Write cleaned data to Silver layer
        
        Args:
            df (pyspark.sql.DataFrame): Cleaned data
            silver_path (str): Path to silver layer
            partition_columns (list): Columns to partition by
        """
        try:
            logger.info(f"Writing data to silver layer: {silver_path}")
            
            writer = df.coalesce(1).write.mode("overwrite").option("header", "true")
            
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            writer.parquet(silver_path)
            
            logger.info(f"Successfully wrote data to {silver_path}")
            
        except Exception as e:
            logger.error(f"Failed to write silver data: {e}")
            raise
    
    def run_diabetes_etl(self):
        """Run ETL for diabetes dataset"""
        logger.info("Starting diabetes ETL pipeline...")
        
        # Read bronze data
        bronze_df = self.read_bronze_data("hdfs://namenode:9000/data/bronze/diabetes/diabetes.csv")
        
        # Clean and transform
        silver_df = self.clean_diabetes_data(bronze_df)
        
        # Write to silver layer
        self.write_silver_data(
            silver_df, 
            "hdfs://namenode:9000/data/silver/diabetes/",
            partition_columns=["BMI_Category"]
        )
        
        logger.info("Diabetes ETL pipeline completed")
    
    def run_personal_health_etl(self):
        """Run ETL for personal health dataset"""
        logger.info("Starting personal health ETL pipeline...")
        
        # Read bronze data
        bronze_df = self.read_bronze_data("hdfs://namenode:9000/data/bronze/personal_health/personal_health_data.csv")
        
        # Clean and transform
        silver_df = self.clean_personal_health_data(bronze_df)
        
        # Write to silver layer
        self.write_silver_data(
            silver_df,
            "hdfs://namenode:9000/data/silver/personal_health/",
            partition_columns=["Health_Risk_Category"]
        )
        
        logger.info("Personal health ETL pipeline completed")
    
    def run_full_etl(self):
        """Run complete ETL pipeline"""
        logger.info("Starting full ETL pipeline...")
        
        try:
            self.run_diabetes_etl()
            self.run_personal_health_etl()
            
            logger.info("Full ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()

    def _define_bronze_schema(self):
        """
        Define schema for bronze layer data
        
        Returns:
            StructType: Schema definition
        """
        return StructType([
            StructField("Pregnancies", IntegerType(), True),
            StructField("Glucose", DoubleType(), True),
            StructField("BloodPressure", DoubleType(), True),
            StructField("SkinThickness", DoubleType(), True),
            StructField("Insulin", DoubleType(), True),
            StructField("BMI", DoubleType(), True),
            StructField("DiabetesPedigreeFunction", DoubleType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Outcome", IntegerType(), True)
        ])
    
    def bronze_to_silver_transformation(self, input_path, output_path):
        """
        Transform data from Bronze layer to Silver layer
        - Data cleaning and validation
        - Standardization and normalization
        - Data quality improvements
        
        Args:
            input_path (str): Bronze layer input path
            output_path (str): Silver layer output path
        """
        logger.info("Starting Bronze → Silver transformation...")
        
        try:
            # Read bronze data from diabetes subdirectory
            diabetes_path = f"{input_path.rstrip('/')}/diabetes/"
            logger.info(f"Reading bronze data from: {diabetes_path}")
            bronze_df = self.spark.read.option("header", "true").csv(diabetes_path, schema=self.bronze_schema)
            
            logger.info(f"Bronze data loaded. Records: {bronze_df.count()}")
            
            # Data quality assessment
            self._assess_data_quality(bronze_df, "Bronze")
            
            # Clean and standardize data
            silver_df = self._clean_bronze_data(bronze_df)
            
            # Add metadata columns
            silver_df = silver_df.withColumn("processed_timestamp", current_timestamp()) \
                               .withColumn("data_source", lit("bronze_layer")) \
                               .withColumn("processing_version", lit("1.0"))
            
            # Data quality assessment after cleaning
            self._assess_data_quality(silver_df, "Silver")
            
            # Write to Silver layer in Parquet format
            logger.info(f"Writing silver data to: {output_path}")
            silver_df.write.mode("overwrite").parquet(output_path)
            
            logger.info("Bronze → Silver transformation completed successfully")
            
        except Exception as e:
            logger.error(f"Bronze → Silver transformation failed: {e}")
            raise
    
    def silver_to_gold_transformation(self, input_path, output_path):
        """
        Transform data from Silver layer to Gold layer
        - Feature engineering
        - Business logic application
        - Analytics-ready data preparation
        
        Args:
            input_path (str): Silver layer input path
            output_path (str): Gold layer output path
        """
        logger.info("Starting Silver → Gold transformation...")
        
        try:
            # Read silver data
            logger.info(f"Reading silver data from: {input_path}")
            silver_df = self.spark.read.parquet(input_path)
            
            logger.info(f"Silver data loaded. Records: {silver_df.count()}")
            
            # Apply feature engineering
            gold_df = self._engineer_features(silver_df)
            
            # Add business metrics
            gold_df = self._add_business_metrics(gold_df)
            
            # Final data quality check
            self._assess_data_quality(gold_df, "Gold")
            
            # Write to Gold layer
            logger.info(f"Writing gold data to: {output_path}")
            gold_df.write.mode("overwrite").parquet(output_path)
            
            logger.info("Silver → Gold transformation completed successfully")
            
        except Exception as e:
            logger.error(f"Silver → Gold transformation failed: {e}")
            raise
    
    def _clean_bronze_data(self, df):
        """
        Clean and standardize bronze layer data
        
        Args:
            df: Bronze DataFrame
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning bronze data...")
        
        # Handle missing values and outliers
        cleaned_df = df
        
        # Replace 0 values with null for certain medical measurements (they can't be 0)
        medical_zero_cols = ["Glucose", "BloodPressure", "SkinThickness", "Insulin", "BMI"]
        
        for col_name in medical_zero_cols:
            cleaned_df = cleaned_df.withColumn(
                col_name,
                when(col(col_name) == 0, None).otherwise(col(col_name))
            )
        
        # Handle outliers using IQR method
        cleaned_df = self._handle_outliers(cleaned_df, ["Glucose", "BloodPressure", "BMI", "Insulin"])
        
        # Impute missing values
        cleaned_df = self._impute_missing_values(cleaned_df)
        
        # Standardize column names
        cleaned_df = cleaned_df.withColumnRenamed("Outcome", "diabetes")
        
        # Add data validation flags
        cleaned_df = self._add_validation_flags(cleaned_df)
        
        logger.info("Bronze data cleaning completed")
        return cleaned_df
    
    def _handle_outliers(self, df, numeric_columns):
        """
        Handle outliers using IQR method
        
        Args:
            df: DataFrame
            numeric_columns: List of numeric columns to check
            
        Returns:
            DataFrame: DataFrame with outliers handled
        """
        logger.info("Handling outliers...")
        
        for col_name in numeric_columns:
            # Calculate Q1, Q3, and IQR
            quantiles = df.select(
                percentile_approx(col_name, 0.25).alias("q1"),
                percentile_approx(col_name, 0.75).alias("q3")
            ).collect()[0]
            
            q1, q3 = quantiles["q1"], quantiles["q3"]
            if q1 is not None and q3 is not None:
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                # Cap outliers
                df = df.withColumn(
                    col_name,
                    when(col(col_name) < lower_bound, lower_bound)
                    .when(col(col_name) > upper_bound, upper_bound)
                    .otherwise(col(col_name))
                )
        
        return df
    
    def _impute_missing_values(self, df):
        """
        Impute missing values using appropriate strategies
        
        Args:
            df: DataFrame with missing values
            
        Returns:
            DataFrame: DataFrame with imputed values
        """
        logger.info("Imputing missing values...")
        
        # Calculate statistics for imputation
        stats = df.select([
            mean(col("Glucose")).alias("glucose_mean"),
            mean(col("BloodPressure")).alias("bp_mean"),
            mean(col("SkinThickness")).alias("skin_mean"),
            mean(col("Insulin")).alias("insulin_mean"),
            mean(col("BMI")).alias("bmi_mean")
        ]).collect()[0]
        
        # Impute with mean values
        imputed_df = df.fillna({
            "Glucose": stats["glucose_mean"],
            "BloodPressure": stats["bp_mean"],
            "SkinThickness": stats["skin_mean"],
            "Insulin": stats["insulin_mean"],
            "BMI": stats["bmi_mean"]
        })
        
        return imputed_df
    
    def _add_validation_flags(self, df):
        """
        Add data validation flags
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame: DataFrame with validation flags
        """
        return df.withColumn(
            "data_quality_flag",
            when(
                (col("Glucose").isNotNull()) & 
                (col("BloodPressure").isNotNull()) & 
                (col("BMI").isNotNull()), "COMPLETE"
            ).otherwise("INCOMPLETE")
        )
    
    def _engineer_features(self, df):
        """
        Engineer features for machine learning
        
        Args:
            df: Silver layer DataFrame
            
        Returns:
            DataFrame: DataFrame with engineered features
        """
        logger.info("Engineering features...")
        
        # BMI categories
        df = df.withColumn(
            "bmi_category",
            when(col("BMI") < 18.5, "Underweight")
            .when((col("BMI") >= 18.5) & (col("BMI") < 25), "Normal")
            .when((col("BMI") >= 25) & (col("BMI") < 30), "Overweight")
            .otherwise("Obese")
        )
        
        # Age groups
        df = df.withColumn(
            "age_group",
            when(col("Age") < 30, "Young")
            .when((col("Age") >= 30) & (col("Age") < 50), "Middle")
            .otherwise("Senior")
        )
        
        # Glucose categories
        df = df.withColumn(
            "glucose_category",
            when(col("Glucose") < 100, "Normal")
            .when((col("Glucose") >= 100) & (col("Glucose") < 126), "Prediabetic")
            .otherwise("Diabetic")
        )
        
        # Blood pressure categories
        df = df.withColumn(
            "bp_category",
            when(col("BloodPressure") < 80, "Normal")
            .when((col("BloodPressure") >= 80) & (col("BloodPressure") < 90), "High_Normal")
            .otherwise("High")
        )
        
        # Risk score calculation (simple weighted sum)
        df = df.withColumn(
            "risk_score",
            (col("Glucose") * 0.3 + 
             col("BMI") * 0.2 + 
             col("Age") * 0.1 + 
             col("BloodPressure") * 0.1 + 
             col("DiabetesPedigreeFunction") * 100 * 0.2 +
             col("Pregnancies") * 5 * 0.1) / 100
        )
        
        # Interaction features
        df = df.withColumn("glucose_bmi_interaction", col("Glucose") * col("BMI"))
        df = df.withColumn("age_glucose_interaction", col("Age") * col("Glucose"))
        
        # Normalized features (Min-Max scaling approximation)
        df = self._add_normalized_features(df)
        
        return df
    
    def _add_normalized_features(self, df):
        """
        Add normalized versions of key features
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame: DataFrame with normalized features
        """
        # Calculate min/max for normalization
        stats = df.select([
            spark_min("Glucose").alias("glucose_min"),
            spark_max("Glucose").alias("glucose_max"),
            spark_min("BMI").alias("bmi_min"),
            spark_max("BMI").alias("bmi_max"),
            spark_min("Age").alias("age_min"),
            spark_max("Age").alias("age_max")
        ]).collect()[0]
        
        # Normalize features
        df = df.withColumn(
            "glucose_normalized",
            (col("Glucose") - stats["glucose_min"]) / (stats["glucose_max"] - stats["glucose_min"])
        )
        
        df = df.withColumn(
            "bmi_normalized",
            (col("BMI") - stats["bmi_min"]) / (stats["bmi_max"] - stats["bmi_min"])
        )
        
        df = df.withColumn(
            "age_normalized",
            (col("Age") - stats["age_min"]) / (stats["age_max"] - stats["age_min"])
        )
        
        return df
    
    def _add_business_metrics(self, df):
        """
        Add business-relevant metrics and insights
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame: DataFrame with business metrics
        """
        logger.info("Adding business metrics...")
        
        # Health risk levels
        df = df.withColumn(
            "health_risk_level",
            when(col("risk_score") < 0.3, "Low")
            .when((col("risk_score") >= 0.3) & (col("risk_score") < 0.6), "Medium")
            .otherwise("High")
        )
        
        # Intervention priority
        df = df.withColumn(
            "intervention_priority",
            when(
                (col("glucose_category") == "Diabetic") | 
                (col("bmi_category") == "Obese"), "Immediate"
            ).when(
                (col("glucose_category") == "Prediabetic") | 
                (col("bmi_category") == "Overweight"), "Soon"
            ).otherwise("Routine")
        )
        
        # Cost category (estimated healthcare cost impact)
        df = df.withColumn(
            "estimated_cost_category",
            when(col("health_risk_level") == "High", "High_Cost")
            .when(col("health_risk_level") == "Medium", "Medium_Cost")
            .otherwise("Low_Cost")
        )
        
        return df
    
    def _assess_data_quality(self, df, layer_name):
        """
        Assess and log data quality metrics
        
        Args:
            df: DataFrame to assess
            layer_name (str): Name of the data layer
        """
        logger.info(f"Assessing data quality for {layer_name} layer...")
        
        total_records = df.count()
        total_columns = len(df.columns)
        
        # Calculate missing value statistics
        missing_stats = df.select([
            (count(when(col(c).isNull() | isnan(col(c)), c)) / total_records * 100).alias(f"{c}_missing_pct")
            for c in df.columns if c not in ['processed_timestamp', 'data_source', 'processing_version']
        ]).collect()[0]
        
        logger.info(f"{layer_name} Data Quality Report:")
        logger.info(f"  Total Records: {total_records}")
        logger.info(f"  Total Columns: {total_columns}")
        
        # Log missing value percentages
        for column, missing_pct in missing_stats.asDict().items():
            if missing_pct is not None and missing_pct > 0:
                logger.info(f"  {column.replace('_missing_pct', '')}: {missing_pct:.2f}% missing")
        
        # Calculate basic statistics for numeric columns
        numeric_columns = [col_name for col_name, data_type in df.dtypes 
                          if data_type in ['int', 'double', 'float']]
        
        if numeric_columns:
            stats = df.select([
                mean(col(c)).alias(f"{c}_mean") for c in numeric_columns[:5]  # Limit to first 5 to avoid too much output
            ]).collect()[0]
            
            logger.info("  Basic Statistics (first 5 numeric columns):")
            for column, mean_val in stats.asDict().items():
                if mean_val is not None:
                    logger.info(f"    {column.replace('_mean', '')} mean: {mean_val:.2f}")
                else:
                    logger.info(f"    {column.replace('_mean', '')} mean: No data")

def main():
    """Main function to run ETL transformations"""
    parser = argparse.ArgumentParser(description='Run ETL transformations for diabetes prediction pipeline')
    parser.add_argument('--input-path', required=True, help='Input data path')
    parser.add_argument('--output-path', required=True, help='Output data path')
    parser.add_argument('--transformation-type', required=True, 
                       choices=['bronze_to_silver', 'silver_to_gold'],
                       help='Type of transformation to perform')
    
    args = parser.parse_args()
    
    # Initialize ETL pipeline
    etl = DiabetesETLPipeline()
    
    # Run specified transformation
    if args.transformation_type == 'bronze_to_silver':
        etl.bronze_to_silver_transformation(args.input_path, args.output_path)
    elif args.transformation_type == 'silver_to_gold':
        etl.silver_to_gold_transformation(args.input_path, args.output_path)
    
    logger.info(f"ETL transformation '{args.transformation_type}' completed successfully!")

if __name__ == "__main__":
    main()
