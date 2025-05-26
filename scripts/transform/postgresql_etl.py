#!/usr/bin/env python3        # PostgreSQL connection parameters
        self.pg_config = {
            'host': 'localhost',  # Use localhost when running outside Docker
            'port': 5439,         # Updated port from .env
            'database': 'diabetes_warehouse',
            'user': 'warehouse',
            'password': 'warehouse_secure_2025'
        }tgreSQL ETL Pipeline - Lightweight Alternative to Hive
Transfers data from HDFS to PostgreSQL warehouse for Grafana visualization
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import uuid

class PostgreSQLETL:
    def __init__(self):
        # PostgreSQL connection parameters
        self.pg_config = {
            'host': 'postgres-warehouse',
            'port': 5439,
            'database': 'diabetes_warehouse',
            'user': 'warehouse',
            'password': 'warehouse_secure_2025'
        }
        
        # Create connection string
        self.connection_string = f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}@{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
        
        # Initialize Spark with minimal resources
        self.spark = SparkSession.builder \
            .appName("PostgreSQLETL") \
            .config("spark.executor.memory", "512m") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.cores", "1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Database engine
        self.engine = None
        self.connection = None
        
    def connect_to_postgres(self):
        """Establish connection to PostgreSQL"""
        try:
            self.engine = create_engine(self.connection_string)
            self.connection = psycopg2.connect(**self.pg_config)
            self.logger.info("✅ Connected to PostgreSQL warehouse successfully")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            return False
    
    def load_data_from_hdfs(self, hdfs_path):
        """Load data from HDFS using Spark"""
        try:
            self.logger.info(f"📥 Loading data from HDFS: {hdfs_path}")
            
            if hdfs_path.endswith('.csv'):
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path)
            elif hdfs_path.endswith('.parquet'):
                df = self.spark.read.parquet(hdfs_path)
            else:
                raise ValueError(f"Unsupported file format: {hdfs_path}")
            
            self.logger.info(f"✅ Loaded {df.count()} records from HDFS")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load data from HDFS: {e}")
            return None
    
    def insert_bronze_data(self, df):
        """Insert raw data into bronze layer"""
        try:
            self.logger.info("📝 Inserting data into bronze layer...")
            
            # Convert Spark DataFrame to Pandas
            pandas_df = df.toPandas()
            
            # Add metadata columns
            pandas_df['id'] = [str(uuid.uuid4()) for _ in range(len(pandas_df))]
            pandas_df['source_file'] = 'hdfs_import'
            pandas_df['ingestion_timestamp'] = datetime.now()
            pandas_df['created_at'] = datetime.now()
            
            # Rename columns to match database schema
            column_mapping = {
                'Pregnancies': 'pregnancies',
                'Glucose': 'glucose',
                'BloodPressure': 'blood_pressure',
                'SkinThickness': 'skin_thickness',
                'Insulin': 'insulin',
                'BMI': 'bmi',
                'DiabetesPedigreeFunction': 'diabetes_pedigree_function',
                'Age': 'age',
                'Outcome': 'outcome'
            }
            
            pandas_df = pandas_df.rename(columns=column_mapping)
            
            # Insert into PostgreSQL
            pandas_df.to_sql(
                'diabetes_raw',
                self.engine,
                schema='bronze',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            self.logger.info(f"✅ Inserted {len(pandas_df)} records into bronze.diabetes_raw")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to insert bronze data: {e}")
            return False
    
    def clean_and_transform_data(self):
        """Clean data and insert into silver layer"""
        try:
            self.logger.info("🧹 Cleaning and transforming data for silver layer...")
            
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Clean and insert into silver layer
                cursor.execute("""
                    INSERT INTO silver.diabetes_cleaned 
                    (source_id, pregnancies, glucose, blood_pressure, skin_thickness, 
                     insulin, bmi, diabetes_pedigree_function, age, outcome, 
                     is_valid, quality_score, anomaly_flags)
                    SELECT 
                        id as source_id,
                        pregnancies,
                        CASE WHEN glucose <= 0 THEN NULL ELSE glucose END as glucose,
                        CASE WHEN blood_pressure <= 0 THEN NULL ELSE blood_pressure END as blood_pressure,
                        CASE WHEN skin_thickness <= 0 THEN NULL ELSE skin_thickness END as skin_thickness,
                        CASE WHEN insulin <= 0 THEN NULL ELSE insulin END as insulin,
                        CASE WHEN bmi <= 0 THEN NULL ELSE bmi END as bmi,
                        diabetes_pedigree_function,
                        age,
                        outcome,
                        -- Validity check
                        CASE WHEN 
                            glucose > 0 AND blood_pressure > 0 AND bmi > 0 AND age > 0 
                            AND glucose < 300 AND blood_pressure < 200 AND bmi < 60 AND age < 120
                        THEN TRUE ELSE FALSE END as is_valid,
                        -- Quality score calculation
                        CASE WHEN 
                            glucose > 0 AND blood_pressure > 0 AND skin_thickness > 0 
                            AND insulin > 0 AND bmi > 0 
                        THEN 1.0 ELSE 0.8 END as quality_score,
                        -- Anomaly flags
                        jsonb_build_object(
                            'zero_glucose', glucose <= 0,
                            'zero_blood_pressure', blood_pressure <= 0,
                            'zero_bmi', bmi <= 0,
                            'extreme_glucose', glucose > 250,
                            'extreme_bmi', bmi > 50
                        ) as anomaly_flags
                    FROM bronze.diabetes_raw 
                    WHERE ingestion_timestamp >= CURRENT_DATE
                    ON CONFLICT DO NOTHING;
                """)
                
                self.connection.commit()
                self.logger.info("✅ Data cleaned and inserted into silver layer")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to clean data: {e}")
            self.connection.rollback()
            return False
        
        return True
    
    def create_features(self):
        """Create features and insert into gold layer"""
        try:
            self.logger.info("⚙️ Creating features for gold layer...")
            
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Create features and insert into gold layer
                cursor.execute("""
                    INSERT INTO gold.diabetes_features 
                    (source_id, pregnancies, glucose, blood_pressure, skin_thickness, 
                     insulin, bmi, diabetes_pedigree_function, age, outcome,
                     age_group, bmi_category, glucose_category, risk_score, high_risk_flag)
                    SELECT 
                        id as source_id,
                        pregnancies,
                        glucose,
                        blood_pressure,
                        skin_thickness,
                        insulin,
                        bmi,
                        diabetes_pedigree_function,
                        age,
                        outcome,
                        -- Age groups
                        CASE 
                            WHEN age < 30 THEN '20-29'
                            WHEN age < 40 THEN '30-39'
                            WHEN age < 50 THEN '40-49'
                            WHEN age < 60 THEN '50-59'
                            ELSE '60+'
                        END as age_group,
                        -- BMI categories
                        CASE 
                            WHEN bmi IS NULL THEN 'Unknown'
                            WHEN bmi < 18.5 THEN 'Underweight'
                            WHEN bmi < 25 THEN 'Normal'
                            WHEN bmi < 30 THEN 'Overweight'
                            ELSE 'Obese'
                        END as bmi_category,
                        -- Glucose categories
                        CASE 
                            WHEN glucose IS NULL THEN 'Unknown'
                            WHEN glucose < 100 THEN 'Normal (<100)'
                            WHEN glucose < 140 THEN 'Pre-diabetic (100-139)'
                            ELSE 'Diabetic (140+)'
                        END as glucose_category,
                        -- Risk score calculation (simple version)
                        CASE 
                            WHEN glucose IS NULL OR bmi IS NULL OR age IS NULL THEN 0.0
                            ELSE ROUND(
                                LEAST(1.0, 
                                    (COALESCE(glucose, 100) / 200.0) * 0.4 +
                                    (COALESCE(bmi, 25) / 50.0) * 0.3 +
                                    (age / 100.0) * 0.2 +
                                    (pregnancies / 10.0) * 0.1
                                ), 2
                            )
                        END as risk_score,
                        -- High risk flag
                        CASE WHEN 
                            (glucose > 140 AND bmi > 30) OR 
                            (glucose > 160) OR 
                            (bmi > 35) OR 
                            (age > 60 AND glucose > 120)
                        THEN TRUE ELSE FALSE END as high_risk_flag
                    FROM silver.diabetes_cleaned 
                    WHERE is_valid = TRUE 
                    AND processed_at >= CURRENT_DATE
                    ON CONFLICT DO NOTHING;
                """)
                
                self.connection.commit()
                self.logger.info("✅ Features created and inserted into gold layer")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to create features: {e}")
            self.connection.rollback()
            return False
        
        return True
    
    def export_metrics_for_grafana(self):
        """Export metrics for Grafana visualization"""
        try:
            self.logger.info("📊 Exporting metrics for Grafana...")
            
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get latest metrics
                cursor.execute("SELECT * FROM analytics.latest_metrics;")
                latest_metrics = {row['metric_name']: row['metric_value'] for row in cursor.fetchall()}
                
                # Get age distribution
                cursor.execute("SELECT * FROM analytics.age_group_analysis;")
                age_distribution = cursor.fetchall()
                
                # Get BMI analysis
                cursor.execute("SELECT * FROM analytics.bmi_category_analysis;")
                bmi_analysis = cursor.fetchall()
                
                # Get risk analysis
                cursor.execute("SELECT * FROM analytics.risk_analysis;")
                risk_analysis = cursor.fetchall()
                
                # Prepare metrics JSON
                metrics = {
                    "basic_stats": latest_metrics,
                    "age_distribution": [dict(row) for row in age_distribution],
                    "bmi_analysis": [dict(row) for row in bmi_analysis],
                    "risk_analysis": [dict(row) for row in risk_analysis],
                    "timestamp": datetime.now().isoformat(),
                    "source": "postgresql_warehouse"
                }
                
                # Save to JSON file for Grafana
                output_path = "/tmp/diabetes_metrics_postgres.json"
                with open(output_path, 'w') as f:
                    json.dump(metrics, f, indent=2, default=str)
                
                self.logger.info(f"✅ Metrics exported to {output_path}")
                return metrics
                
        except Exception as e:
            self.logger.error(f"❌ Failed to export metrics: {e}")
            return None
    
    def run_full_etl(self, hdfs_path="hdfs://namenode:9000/data/bronze/diabetes.csv"):
        """Run complete ETL pipeline"""
        try:
            self.logger.info("🚀 Starting PostgreSQL ETL pipeline...")
            
            # Connect to PostgreSQL
            if not self.connect_to_postgres():
                return False
            
            # Load data from HDFS
            df = self.load_data_from_hdfs(hdfs_path)
            if df is None:
                return False
            
            # Insert into bronze layer
            if not self.insert_bronze_data(df):
                return False
            
            # Clean and transform data
            if not self.clean_and_transform_data():
                return False
            
            # Create features
            if not self.create_features():
                return False
            
            # Export metrics
            metrics = self.export_metrics_for_grafana()
            if metrics is None:
                return False
            
            self.logger.info("🎉 PostgreSQL ETL pipeline completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ ETL pipeline failed: {e}")
            return False
            
        finally:
            # Cleanup
            if self.connection:
                self.connection.close()
            if self.spark:
                self.spark.stop()
    
    def get_data_summary(self):
        """Get summary of data in warehouse"""
        try:
            if not self.connect_to_postgres():
                return None
                
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        'bronze' as layer,
                        COUNT(*) as record_count
                    FROM bronze.diabetes_raw
                    UNION ALL
                    SELECT 
                        'silver' as layer,
                        COUNT(*) as record_count
                    FROM silver.diabetes_cleaned
                    UNION ALL
                    SELECT 
                        'gold' as layer,
                        COUNT(*) as record_count
                    FROM gold.diabetes_features;
                """)
                
                summary = cursor.fetchall()
                return [dict(row) for row in summary]
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get data summary: {e}")
            return None
        finally:
            if self.connection:
                self.connection.close()

def main():
    """Main function for standalone execution"""
    etl = PostgreSQLETL()
    
    # Run ETL pipeline
    success = etl.run_full_etl()
    
    if success:
        print("✅ ETL Pipeline completed successfully!")
        
        # Show data summary
        summary = etl.get_data_summary()
        if summary:
            print("\n📊 Data Summary:")
            for layer in summary:
                print(f"  {layer['layer'].upper()} Layer: {layer['record_count']} records")
    else:
        print("❌ ETL Pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
