#!/usr/bin/env python3
"""
Enhanced PostgreSQL ETL Pipeline for Diabetes Prediction
=========================================================
Comprehensive ETL pipeline that matches the Hive gold schema structure with:
- Feature engineering and interaction features
- Risk scoring and predictions
- Business intelligence features
- Data quality scoring
- Model metadata tracking
"""
import os
import sys
import pandas as pd
import numpy as np
import psycopg2
import uuid
import json
import logging
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedPostgreSQLETL:
    """Enhanced ETL pipeline for comprehensive diabetes prediction data"""
    
    def __init__(self):
        """Initialize the enhanced ETL pipeline"""
        self.load_config()
        self.connection = None
        self.execution_id = None
        
    def load_config(self):
        """Load database configuration from environment variables"""
        self.config = {
            'host': os.getenv('POSTGRES_WAREHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_WAREHOUSE_PORT', 5439)),
            'database': os.getenv('POSTGRES_WAREHOUSE_DB', 'diabetes_warehouse'),
            'user': os.getenv('POSTGRES_WAREHOUSE_USER', 'warehouse'),
            'password': os.getenv('POSTGRES_WAREHOUSE_PASSWORD', 'warehouse_secure_2025')
        }
        
        # HDFS configuration
        self.hdfs_config = {
            'bronze_path': '/data/bronze/',
            'silver_path': '/data/silver/',
            'gold_path': '/data/gold/'
        }
        
    def connect_postgres(self) -> psycopg2.extensions.connection:
        """Connect to PostgreSQL warehouse"""
        try:
            connection = psycopg2.connect(**self.config)
            connection.autocommit = True
            logger.info("Successfully connected to PostgreSQL warehouse")
            return connection
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
            
    def read_sample_data(self) -> pd.DataFrame:
        """Read sample diabetes data (simulating HDFS read)"""
        try:
            # For demo purposes, create sample data that matches the expected structure
            # In production, this would read from HDFS using pyspark or hdfs3
            sample_data = {
                'pregnancies': [6, 1, 8, 1, 0, 5, 3, 10, 2, 8, 4, 2, 7, 9, 1],
                'glucose': [148, 85, 183, 89, 137, 116, 78, 115, 197, 125, 162, 96, 175, 142, 103],
                'blood_pressure': [72, 66, 64, 66, 40, 74, 50, 0, 70, 96, 84, 68, 88, 82, 76],
                'skin_thickness': [35, 29, 0, 23, 35, 0, 32, 0, 45, 0, 28, 33, 41, 36, 25],
                'insulin': [0, 0, 0, 94, 168, 0, 88, 0, 543, 0, 156, 112, 342, 298, 178],
                'bmi': [33.6, 26.6, 23.3, 28.1, 43.1, 25.6, 31.0, 35.3, 30.5, 0, 29.8, 27.2, 38.4, 32.1, 24.7],
                'diabetes_pedigree_function': [0.627, 0.351, 0.672, 0.167, 2.288, 0.201, 0.248, 0.134, 0.158, 0.232, 0.415, 0.298, 0.781, 0.593, 0.324],
                'age': [50, 31, 32, 21, 33, 30, 26, 29, 53, 54, 45, 28, 58, 47, 35],
                'diabetes': [1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 1, 0]
            }
            
            df = pd.DataFrame(sample_data)
            logger.info(f"Loaded sample data with {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read sample data: {e}")
            raise
            
    def data_quality_check(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
        """Perform comprehensive data quality checks"""
        try:
            # Calculate quality scores for each record
            quality_scores = []
            anomaly_flags = []
            
            for _, row in df.iterrows():
                score = 1.0
                anomalies = []
                
                # Check for missing values
                missing_count = row.isnull().sum()
                if missing_count > 0:
                    score -= missing_count * 0.1
                    anomalies.append(f"missing_values_{missing_count}")
                
                # Check for zero values that might indicate missing data
                if row['glucose'] == 0:
                    score -= 0.15
                    anomalies.append("zero_glucose")
                if row['blood_pressure'] == 0:
                    score -= 0.15
                    anomalies.append("zero_blood_pressure")
                if row['bmi'] == 0:
                    score -= 0.15
                    anomalies.append("zero_bmi")
                
                # Check for outliers using domain knowledge
                if row['glucose'] > 0 and (row['glucose'] < 50 or row['glucose'] > 250):
                    score -= 0.2
                    anomalies.append("glucose_outlier")
                    
                if row['bmi'] > 0 and (row['bmi'] < 15 or row['bmi'] > 50):
                    score -= 0.2
                    anomalies.append("bmi_outlier")
                    
                if row['age'] < 18 or row['age'] > 100:
                    score -= 0.2
                    anomalies.append("age_outlier")
                
                # Logical consistency checks
                if row['pregnancies'] > 0 and row['age'] < 18:
                    score -= 0.3
                    anomalies.append("pregnancy_age_inconsistent")
                
                quality_scores.append(max(0.0, score))
                anomaly_flags.append(anomalies if anomalies else [])
            
            df['quality_score'] = quality_scores
            df['anomaly_flags'] = anomaly_flags
            
            avg_quality = np.mean(quality_scores)
            logger.info(f"Data quality check completed. Average quality score: {avg_quality:.3f}")
            
            return df, avg_quality
            
        except Exception as e:
            logger.error(f"Data quality check failed: {e}")
            raise
            
    def feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Comprehensive feature engineering matching Hive gold schema"""
        try:
            df_enhanced = df.copy()
            
            # Fill zero values with median for calculations
            numeric_cols = ['glucose', 'blood_pressure', 'skin_thickness', 'insulin', 'bmi']
            for col in numeric_cols:
                median_val = df_enhanced[df_enhanced[col] > 0][col].median()
                df_enhanced[col] = df_enhanced[col].replace(0, median_val)
            
            # Categorical features
            df_enhanced['bmi_category'] = pd.cut(
                df_enhanced['bmi'], 
                bins=[0, 18.5, 25, 30, float('inf')], 
                labels=['Underweight', 'Normal', 'Overweight', 'Obese']
            ).astype(str)
            
            df_enhanced['age_group'] = pd.cut(
                df_enhanced['age'], 
                bins=[0, 25, 35, 45, 55, float('inf')], 
                labels=['18-25', '26-35', '36-45', '46-55', '55+']
            ).astype(str)
            
            df_enhanced['glucose_category'] = pd.cut(
                df_enhanced['glucose'], 
                bins=[0, 100, 125, 200, float('inf')], 
                labels=['Normal', 'Prediabetic', 'Diabetic', 'Severe']
            ).astype(str)
            
            df_enhanced['bp_category'] = pd.cut(
                df_enhanced['blood_pressure'], 
                bins=[0, 80, 90, 140, float('inf')], 
                labels=['Normal', 'Elevated', 'High', 'Very High']
            ).astype(str)
            
            # Interaction features
            df_enhanced['glucose_bmi_interaction'] = df_enhanced['glucose'] * df_enhanced['bmi'] / 1000
            df_enhanced['age_glucose_interaction'] = df_enhanced['age'] * df_enhanced['glucose'] / 100
            df_enhanced['insulin_glucose_interaction'] = df_enhanced['insulin'] * df_enhanced['glucose'] / 1000
            df_enhanced['age_bmi_interaction'] = df_enhanced['age'] * df_enhanced['bmi'] / 100
            
            # Normalized features (Min-Max scaling to 0-1)
            normalization_ranges = {
                'glucose': (50, 300),
                'bmi': (15, 50),
                'age': (18, 80),
                'blood_pressure': (40, 180),
                'insulin': (0, 500)
            }
            
            for col, (min_val, max_val) in normalization_ranges.items():
                if col in df_enhanced.columns:
                    if col == 'blood_pressure':
                        df_enhanced['bp_normalized'] = (df_enhanced[col] - min_val) / (max_val - min_val)
                        df_enhanced['bp_normalized'] = df_enhanced['bp_normalized'].clip(0, 1)
                    else:
                        df_enhanced[f'{col}_normalized'] = (df_enhanced[col] - min_val) / (max_val - min_val)
                        df_enhanced[f'{col}_normalized'] = df_enhanced[f'{col}_normalized'].clip(0, 1)
            
            # Risk scoring (weighted risk factors)
            df_enhanced['risk_score'] = self.calculate_risk_score(df_enhanced)
            
            # Predictions (simulated ML predictions based on risk score)
            df_enhanced['prediction'] = (df_enhanced['risk_score'] > 0.5).astype(int)
            df_enhanced['prediction_probability'] = df_enhanced['risk_score']
            
            # Business Intelligence features
            df_enhanced['health_risk_level'] = df_enhanced['risk_score'].apply(
                lambda x: 'Critical' if x > 0.8 else 'High' if x > 0.6 else 'Medium' if x > 0.3 else 'Low'
            )
            
            df_enhanced['intervention_priority'] = df_enhanced.apply(
                lambda row: 'Immediate' if row['health_risk_level'] == 'Critical' 
                else 'Soon' if row['health_risk_level'] == 'High'
                else 'Routine' if row['health_risk_level'] == 'Medium'
                else 'Monitor', axis=1
            )
            
            df_enhanced['estimated_cost_category'] = df_enhanced['health_risk_level'].map({
                'Critical': 'Very High',
                'High': 'High', 
                'Medium': 'Medium',
                'Low': 'Low'
            })
            
            # Additional risk factors
            df_enhanced['metabolic_syndrome_risk'] = self.calculate_metabolic_syndrome_risk(df_enhanced)
            df_enhanced['cardiovascular_risk'] = self.calculate_cardiovascular_risk(df_enhanced)
            df_enhanced['pregnancy_risk_factor'] = df_enhanced['pregnancies'] * 0.1
            df_enhanced['lifestyle_risk_score'] = (df_enhanced['bmi_normalized'] + df_enhanced['glucose_normalized']) / 2
            
            # Treatment recommendations
            df_enhanced['treatment_recommendation'] = df_enhanced.apply(
                self.generate_treatment_recommendation, axis=1
            )
            
            # Metadata
            df_enhanced['data_source'] = 'hdfs_diabetes_dataset'
            df_enhanced['processing_version'] = '2.0'
            df_enhanced['processed_timestamp'] = datetime.now()
            df_enhanced['processing_date'] = date.today()
            
            logger.info(f"Feature engineering completed. Added {len(df_enhanced.columns) - len(df.columns)} new features")
            
            return df_enhanced
            
        except Exception as e:
            logger.error(f"Feature engineering failed: {e}")
            raise
            
    def calculate_risk_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate comprehensive risk score"""
        try:
            # Weighted risk factors based on clinical importance
            weights = {
                'glucose': 0.25,
                'bmi': 0.20,
                'age': 0.15,
                'blood_pressure': 0.15,
                'insulin': 0.10,
                'diabetes_pedigree_function': 0.10,
                'pregnancies': 0.05
            }
            
            # Calculate weighted sum of normalized features
            risk_score = (
                df['glucose_normalized'] * weights['glucose'] +
                df['bmi_normalized'] * weights['bmi'] +
                df['age_normalized'] * weights['age'] +
                df['bp_normalized'] * weights['blood_pressure'] +
                df['insulin_normalized'] * weights['insulin'] +
                (df['diabetes_pedigree_function'] / 3.0).clip(0, 1) * weights['diabetes_pedigree_function'] +
                (df['pregnancies'] / 15.0).clip(0, 1) * weights['pregnancies']
            )
            
            return risk_score.clip(0, 1)
            
        except Exception as e:
            logger.error(f"Risk score calculation failed: {e}")
            return pd.Series([0.5] * len(df))
            
    def calculate_metabolic_syndrome_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate metabolic syndrome risk"""
        try:
            # Metabolic syndrome criteria
            bmi_risk = (df['bmi'] > 30).astype(float) * 0.35
            glucose_risk = (df['glucose'] > 100).astype(float) * 0.35
            bp_risk = (df['blood_pressure'] > 130).astype(float) * 0.30
            
            return (bmi_risk + glucose_risk + bp_risk).clip(0, 1)
            
        except Exception as e:
            logger.error(f"Metabolic syndrome risk calculation failed: {e}")
            return pd.Series([0.3] * len(df))
            
    def calculate_cardiovascular_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate cardiovascular risk"""
        try:
            # Cardiovascular risk factors
            age_risk = (df['age'] > 45).astype(float) * 0.25
            bp_risk = (df['blood_pressure'] > 140).astype(float) * 0.30
            bmi_risk = (df['bmi'] > 30).astype(float) * 0.25
            diabetes_risk = df['diabetes'].astype(float) * 0.20
            
            return (age_risk + bp_risk + bmi_risk + diabetes_risk).clip(0, 1)
            
        except Exception as e:
            logger.error(f"Cardiovascular risk calculation failed: {e}")
            return pd.Series([0.4] * len(df))
            
    def generate_treatment_recommendation(self, row) -> str:
        """Generate treatment recommendations based on risk profile"""
        try:
            recommendations = []
            
            if row['health_risk_level'] == 'Critical':
                recommendations.append("Immediate medical consultation required")
                recommendations.append("Comprehensive diabetes management plan")
                
            if row['bmi'] > 30:
                recommendations.append("Weight management program")
                
            if row['glucose'] > 140:
                recommendations.append("Blood glucose monitoring and medication review")
                
            if row['blood_pressure'] > 140:
                recommendations.append("Blood pressure management")
                
            if row['age'] > 50:
                recommendations.append("Regular health screenings")
                
            if row['insulin'] > 200:
                recommendations.append("Insulin resistance evaluation")
                
            if not recommendations:
                recommendations.append("Maintain healthy lifestyle and regular check-ups")
                
            return "; ".join(recommendations)
            
        except Exception as e:
            logger.error(f"Treatment recommendation generation failed: {e}")
            return "Standard care and monitoring"
            
    def load_to_bronze(self, df: pd.DataFrame) -> None:
        """Load raw data to bronze layer"""
        try:
            connection = self.connect_postgres()
            cursor = connection.cursor()
            
            # Prepare data for bronze layer
            bronze_data = df[['pregnancies', 'glucose', 'blood_pressure', 'skin_thickness', 
                            'insulin', 'bmi', 'diabetes_pedigree_function', 'age', 'diabetes']].copy()
            bronze_data['source_file'] = 'etl_pipeline_load'
            bronze_data['ingestion_timestamp'] = datetime.now()
            bronze_data['data_source'] = 'enhanced_etl_pipeline'
            bronze_data['ingestion_version'] = '2.0'
            
            # Insert data using executemany for better performance
            insert_query = """
                INSERT INTO bronze.diabetes_clinical 
                (pregnancies, glucose, blood_pressure, skin_thickness, insulin, 
                 bmi, diabetes_pedigree_function, age, outcome, data_source, ingestion_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = []
            for _, row in bronze_data.iterrows():
                values.append((
                    int(row['pregnancies']), 
                    float(row['glucose']), 
                    float(row['blood_pressure']), 
                    float(row['skin_thickness']), 
                    float(row['insulin']), 
                    float(row['bmi']), 
                    float(row['diabetes_pedigree_function']), 
                    int(row['age']), 
                    int(row['diabetes']),
                    row['data_source'],
                    row['ingestion_version']
                ))
            
            cursor.executemany(insert_query, values)
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"Successfully loaded {len(bronze_data)} records to bronze layer")
            
        except Exception as e:
            logger.error(f"Failed to load data to bronze layer: {e}")
            raise
            
    def load_to_gold(self, df: pd.DataFrame) -> None:
        """Load enhanced feature data to gold layer"""
        try:
            connection = self.connect_postgres()
            cursor = connection.cursor()
            
            # Prepare data for gold layer (using diabetes_comprehensive table)
            insert_query = """
                INSERT INTO gold.diabetes_comprehensive 
                (pregnancies, glucose, blood_pressure, skin_thickness, insulin, 
                 bmi, diabetes_pedigree_function, age, diabetes,
                 bmi_category, age_group, glucose_category, bp_category,
                 risk_score, prediction, prediction_probability,
                 glucose_bmi_interaction, age_glucose_interaction, insulin_glucose_interaction, age_bmi_interaction,
                 glucose_normalized, bmi_normalized, age_normalized, bp_normalized, insulin_normalized,
                 health_risk_level, intervention_priority, estimated_cost_category, treatment_recommendation,
                 metabolic_syndrome_risk, cardiovascular_risk, pregnancy_risk_factor, lifestyle_risk_score,
                 data_source, processing_version, quality_score, anomaly_flags, processed_timestamp, processing_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = []
            for _, row in df.iterrows():
                anomaly_flags_json = json.dumps(row.get('anomaly_flags', []))
                
                values.append((
                    int(row['pregnancies']),
                    float(row['glucose']),
                    float(row['blood_pressure']),
                    float(row['skin_thickness']),
                    float(row['insulin']),
                    float(row['bmi']),
                    float(row['diabetes_pedigree_function']),
                    int(row['age']),
                    int(row['diabetes']),
                    str(row['bmi_category']),
                    str(row['age_group']),
                    str(row['glucose_category']),
                    str(row['bp_category']),
                    float(row['risk_score']),
                    int(row['prediction']),
                    float(row['prediction_probability']),
                    float(row['glucose_bmi_interaction']),
                    float(row['age_glucose_interaction']),
                    float(row['insulin_glucose_interaction']),
                    float(row['age_bmi_interaction']),
                    float(row['glucose_normalized']),
                    float(row['bmi_normalized']),
                    float(row['age_normalized']),
                    float(row['bp_normalized']),
                    float(row['insulin_normalized']),
                    str(row['health_risk_level']),
                    str(row['intervention_priority']),
                    str(row['estimated_cost_category']),
                    str(row['treatment_recommendation']),
                    float(row['metabolic_syndrome_risk']),
                    float(row['cardiovascular_risk']),
                    float(row['pregnancy_risk_factor']),
                    float(row['lifestyle_risk_score']),
                    str(row['data_source']),
                    str(row['processing_version']),
                    float(row['quality_score']),
                    anomaly_flags_json,
                    row['processed_timestamp'],
                    row['processing_date']
                ))
            
            cursor.executemany(insert_query, values)
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"Successfully loaded {len(df)} records to gold layer")
            
        except Exception as e:
            logger.error(f"Failed to load data to gold layer: {e}")
            raise
            
    def update_model_metadata(self) -> None:
        """Update model metadata table with current run information"""
        try:
            connection = self.connect_postgres()
            cursor = connection.cursor()
            
            # Simulate model performance metrics
            model_data = (
                'diabetes_prediction_enhanced',
                '2.0',
                'RandomForest',
                0.85,
                0.82,
                0.78,
                0.80,
                0.88,
                0.83,
                0.87,
                0.77,
                0.85,
                0.71,
                json.dumps({
                    'glucose': 0.25, 'bmi': 0.20, 'age': 0.15, 'blood_pressure': 0.15,
                    'insulin': 0.10, 'diabetes_pedigree_function': 0.10, 'pregnancies': 0.05
                }),
                1000,
                200,
                100,
                datetime.now(),
                datetime.now(),
                'Active',
                '2.0',
                '2.0',
                'Enhanced model with comprehensive feature engineering and risk scoring'
            )
            
            cursor.execute("""
                INSERT INTO analytics.model_performance 
                (model_name, model_version, notes, training_date)
                VALUES (%s, %s, %s, %s)
            """, (
                'diabetes_prediction_enhanced',
                '2.0',
                'Enhanced ETL pipeline run completed successfully',
                datetime.now()
            ))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info("Successfully updated model metadata")
            
        except Exception as e:
            logger.error(f"Failed to update model metadata: {e}")
            
    def log_pipeline_execution(self, status: str, records_processed: int, data_quality_score: float, 
                             error_details: Optional[Dict] = None) -> None:
        """Log pipeline execution to monitoring table"""
        try:
            connection = self.connect_postgres()
            cursor = connection.cursor()
            
            end_time = datetime.now()
            start_time = getattr(self, 'start_time', end_time)
            duration = int((end_time - start_time).total_seconds())
            
            # Log to model_performance table as a monitoring entry
            cursor.execute("""
                INSERT INTO analytics.model_performance 
                (model_name, model_version, notes, training_date)
                VALUES (%s, %s, %s, %s)
            """, (
                'etl_pipeline_execution',
                '1.0',
                f'Pipeline {status}: {records_processed} records processed, quality score: {data_quality_score:.3f}',
                end_time
            ))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"Pipeline execution logged with status: {status}")
            
        except Exception as e:
            logger.error(f"Failed to log pipeline execution: {e}")
            
    def run_etl_pipeline(self) -> None:
        """Run the complete enhanced ETL pipeline"""
        self.execution_id = str(uuid.uuid4())
        self.start_time = datetime.now()
        
        try:
            logger.info("Starting Enhanced PostgreSQL ETL Pipeline")
            logger.info(f"Execution ID: {self.execution_id}")
            
            # Step 1: Read data from HDFS (simulated)
            logger.info("Step 1: Reading data from HDFS...")
            df_raw = self.read_sample_data()
            
            # Step 2: Data quality checks
            logger.info("Step 2: Performing data quality checks...")
            df_quality, avg_quality = self.data_quality_check(df_raw)
            
            # Step 3: Feature engineering
            logger.info("Step 3: Performing comprehensive feature engineering...")
            df_enhanced = self.feature_engineering(df_quality)
            
            # Step 4: Load to bronze layer
            logger.info("Step 4: Loading to bronze layer...")
            self.load_to_bronze(df_raw)
            
            # Step 5: Load to gold layer
            logger.info("Step 5: Loading to enhanced gold layer...")
            self.load_to_gold(df_enhanced)
            
            # Step 6: Update model metadata
            logger.info("Step 6: Updating model metadata...")
            self.update_model_metadata()
            
            # Step 7: Log successful execution
            self.log_pipeline_execution('Success', len(df_enhanced), avg_quality)
            
            logger.info("=" * 60)
            logger.info("Enhanced ETL Pipeline completed successfully!")
            logger.info(f"Processed {len(df_enhanced)} records")
            logger.info(f"Average quality score: {avg_quality:.3f}")
            logger.info(f"Execution time: {(datetime.now() - self.start_time).total_seconds():.2f} seconds")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            self.log_pipeline_execution('Failed', 0, 0.0, {'error': str(e)})
            raise

def main():
    """Main execution function"""
    try:
        etl = EnhancedPostgreSQLETL()
        etl.run_etl_pipeline()
        
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
