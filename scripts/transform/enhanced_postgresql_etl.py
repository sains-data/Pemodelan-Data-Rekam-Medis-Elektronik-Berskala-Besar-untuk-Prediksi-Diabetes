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
from psycopg2.extras import RealDictCursor
import json
import logging
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
                'pregnancies': [6, 1, 8, 1, 0, 5, 3, 10, 2, 8],
                'glucose': [148, 85, 183, 89, 137, 116, 78, 115, 197, 125],
                'blood_pressure': [72, 66, 64, 66, 40, 74, 50, 0, 70, 96],
                'skin_thickness': [35, 29, 0, 23, 35, 0, 32, 0, 45, 0],
                'insulin': [0, 0, 0, 94, 168, 0, 88, 0, 543, 0],
                'bmi': [33.6, 26.6, 23.3, 28.1, 43.1, 25.6, 31.0, 35.3, 30.5, 0],
                'diabetes_pedigree_function': [0.627, 0.351, 0.672, 0.167, 2.288, 0.201, 0.248, 0.134, 0.158, 0.232],
                'age': [50, 31, 32, 21, 33, 30, 26, 29, 53, 54],
                'diabetes': [1, 0, 1, 0, 1, 0, 1, 0, 1, 1]
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
                
                # Check for outliers using IQR method
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
                anomaly_flags.append(anomalies if anomalies else None)
            
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
            
            # Categorical features
            df_enhanced['bmi_category'] = pd.cut(
                df['bmi'], 
                bins=[0, 18.5, 25, 30, float('inf')], 
                labels=['Underweight', 'Normal', 'Overweight', 'Obese']
            ).astype(str)
            
            df_enhanced['age_group'] = pd.cut(
                df['age'], 
                bins=[0, 25, 35, 45, 55, float('inf')], 
                labels=['18-25', '26-35', '36-45', '46-55', '55+']
            ).astype(str)
            
            df_enhanced['glucose_category'] = pd.cut(
                df['glucose'], 
                bins=[0, 100, 125, 200, float('inf')], 
                labels=['Normal', 'Prediabetic', 'Diabetic', 'Severe']
            ).astype(str)
            
            df_enhanced['bp_category'] = pd.cut(
                df['blood_pressure'], 
                bins=[0, 80, 90, 100, float('inf')], 
                labels=['Normal', 'Elevated', 'High', 'Very High']
            ).astype(str)
            
            # Interaction features
            df_enhanced['glucose_bmi_interaction'] = df['glucose'] * df['bmi'] / 1000
            df_enhanced['age_glucose_interaction'] = df['age'] * df['glucose'] / 100
            df_enhanced['insulin_glucose_interaction'] = df['insulin'] * df['glucose'] / 1000
            df_enhanced['age_bmi_interaction'] = df['age'] * df['bmi'] / 100
            
            # Normalized features (Min-Max scaling to 0-1)
            for col in ['glucose', 'bmi', 'age', 'blood_pressure', 'insulin']:
                if col in df_enhanced.columns:
                    min_val = df_enhanced[col].min()
                    max_val = df_enhanced[col].max()
                    if max_val > min_val:
                        if col == 'blood_pressure':
                            df_enhanced['bp_normalized'] = (df_enhanced[col] - min_val) / (max_val - min_val)
                        else:
                            df_enhanced[f'{col}_normalized'] = (df_enhanced[col] - min_val) / (max_val - min_val)
                    else:
                        if col == 'blood_pressure':
                            df_enhanced['bp_normalized'] = 0.5
                        else:
                            df_enhanced[f'{col}_normalized'] = 0.5
            
            # Risk scoring (simplified ML-like scoring)
            df_enhanced['risk_score'] = self.calculate_risk_score(df_enhanced)
            
            # Predictions (simulated ML predictions)
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
            # Weighted risk factors
            glucose_weight = 0.25
            bmi_weight = 0.20
            age_weight = 0.15
            bp_weight = 0.15
            insulin_weight = 0.10
            pedigree_weight = 0.10
            pregnancies_weight = 0.05
            
            # Normalize each factor to 0-1 scale and calculate weighted sum
            risk_score = (
                df['glucose_normalized'] * glucose_weight +
                df['bmi_normalized'] * bmi_weight +
                df['age_normalized'] * age_weight +
                df['blood_pressure_normalized'] * bp_weight +
                df['insulin_normalized'] * insulin_weight +
                (df['diabetes_pedigree_function'] / df['diabetes_pedigree_function'].max()) * pedigree_weight +
                (df['pregnancies'] / df['pregnancies'].max() if df['pregnancies'].max() > 0 else 0) * pregnancies_weight
            )
            
            return risk_score.clip(0, 1)
            
        except Exception as e:
            logger.error(f"Risk score calculation failed: {e}")
            return pd.Series([0.5] * len(df))
            
    def calculate_metabolic_syndrome_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate metabolic syndrome risk"""
        try:
            # Simplified metabolic syndrome risk based on BMI, glucose, and blood pressure
            bmi_risk = (df['bmi'] > 30).astype(int) * 0.4
            glucose_risk = (df['glucose'] > 125).astype(int) * 0.4
            bp_risk = (df['blood_pressure'] > 90).astype(int) * 0.2
            
            return (bmi_risk + glucose_risk + bp_risk).clip(0, 1)
            
        except Exception as e:
            logger.error(f"Metabolic syndrome risk calculation failed: {e}")
            return pd.Series([0.3] * len(df))
            
    def calculate_cardiovascular_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate cardiovascular risk"""
        try:
            # Simplified cardiovascular risk
            age_risk = (df['age'] > 45).astype(int) * 0.3
            bp_risk = (df['blood_pressure'] > 90).astype(int) * 0.3
            bmi_risk = (df['bmi'] > 30).astype(int) * 0.2
            diabetes_risk = df['diabetes'] * 0.2
            
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
                
            if row['blood_pressure'] > 90:
                recommendations.append("Blood pressure management")
                
            if row['age'] > 50:
                recommendations.append("Regular health screenings")
                
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
            
            # Insert data
            for _, row in bronze_data.iterrows():
                cursor.execute("""
                    INSERT INTO bronze.diabetes_raw 
                    (pregnancies, glucose, blood_pressure, skin_thickness, insulin, 
                     bmi, diabetes_pedigree_function, age, outcome, source_file, ingestion_timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['pregnancies'], row['glucose'], row['blood_pressure'], 
                    row['skin_thickness'], row['insulin'], row['bmi'], 
                    row['diabetes_pedigree_function'], row['age'], row['diabetes'],
                    row['source_file'], row['ingestion_timestamp']
                ))
            
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
            
            # Prepare columns for insertion (matching the enhanced gold schema)
            columns = [
                'pregnancies', 'glucose', 'blood_pressure', 'skin_thickness', 'insulin', 
                'bmi', 'diabetes_pedigree_function', 'age', 'diabetes',
                'bmi_category', 'age_group', 'glucose_category', 'bp_category',
                'risk_score', 'prediction', 'prediction_probability',
                'glucose_bmi_interaction', 'age_glucose_interaction', 'insulin_glucose_interaction', 'age_bmi_interaction',
                'glucose_normalized', 'bmi_normalized', 'age_normalized', 'bp_normalized', 'insulin_normalized',
                'health_risk_level', 'intervention_priority', 'estimated_cost_category', 'treatment_recommendation',
                'metabolic_syndrome_risk', 'cardiovascular_risk', 'pregnancy_risk_factor', 'lifestyle_risk_score',
                'data_source', 'processing_version', 'quality_score', 'processed_timestamp', 'processing_date'
            ]
            
            # Handle anomaly_flags as JSONB
            for _, row in df.iterrows():
                anomaly_flags_json = json.dumps(row.get('anomaly_flags', []))
                
                placeholders = ', '.join(['%s'] * (len(columns) + 1))  # +1 for anomaly_flags
                column_names = ', '.join(columns + ['anomaly_flags'])
                
                values = [row.get(col) for col in columns] + [anomaly_flags_json]
                
                cursor.execute(f"""
                    INSERT INTO gold.diabetes_gold ({column_names})
                    VALUES ({placeholders})
                """, values)
            
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
            model_metrics = {
                'model_name': 'diabetes_prediction_enhanced',
                'model_version': '2.0',
                'model_type': 'RandomForest',
                'accuracy': 0.85,
                'precision_score': 0.82,
                'recall': 0.78,
                'f1_score': 0.80,
                'auc_roc': 0.88,
                'auc_pr': 0.83,
                'precision_class_0': 0.87,
                'precision_class_1': 0.77,
                'recall_class_0': 0.85,
                'recall_class_1': 0.71,
                'feature_importance': json.dumps({
                    'glucose': 0.25, 'bmi': 0.20, 'age': 0.15, 'blood_pressure': 0.15,
                    'insulin': 0.10, 'diabetes_pedigree_function': 0.10, 'pregnancies': 0.05
                }),
                'training_dataset_size': 1000,
                'validation_dataset_size': 200,
                'test_dataset_size': 100,
                'training_date': datetime.now(),
                'deployment_date': datetime.now(),
                'model_status': 'Active',
                'data_version': '2.0',
                'processing_pipeline_version': '2.0',
                'notes': 'Enhanced model with comprehensive feature engineering and risk scoring'
            }
            
            cursor.execute("""
                INSERT INTO gold.model_metadata_gold 
                (model_name, model_version, model_type, accuracy, precision_score, recall, f1_score, 
                 auc_roc, auc_pr, precision_class_0, precision_class_1, recall_class_0, recall_class_1,
                 feature_importance, training_dataset_size, validation_dataset_size, test_dataset_size,
                 training_date, deployment_date, model_status, data_version, processing_pipeline_version, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(model_metrics.values()))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info("Successfully updated model metadata")
            
        except Exception as e:
            logger.error(f"Failed to update model metadata: {e}")
            raise
            
    def log_pipeline_execution(self, status: str, records_processed: int, data_quality_score: float, 
                             error_details: Optional[Dict] = None) -> None:
        """Log pipeline execution to monitoring table"""
        try:
            connection = self.connect_postgres()
            cursor = connection.cursor()
            
            end_time = datetime.now()
            start_time = getattr(self, 'start_time', end_time)
            duration = int((end_time - start_time).total_seconds())
            
            monitoring_data = {
                'pipeline_name': 'enhanced_diabetes_etl',
                'pipeline_stage': 'hdfs_to_postgresql_gold',
                'execution_id': self.execution_id,
                'start_time': start_time,
                'end_time': end_time,
                'duration_seconds': duration,
                'status': status,
                'records_processed': records_processed,
                'records_valid': records_processed,  # Simplified
                'records_invalid': 0,
                'data_quality_score': data_quality_score,
                'error_count': 1 if error_details else 0,
                'warning_count': 0,
                'error_details': json.dumps(error_details) if error_details else None,
                'warning_details': None,
                'memory_usage_mb': 50.0,  # Estimated
                'cpu_usage_percent': 25.0,  # Estimated
                'input_sources': json.dumps(['hdfs://data/diabetes_dataset']),
                'output_targets': json.dumps(['postgresql://gold.diabetes_gold'])
            }
            
            cursor.execute("""
                INSERT INTO gold.pipeline_monitoring_gold 
                (pipeline_name, pipeline_stage, execution_id, start_time, end_time, duration_seconds,
                 status, records_processed, records_valid, records_invalid, data_quality_score,
                 error_count, warning_count, error_details, warning_details, memory_usage_mb,
                 cpu_usage_percent, input_sources, output_targets)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(monitoring_data.values()))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"Pipeline execution logged with status: {status}")
            
        except Exception as e:
            logger.error(f"Failed to log pipeline execution: {e}")
            
    def run_etl_pipeline(self) -> None:
        """Run the complete enhanced ETL pipeline"""
        import uuid
        self.execution_id = str(uuid.uuid4())
        self.start_time = datetime.now()
        
        try:
            logger.info("Starting Enhanced PostgreSQL ETL Pipeline")
            
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
            
            logger.info("Enhanced ETL Pipeline completed successfully!")
            logger.info(f"Processed {len(df_enhanced)} records with average quality score: {avg_quality:.3f}")
            
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
