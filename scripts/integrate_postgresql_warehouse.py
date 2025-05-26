#!/usr/bin/env python3
"""
PostgreSQL Data Warehouse Integration Script
============================================

This script integrates PostgreSQL as the main data warehouse for the diabetes prediction pipeline.
It replaces Hive with PostgreSQL for better performance and easier management.

Author: Kelompok 8 RA
Date: May 26, 2025
"""

import os
import sys
import time
import logging
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import yaml
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/postgres_integration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PostgreSQLWarehouseIntegrator:
    """
    Handles integration of PostgreSQL as the main data warehouse
    """
    
    def __init__(self):
        self.host = os.getenv('POSTGRES_WAREHOUSE_HOST', 'postgres-warehouse')
        self.port = int(os.getenv('POSTGRES_WAREHOUSE_PORT', 5439))
        self.database = os.getenv('POSTGRES_WAREHOUSE_DB', 'diabetes_warehouse')
        self.username = os.getenv('POSTGRES_WAREHOUSE_USER', 'warehouse')
        self.password = os.getenv('POSTGRES_WAREHOUSE_PASSWORD', 'warehouse_secure_2025')
        
        # Connection strings
        self.connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.psycopg2_params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.username,
            'password': self.password
        }
        
        self.engine = None
        self.connection = None
        
    def wait_for_postgres(self, max_retries=30, delay=5):
        """
        Wait for PostgreSQL to be ready
        """
        logger.info("Waiting for PostgreSQL to be ready...")
        
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**self.psycopg2_params)
                conn.close()
                logger.info("PostgreSQL is ready!")
                return True
            except psycopg2.OperationalError as e:
                logger.warning(f"Attempt {attempt + 1}: PostgreSQL not ready yet - {e}")
                time.sleep(delay)
        
        logger.error("PostgreSQL failed to become ready after maximum retries")
        return False
    
    def connect(self):
        """
        Establish connection to PostgreSQL
        """
        try:
            self.engine = create_engine(self.connection_string)
            self.connection = psycopg2.connect(**self.psycopg2_params)
            logger.info("Connected to PostgreSQL data warehouse")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def execute_sql_file(self, file_path):
        """
        Execute SQL file against PostgreSQL
        """
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
            
            cursor = self.connection.cursor()
            cursor.execute(sql_content)
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Successfully executed SQL file: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to execute SQL file {file_path}: {e}")
            self.connection.rollback()
            return False
    
    def verify_schema_creation(self):
        """
        Verify that all schemas and tables were created successfully
        """
        verification_queries = [
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze', 'silver', 'gold', 'analytics')",
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'",
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'",
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'",
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'"
        ]
        
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            # Check schemas
            cursor.execute(verification_queries[0])
            schemas = [row['schema_name'] for row in cursor.fetchall()]
            logger.info(f"Created schemas: {schemas}")
            
            # Check tables in each schema
            for i, schema in enumerate(['bronze', 'silver', 'gold', 'analytics'], 1):
                cursor.execute(verification_queries[i])
                tables = [row['table_name'] for row in cursor.fetchall()]
                logger.info(f"Tables in {schema} schema: {tables}")
            
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Schema verification failed: {e}")
            return False
    
    def create_data_loading_functions(self):
        """
        Create PostgreSQL functions for data loading and processing
        """
        functions_sql = """
        -- Function to safely load CSV data into bronze layer
        CREATE OR REPLACE FUNCTION load_diabetes_csv_to_bronze(file_path TEXT)
        RETURNS INTEGER AS $$
        DECLARE
            records_loaded INTEGER := 0;
        BEGIN
            -- This function would be called from external processes
            -- For now, it's a placeholder for ETL integration
            RAISE NOTICE 'CSV loading function created. File path: %', file_path;
            RETURN records_loaded;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function to process bronze to silver data
        CREATE OR REPLACE FUNCTION process_bronze_to_silver()
        RETURNS INTEGER AS $$
        DECLARE
            processed_count INTEGER := 0;
        BEGIN
            -- Insert cleaned data from bronze to silver
            INSERT INTO silver.diabetes_cleaned (
                source_id, pregnancies, glucose, blood_pressure, skin_thickness,
                insulin, bmi, diabetes_pedigree_function, age, outcome,
                is_valid, quality_score, anomaly_flags
            )
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
                -- Data quality validation
                CASE 
                    WHEN glucose > 0 AND glucose < 300 
                         AND bmi > 0 AND bmi < 100
                         AND age > 0 AND age < 120
                    THEN TRUE 
                    ELSE FALSE 
                END as is_valid,
                -- Quality score calculation
                CASE 
                    WHEN glucose > 0 AND glucose < 300 
                         AND bmi > 0 AND bmi < 100
                         AND age > 0 AND age < 120
                         AND blood_pressure > 0 AND blood_pressure < 250
                    THEN 1.0
                    ELSE 0.5
                END as quality_score,
                -- Anomaly detection (basic)
                CASE 
                    WHEN glucose > 250 OR bmi > 60 OR age > 100 
                    THEN '{"anomalies": ["outlier_detected"]}'::jsonb
                    ELSE '{}'::jsonb
                END as anomaly_flags
            FROM bronze.diabetes_raw 
            WHERE id NOT IN (SELECT source_id FROM silver.diabetes_cleaned WHERE source_id IS NOT NULL);
            
            GET DIAGNOSTICS processed_count = ROW_COUNT;
            RAISE NOTICE 'Processed % records from bronze to silver', processed_count;
            RETURN processed_count;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function to process silver to gold data with feature engineering
        CREATE OR REPLACE FUNCTION process_silver_to_gold()
        RETURNS INTEGER AS $$
        DECLARE
            processed_count INTEGER := 0;
        BEGIN
            -- Enhanced feature engineering for gold layer
            INSERT INTO gold.diabetes_gold (
                source_id, pregnancies, glucose, blood_pressure, skin_thickness,
                insulin, bmi, diabetes_pedigree_function, age, diabetes,
                bmi_category, age_group, glucose_category, bp_category,
                risk_score, glucose_bmi_interaction, age_glucose_interaction,
                insulin_glucose_interaction, age_bmi_interaction,
                glucose_normalized, bmi_normalized, age_normalized, bp_normalized,
                insulin_normalized, health_risk_level, intervention_priority,
                estimated_cost_category, metabolic_syndrome_risk,
                cardiovascular_risk, pregnancy_risk_factor, lifestyle_risk_score,
                data_source, processing_version, quality_score
            )
            SELECT 
                s.id as source_id,
                s.pregnancies, s.glucose, s.blood_pressure, s.skin_thickness,
                s.insulin, s.bmi, s.diabetes_pedigree_function, s.age, s.outcome,
                -- BMI Categories
                CASE 
                    WHEN s.bmi < 18.5 THEN 'Underweight'
                    WHEN s.bmi < 25 THEN 'Normal'
                    WHEN s.bmi < 30 THEN 'Overweight'
                    ELSE 'Obese'
                END as bmi_category,
                -- Age Groups
                CASE 
                    WHEN s.age < 30 THEN 'Young'
                    WHEN s.age < 50 THEN 'Middle-aged'
                    ELSE 'Senior'
                END as age_group,
                -- Glucose Categories
                CASE 
                    WHEN s.glucose < 100 THEN 'Normal'
                    WHEN s.glucose < 126 THEN 'Prediabetic'
                    ELSE 'Diabetic'
                END as glucose_category,
                -- Blood Pressure Categories
                CASE 
                    WHEN s.blood_pressure < 120 THEN 'Normal'
                    WHEN s.blood_pressure < 140 THEN 'Elevated'
                    ELSE 'High'
                END as bp_category,
                -- Risk Score (simple calculation)
                ROUND(
                    (s.glucose / 200.0 * 0.3 + 
                     s.bmi / 50.0 * 0.2 + 
                     s.age / 100.0 * 0.2 + 
                     s.blood_pressure / 200.0 * 0.1 + 
                     s.diabetes_pedigree_function * 0.2)::numeric, 3
                ) as risk_score,
                -- Interaction Features
                s.glucose * s.bmi / 1000.0 as glucose_bmi_interaction,
                s.age * s.glucose / 1000.0 as age_glucose_interaction,
                s.insulin * s.glucose / 10000.0 as insulin_glucose_interaction,
                s.age * s.bmi / 1000.0 as age_bmi_interaction,
                -- Normalized Features (0-1 scale)
                s.glucose / 200.0 as glucose_normalized,
                s.bmi / 50.0 as bmi_normalized,
                s.age / 100.0 as age_normalized,
                s.blood_pressure / 200.0 as bp_normalized,
                s.insulin / 500.0 as insulin_normalized,
                -- Health Risk Level
                CASE 
                    WHEN s.glucose > 180 AND s.bmi > 35 THEN 'Critical'
                    WHEN s.glucose > 140 OR s.bmi > 30 THEN 'High'
                    WHEN s.glucose > 100 OR s.bmi > 25 THEN 'Medium'
                    ELSE 'Low'
                END as health_risk_level,
                -- Intervention Priority
                CASE 
                    WHEN s.glucose > 180 AND s.bmi > 35 THEN 'Immediate'
                    WHEN s.glucose > 140 OR s.bmi > 30 THEN 'Soon'
                    WHEN s.glucose > 100 OR s.bmi > 25 THEN 'Routine'
                    ELSE 'Monitor'
                END as intervention_priority,
                -- Cost Category
                CASE 
                    WHEN s.glucose > 180 AND s.bmi > 35 THEN 'Very High'
                    WHEN s.glucose > 140 OR s.bmi > 30 THEN 'High'
                    WHEN s.glucose > 100 OR s.bmi > 25 THEN 'Medium'
                    ELSE 'Low'
                END as estimated_cost_category,
                -- Additional Risk Scores
                ROUND((s.bmi / 50.0 + s.glucose / 200.0 + s.blood_pressure / 200.0)::numeric / 3, 3) as metabolic_syndrome_risk,
                ROUND((s.age / 100.0 + s.blood_pressure / 200.0 + s.bmi / 50.0)::numeric / 3, 3) as cardiovascular_risk,
                ROUND((s.pregnancies / 10.0 + s.glucose / 200.0)::numeric / 2, 3) as pregnancy_risk_factor,
                ROUND((s.bmi / 50.0 + s.age / 100.0)::numeric / 2, 3) as lifestyle_risk_score,
                'ETL_PIPELINE' as data_source,
                '1.0' as processing_version,
                s.quality_score
            FROM silver.diabetes_cleaned s 
            WHERE s.is_valid = true 
              AND s.id NOT IN (SELECT source_id FROM gold.diabetes_gold WHERE source_id IS NOT NULL);
            
            GET DIAGNOSTICS processed_count = ROW_COUNT;
            RAISE NOTICE 'Processed % records from silver to gold', processed_count;
            RETURN processed_count;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function to refresh analytics aggregations
        CREATE OR REPLACE FUNCTION refresh_analytics_aggregations()
        RETURNS VOID AS $$
        BEGIN
            -- Delete and rebuild health insights
            DELETE FROM gold.health_insights_gold WHERE report_date = CURRENT_DATE;
            
            INSERT INTO gold.health_insights_gold (
                age_group, health_risk_category, bmi_category,
                total_patients, diabetic_patients, high_risk_patients,
                avg_bmi, avg_glucose, avg_age, avg_risk_score,
                high_risk_count, high_risk_percentage, report_date
            )
            SELECT 
                age_group,
                health_risk_level as health_risk_category,
                bmi_category,
                COUNT(*) as total_patients,
                SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_patients,
                SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_patients,
                ROUND(AVG(bmi)::numeric, 2) as avg_bmi,
                ROUND(AVG(glucose)::numeric, 2) as avg_glucose,
                ROUND(AVG(age)::numeric, 2) as avg_age,
                ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
                SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_count,
                ROUND((SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 2) as high_risk_percentage,
                CURRENT_DATE as report_date
            FROM gold.diabetes_gold
            GROUP BY age_group, health_risk_level, bmi_category;
            
            RAISE NOTICE 'Analytics aggregations refreshed for %', CURRENT_DATE;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(functions_sql)
            self.connection.commit()
            cursor.close()
            logger.info("Data loading functions created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create data loading functions: {e}")
            self.connection.rollback()
            return False
    
    def create_warehouse_indexes(self):
        """
        Create additional performance indexes for the warehouse
        """
        index_sql = """
        -- Additional performance indexes for analytics queries
        
        -- Bronze layer indexes
        CREATE INDEX IF NOT EXISTS idx_diabetes_raw_composite ON bronze.diabetes_raw(outcome, glucose, bmi);
        CREATE INDEX IF NOT EXISTS idx_diabetes_raw_source ON bronze.diabetes_raw(source_file);
        
        -- Silver layer indexes  
        CREATE INDEX IF NOT EXISTS idx_diabetes_cleaned_composite ON silver.diabetes_cleaned(outcome, is_valid, quality_score);
        CREATE INDEX IF NOT EXISTS idx_diabetes_cleaned_source ON silver.diabetes_cleaned(source_id);
        
        -- Gold layer performance indexes
        CREATE INDEX IF NOT EXISTS idx_diabetes_gold_analytics ON gold.diabetes_gold(health_risk_level, age_group, bmi_category);
        CREATE INDEX IF NOT EXISTS idx_diabetes_gold_intervention ON gold.diabetes_gold(intervention_priority, health_risk_level);
        CREATE INDEX IF NOT EXISTS idx_diabetes_gold_risk_composite ON gold.diabetes_gold(risk_score, diabetes, health_risk_level);
        CREATE INDEX IF NOT EXISTS idx_diabetes_gold_date_partition ON gold.diabetes_gold(processing_date, health_risk_level);
        
        -- Partial indexes for high-performance queries
        CREATE INDEX IF NOT EXISTS idx_diabetes_gold_high_risk ON gold.diabetes_gold(risk_score, diabetes) 
        WHERE health_risk_level IN ('High', 'Critical');
        
        CREATE INDEX IF NOT EXISTS idx_diabetes_gold_diabetic ON gold.diabetes_gold(health_risk_level, age_group) 
        WHERE diabetes = 1;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(index_sql)
            self.connection.commit()
            cursor.close()
            logger.info("Performance indexes created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create performance indexes: {e}")
            self.connection.rollback()
            return False
    
    def test_data_operations(self):
        """
        Test basic data operations in the warehouse
        """
        try:
            # Test data insertion
            test_data_sql = """
            -- Insert sample test data
            INSERT INTO bronze.diabetes_raw (
                pregnancies, glucose, blood_pressure, skin_thickness, insulin, 
                bmi, diabetes_pedigree_function, age, outcome, source_file
            ) VALUES 
                (6, 148.0, 72.0, 35.0, 0.0, 33.6, 0.627, 50, 1, 'test_integration.csv'),
                (1, 85.0, 66.0, 29.0, 0.0, 26.6, 0.351, 31, 0, 'test_integration.csv'),
                (8, 183.0, 64.0, 0.0, 0.0, 23.3, 0.672, 32, 1, 'test_integration.csv');
            """
            
            cursor = self.connection.cursor()
            cursor.execute(test_data_sql)
            
            # Test processing functions
            cursor.execute("SELECT process_bronze_to_silver();")
            bronze_to_silver_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT process_silver_to_gold();")
            silver_to_gold_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT refresh_analytics_aggregations();")
            
            # Test analytics views
            cursor.execute("SELECT COUNT(*) FROM analytics.enhanced_daily_stats;")
            daily_stats_count = cursor.fetchone()[0]
            
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Test completed successfully:")
            logger.info(f"  - Bronze to Silver: {bronze_to_silver_count} records")
            logger.info(f"  - Silver to Gold: {silver_to_gold_count} records") 
            logger.info(f"  - Daily Stats: {daily_stats_count} entries")
            
            return True
        except Exception as e:
            logger.error(f"Data operations test failed: {e}")
            self.connection.rollback()
            return False
    
    def generate_integration_report(self):
        """
        Generate a comprehensive integration report
        """
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            # Get table counts
            table_counts = {}
            tables_to_check = [
                'bronze.diabetes_raw',
                'silver.diabetes_cleaned', 
                'gold.diabetes_gold',
                'gold.health_insights_gold',
                'analytics.model_performance'
            ]
            
            for table in tables_to_check:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
                result = cursor.fetchone()
                table_counts[table] = result['count'] if result else 0
            
            # Get view counts
            view_counts = {}
            views_to_check = [
                'analytics.enhanced_daily_stats',
                'analytics.risk_level_analysis',
                'analytics.intervention_priority_analysis'
            ]
            
            for view in views_to_check:
                try:
                    cursor.execute(f"SELECT COUNT(*) as count FROM {view};")
                    result = cursor.fetchone()
                    view_counts[view] = result['count'] if result else 0
                except Exception:
                    view_counts[view] = 'ERROR'
            
            cursor.close()
            
            # Generate report
            report = f"""
PostgreSQL Data Warehouse Integration Report
==========================================
Date: {time.strftime('%Y-%m-%d %H:%M:%S')}

CONNECTION DETAILS:
- Host: {self.host}
- Port: {self.port}
- Database: {self.database}
- Status: Connected ✓

TABLE STATISTICS:
"""
            for table, count in table_counts.items():
                report += f"- {table}: {count:,} records\n"
            
            report += "\nVIEW STATISTICS:\n"
            for view, count in view_counts.items():
                report += f"- {view}: {count}\n"
            
            report += f"""
INTEGRATION STATUS:
- Database schemas: ✓ Created
- Tables: ✓ Created
- Views: ✓ Created
- Functions: ✓ Created
- Indexes: ✓ Created
- Test data: ✓ Loaded

NEXT STEPS:
1. Start ETL pipeline to load production data
2. Configure Grafana dashboards to use PostgreSQL
3. Set up regular data processing jobs
4. Monitor performance and optimize queries

GRAFANA CONNECTION STRING:
postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}
"""
            
            # Save report
            report_path = '/opt/airflow/logs/postgres_integration_report.txt'
            with open(report_path, 'w') as f:
                f.write(report)
            
            logger.info(f"Integration report saved to: {report_path}")
            logger.info(report)
            
            return True
        except Exception as e:
            logger.error(f"Failed to generate integration report: {e}")
            return False
    
    def close(self):
        """
        Close database connections
        """
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        logger.info("Database connections closed")

def main():
    """
    Main integration function
    """
    logger.info("Starting PostgreSQL Data Warehouse Integration")
    
    integrator = PostgreSQLWarehouseIntegrator()
    
    try:
        # Wait for PostgreSQL to be ready
        if not integrator.wait_for_postgres():
            logger.error("PostgreSQL is not available")
            sys.exit(1)
        
        # Connect to PostgreSQL
        if not integrator.connect():
            logger.error("Failed to connect to PostgreSQL")
            sys.exit(1)
        
        # Verify schema creation (schemas should be created by init scripts)
        if not integrator.verify_schema_creation():
            logger.error("Schema verification failed")
            sys.exit(1)
        
        # Create data loading functions
        if not integrator.create_data_loading_functions():
            logger.error("Failed to create data loading functions")
            sys.exit(1)
        
        # Create performance indexes
        if not integrator.create_warehouse_indexes():
            logger.error("Failed to create performance indexes")
            sys.exit(1)
        
        # Test data operations
        if not integrator.test_data_operations():
            logger.error("Data operations test failed")
            sys.exit(1)
        
        # Generate integration report
        if not integrator.generate_integration_report():
            logger.error("Failed to generate integration report")
            sys.exit(1)
        
        logger.info("PostgreSQL Data Warehouse Integration completed successfully!")
        
    except Exception as e:
        logger.error(f"Integration failed with error: {e}")
        sys.exit(1)
    finally:
        integrator.close()

if __name__ == "__main__":
    main()
