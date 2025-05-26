#!/usr/bin/env python3
"""
Generate Realistic Diabetes Dataset - 3 Million Rows
====================================================

This script creates a realistic diabetes dataset with 3 million rows,
ensuring all values are within medically realistic ranges.

Features:
- Pregnancies: 0-17 (realistic range for women)
- Glucose: 70-200 mg/dL (normal to diabetic range)
- BloodPressure: 80-180 mmHg (normal to high range)
- SkinThickness: 10-99 mm (realistic skin fold thickness)
- Insulin: 0-846 (realistic insulin levels)
- BMI: 18.5-67.1 (underweight to obese range)
- DiabetesPedigreeFunction: 0.078-2.42 (genetic predisposition)
- Age: 21-81 years (adult range)
- Outcome: 0 or 1 (binary classification)

Author: Generated for Big Data Pipeline Project
Date: May 2025
"""

import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any
import os
import sys
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/data/logs/dataset_generation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class RealisticDiabetesGenerator:
    """Generate realistic diabetes dataset with proper medical constraints."""
    
    def __init__(self, target_rows: int = 3_000_000, random_seed: int = 42):
        """
        Initialize the dataset generator.
        
        Args:
            target_rows: Number of rows to generate (default: 3 million)
            random_seed: Random seed for reproducibility
        """
        self.target_rows = target_rows
        self.random_seed = random_seed
        np.random.seed(random_seed)
        
        # Define realistic ranges for each feature
        self.feature_ranges = {
            'Pregnancies': {'min': 0, 'max': 17, 'type': 'int'},
            'Glucose': {'min': 70, 'max': 200, 'type': 'float'},
            'BloodPressure': {'min': 80, 'max': 180, 'type': 'float'},
            'SkinThickness': {'min': 10, 'max': 99, 'type': 'float'},
            'Insulin': {'min': 0, 'max': 846, 'type': 'float'},
            'BMI': {'min': 18.5, 'max': 67.1, 'type': 'float'},
            'DiabetesPedigreeFunction': {'min': 0.078, 'max': 2.42, 'type': 'float'},
            'Age': {'min': 21, 'max': 81, 'type': 'int'},
            'Outcome': {'min': 0, 'max': 1, 'type': 'binary'}
        }
        
        logger.info(f"Initialized generator for {target_rows:,} rows with seed {random_seed}")
    
    def generate_pregnancies(self, size: int) -> np.ndarray:
        """Generate realistic pregnancy counts (0-17)."""
        # Most women have 0-4 pregnancies, fewer have more
        probabilities = [0.25, 0.20, 0.18, 0.15, 0.10, 0.05, 0.03, 0.02, 0.01, 0.005, 
                        0.003, 0.002, 0.001, 0.001, 0.0005, 0.0003, 0.0002, 0.0001]
        return np.random.choice(range(18), size=size, p=probabilities)
    
    def generate_glucose(self, size: int, outcome: np.ndarray) -> np.ndarray:
        """Generate glucose levels based on diabetes outcome."""
        glucose = np.zeros(size)
        
        # Non-diabetic (outcome=0): mostly normal glucose (70-125)
        non_diabetic_mask = outcome == 0
        glucose[non_diabetic_mask] = np.random.normal(95, 15, np.sum(non_diabetic_mask))
        glucose[non_diabetic_mask] = np.clip(glucose[non_diabetic_mask], 70, 125)
        
        # Diabetic (outcome=1): higher glucose levels (126-200)
        diabetic_mask = outcome == 1
        glucose[diabetic_mask] = np.random.normal(150, 25, np.sum(diabetic_mask))
        glucose[diabetic_mask] = np.clip(glucose[diabetic_mask], 126, 200)
        
        return glucose
    
    def generate_blood_pressure(self, size: int, age: np.ndarray) -> np.ndarray:
        """Generate blood pressure correlated with age."""
        # Base blood pressure increases with age
        base_bp = 90 + (age - 21) * 0.5
        noise = np.random.normal(0, 10, size)
        bp = base_bp + noise
        return np.clip(bp, 80, 180)
    
    def generate_skin_thickness(self, size: int, bmi: np.ndarray = None) -> np.ndarray:
        """Generate skin thickness, correlated with BMI if provided."""
        if bmi is not None:
            # Higher BMI tends to have higher skin thickness
            base_thickness = 15 + (bmi - 18.5) * 0.8
            noise = np.random.normal(0, 5, size)
            thickness = base_thickness + noise
        else:
            thickness = np.random.normal(25, 8, size)
        
        return np.clip(thickness, 10, 99)
    
    def generate_insulin(self, size: int, glucose: np.ndarray) -> np.ndarray:
        """Generate insulin levels correlated with glucose."""
        # Higher glucose often correlates with higher insulin
        base_insulin = (glucose - 70) * 2.5
        noise = np.random.exponential(50, size)  # Exponential distribution for insulin
        insulin = base_insulin + noise
        return np.clip(insulin, 0, 846)
    
    def generate_bmi(self, size: int, age: np.ndarray) -> np.ndarray:
        """Generate BMI with slight correlation to age."""
        # BMI tends to increase slightly with age
        base_bmi = 25 + (age - 21) * 0.1
        noise = np.random.normal(0, 5, size)
        bmi = base_bmi + noise
        return np.clip(bmi, 18.5, 67.1)
    
    def generate_diabetes_pedigree(self, size: int) -> np.ndarray:
        """Generate diabetes pedigree function (genetic predisposition)."""
        # Log-normal distribution for pedigree function
        pedigree = np.random.lognormal(mean=-0.5, sigma=0.5, size=size)
        return np.clip(pedigree, 0.078, 2.42)
    
    def generate_age(self, size: int) -> np.ndarray:
        """Generate age distribution (21-81 years)."""
        # More younger adults, fewer elderly
        ages = np.random.gamma(shape=2, scale=15, size=size) + 21
        return np.clip(ages, 21, 81).astype(int)
    
    def generate_outcome(self, size: int, glucose: np.ndarray, bmi: np.ndarray, 
                        age: np.ndarray, pedigree: np.ndarray) -> np.ndarray:
        """Generate diabetes outcome based on risk factors."""
        # Calculate diabetes probability based on risk factors
        glucose_risk = (glucose - 70) / 130  # Normalize glucose risk
        bmi_risk = np.maximum(0, (bmi - 25) / 25)  # BMI risk starts at 25
        age_risk = (age - 21) / 60  # Age risk
        pedigree_risk = pedigree / 2.42  # Genetic risk
        
        # Combine risk factors (weighted)
        total_risk = (0.4 * glucose_risk + 0.25 * bmi_risk + 
                     0.2 * age_risk + 0.15 * pedigree_risk)
        
        # Apply sigmoid function to get probability
        probability = 1 / (1 + np.exp(-5 * (total_risk - 0.5)))
        
        # Generate outcomes based on probability
        return (np.random.random(size) < probability).astype(int)
    
    def generate_dataset(self) -> pd.DataFrame:
        """Generate the complete realistic diabetes dataset."""
        logger.info(f"Starting generation of {self.target_rows:,} rows...")
        
        # Generate features in logical order to maintain correlations
        logger.info("Generating age distribution...")
        age = self.generate_age(self.target_rows)
        
        logger.info("Generating pregnancies...")
        pregnancies = self.generate_pregnancies(self.target_rows)
        
        logger.info("Generating BMI...")
        bmi = self.generate_bmi(self.target_rows, age)
        
        logger.info("Generating diabetes pedigree function...")
        pedigree = self.generate_diabetes_pedigree(self.target_rows)
        
        logger.info("Generating preliminary glucose levels...")
        # Generate initial glucose to determine outcomes
        glucose_temp = np.random.normal(120, 30, self.target_rows)
        glucose_temp = np.clip(glucose_temp, 70, 200)
        
        logger.info("Generating diabetes outcomes...")
        outcome = self.generate_outcome(self.target_rows, glucose_temp, bmi, age, pedigree)
        
        logger.info("Generating final glucose levels based on outcomes...")
        glucose = self.generate_glucose(self.target_rows, outcome)
        
        logger.info("Generating blood pressure...")
        blood_pressure = self.generate_blood_pressure(self.target_rows, age)
        
        logger.info("Generating skin thickness...")
        skin_thickness = self.generate_skin_thickness(self.target_rows, bmi)
        
        logger.info("Generating insulin levels...")
        insulin = self.generate_insulin(self.target_rows, glucose)
        
        # Create DataFrame
        logger.info("Creating DataFrame...")
        df = pd.DataFrame({
            'Pregnancies': pregnancies,
            'Glucose': np.round(glucose, 2),
            'BloodPressure': np.round(blood_pressure, 2),
            'SkinThickness': np.round(skin_thickness, 2),
            'Insulin': np.round(insulin, 2),
            'BMI': np.round(bmi, 2),
            'DiabetesPedigreeFunction': np.round(pedigree, 4),
            'Age': age,
            'Outcome': outcome
        })
        
        logger.info(f"Dataset generated successfully with shape: {df.shape}")
        return df
    
    def validate_dataset(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate the generated dataset against realistic ranges."""
        logger.info("Validating generated dataset...")
        
        validation_results = {
            'total_rows': len(df),
            'columns': list(df.columns),
            'null_values': df.isnull().sum().to_dict(),
            'outcome_distribution': df['Outcome'].value_counts().to_dict(),
            'feature_ranges': {}
        }
        
        for feature, constraints in self.feature_ranges.items():
            if feature == 'Outcome':
                continue
                
            min_val = df[feature].min()
            max_val = df[feature].max()
            mean_val = df[feature].mean()
            
            validation_results['feature_ranges'][feature] = {
                'min': min_val,
                'max': max_val,
                'mean': round(mean_val, 2),
                'within_range': (min_val >= constraints['min'] and max_val <= constraints['max'])
            }
        
        logger.info("Validation completed")
        return validation_results
    
    def save_dataset(self, df: pd.DataFrame, output_path: str) -> None:
        """Save the dataset to CSV format."""
        logger.info(f"Saving dataset to {output_path}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        # Log file size
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"Dataset saved successfully. File size: {file_size_mb:.2f} MB")
        
        # Save metadata
        metadata_path = output_path.replace('.csv', '_metadata.txt')
        with open(metadata_path, 'w') as f:
            f.write(f"Dataset Generation Metadata\n")
            f.write(f"===========================\n")
            f.write(f"Generation Date: {datetime.now()}\n")
            f.write(f"Total Rows: {len(df):,}\n")
            f.write(f"Total Columns: {len(df.columns)}\n")
            f.write(f"File Size: {file_size_mb:.2f} MB\n")
            f.write(f"Random Seed: {self.random_seed}\n")
            f.write(f"Target Rows: {self.target_rows:,}\n")
            f.write(f"\nColumn Statistics:\n")
            f.write(str(df.describe()))
        
        logger.info(f"Metadata saved to {metadata_path}")


def main():
    """Main function to generate the realistic diabetes dataset."""
    
    # Configuration
    TARGET_ROWS = 3_000_000
    OUTPUT_PATH = "/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/data/bronze/diabetes_realistic_3m.csv"
    RANDOM_SEED = 42
    
    try:
        # Initialize generator
        generator = RealisticDiabetesGenerator(
            target_rows=TARGET_ROWS,
            random_seed=RANDOM_SEED
        )
        
        # Generate dataset
        logger.info("="*60)
        logger.info("REALISTIC DIABETES DATASET GENERATOR")
        logger.info("="*60)
        
        df = generator.generate_dataset()
        
        # Validate dataset
        validation_results = generator.validate_dataset(df)
        
        # Print validation summary
        print("\n" + "="*60)
        print("DATASET VALIDATION SUMMARY")
        print("="*60)
        print(f"Total Rows: {validation_results['total_rows']:,}")
        print(f"Outcome Distribution: {validation_results['outcome_distribution']}")
        print(f"Null Values: {sum(validation_results['null_values'].values())}")
        
        print("\nFeature Range Validation:")
        for feature, stats in validation_results['feature_ranges'].items():
            status = "✓" if stats['within_range'] else "✗"
            print(f"  {status} {feature}: {stats['min']:.2f} - {stats['max']:.2f} (mean: {stats['mean']})")
        
        # Save dataset
        generator.save_dataset(df, OUTPUT_PATH)
        
        print(f"\n✓ Dataset generation completed successfully!")
        print(f"✓ Output saved to: {OUTPUT_PATH}")
        print(f"✓ File size: {os.path.getsize(OUTPUT_PATH) / (1024**2):.2f} MB")
        
    except Exception as e:
        logger.error(f"Error during dataset generation: {str(e)}")
        raise


if __name__ == "__main__":
    main()
