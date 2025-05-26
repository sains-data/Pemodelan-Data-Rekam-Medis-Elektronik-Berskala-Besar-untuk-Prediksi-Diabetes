#!/usr/bin/env python3
"""
Generate a realistic 3 million row diabetes dataset
Following data engineering best practices for realistic data generation
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import os

# Setup logging
log_dir = "data/logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{log_dir}/realistic_dataset_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_realistic_diabetes_data(n_samples=3_000_000):
    """
    Generate realistic diabetes dataset with proper constraints
    
    Args:
        n_samples (int): Number of samples to generate
        
    Returns:
        pd.DataFrame: Generated dataset
    """
    logger.info(f"Starting generation of {n_samples:,} realistic diabetes records")
    
    np.random.seed(42)  # For reproducibility
    
    # Generate Age first (affects other variables)
    age = np.random.randint(18, 81, n_samples)
    
    # Pregnancies (only for females, and realistic based on age)
    # Assuming 50% female, 50% male for simplicity
    # For females: age-appropriate pregnancy counts
    pregnancies = np.zeros(n_samples, dtype=int)
    female_mask = np.random.choice([True, False], n_samples, p=[0.6, 0.4])  # Slightly more females in diabetes studies
    
    for i in range(n_samples):
        if female_mask[i]:
            if age[i] < 20:
                pregnancies[i] = np.random.choice([0, 1], p=[0.8, 0.2])
            elif age[i] < 30:
                pregnancies[i] = np.random.choice([0, 1, 2, 3], p=[0.3, 0.4, 0.2, 0.1])
            elif age[i] < 40:
                pregnancies[i] = np.random.choice([0, 1, 2, 3, 4, 5], p=[0.2, 0.25, 0.25, 0.15, 0.1, 0.05])
            elif age[i] < 50:
                pregnancies[i] = np.random.choice([0, 1, 2, 3, 4, 5, 6], p=[0.15, 0.2, 0.25, 0.2, 0.1, 0.05, 0.05])
            else:
                pregnancies[i] = np.random.choice([0, 1, 2, 3, 4, 5, 6, 7], p=[0.1, 0.15, 0.25, 0.2, 0.15, 0.1, 0.03, 0.02])
        else:
            pregnancies[i] = 0  # Males have 0 pregnancies
    
    # Glucose levels (mg/dL) - Normal: 70-99, Prediabetes: 100-125, Diabetes: 126+
    glucose_base = np.random.normal(110, 25, n_samples)
    glucose = np.clip(glucose_base, 60, 300).astype(int)
    
    # Blood Pressure (mmHg) - Systolic pressure, age-correlated
    bp_base = 90 + (age - 18) * 0.5 + np.random.normal(0, 15, n_samples)
    blood_pressure = np.clip(bp_base, 60, 180).astype(int)
    
    # Skin Thickness (mm) - Triceps skin fold, BMI-correlated
    # Generate BMI first for correlation
    bmi_base = np.random.normal(28, 6, n_samples)
    bmi = np.clip(bmi_base, 15, 50)
    
    # Skin thickness correlated with BMI
    skin_base = 15 + (bmi - 20) * 0.8 + np.random.normal(0, 5, n_samples)
    skin_thickness = np.clip(skin_base, 7, 50).astype(int)
    
    # Insulin levels (μU/mL) - highly variable, correlated with glucose
    insulin_base = 50 + (glucose - 100) * 2 + np.random.exponential(30, n_samples)
    insulin = np.clip(insulin_base, 10, 600).astype(int)
    
    # Diabetes Pedigree Function - genetic predisposition (0.078 to 2.42 typical range)
    dpf = np.random.gamma(2, 0.3, n_samples)
    dpf = np.clip(dpf, 0.05, 2.5)
    
    # Outcome - binary diabetes diagnosis
    # Higher probability with: older age, higher glucose, higher BMI, family history
    risk_score = (
        (age - 18) / 63 * 0.3 +  # Age factor
        (glucose - 60) / 240 * 0.4 +  # Glucose factor
        (bmi - 15) / 35 * 0.2 +  # BMI factor
        dpf / 2.5 * 0.1  # Genetic factor
    )
    
    # Add some randomness
    risk_score += np.random.normal(0, 0.1, n_samples)
    
    # Convert to probability using logistic function
    probability = 1 / (1 + np.exp(-5 * (risk_score - 0.5)))
    outcome = np.random.binomial(1, probability, n_samples)
    
    # Create DataFrame
    df = pd.DataFrame({
        'Pregnancies': pregnancies,
        'Glucose': glucose,
        'BloodPressure': blood_pressure,
        'SkinThickness': skin_thickness,
        'Insulin': insulin,
        'BMI': np.round(bmi, 1),
        'DiabetesPedigreeFunction': np.round(dpf, 6),
        'Age': age,
        'Outcome': outcome
    })
    
    logger.info(f"Generated dataset shape: {df.shape}")
    logger.info(f"Diabetes prevalence: {df['Outcome'].mean():.3f}")
    
    return df

def validate_dataset(df):
    """Validate the generated dataset for realistic constraints"""
    logger.info("Validating dataset constraints...")
    
    validations = {
        'Pregnancies': (df['Pregnancies'] >= 0) & (df['Pregnancies'] <= 17),
        'Glucose': (df['Glucose'] >= 60) & (df['Glucose'] <= 300),
        'BloodPressure': (df['BloodPressure'] >= 60) & (df['BloodPressure'] <= 180),
        'SkinThickness': (df['SkinThickness'] >= 7) & (df['SkinThickness'] <= 50),
        'Insulin': (df['Insulin'] >= 10) & (df['Insulin'] <= 600),
        'BMI': (df['BMI'] >= 15) & (df['BMI'] <= 50),
        'DiabetesPedigreeFunction': (df['DiabetesPedigreeFunction'] >= 0.05) & (df['DiabetesPedigreeFunction'] <= 2.5),
        'Age': (df['Age'] >= 18) & (df['Age'] <= 80),
        'Outcome': df['Outcome'].isin([0, 1])
    }
    
    for col, condition in validations.items():
        invalid_count = (~condition).sum()
        if invalid_count > 0:
            logger.warning(f"{col}: {invalid_count} invalid values")
        else:
            logger.info(f"{col}: All values valid")
    
    return all(condition.all() for condition in validations.values())

def save_dataset(df, output_path):
    """Save dataset with logging and validation"""
    logger.info(f"Saving dataset to {output_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    # Log statistics
    file_size = os.path.getsize(output_path) / (1024**2)  # MB
    logger.info(f"Dataset saved successfully. File size: {file_size:.2f} MB")
    
    # Save summary statistics
    stats_path = output_path.replace('.csv', '_stats.txt')
    with open(stats_path, 'w') as f:
        f.write(f"Dataset Statistics - Generated on {datetime.now()}\n")
        f.write("="*50 + "\n\n")
        f.write(f"Shape: {df.shape}\n")
        f.write(f"File size: {file_size:.2f} MB\n\n")
        f.write("Descriptive Statistics:\n")
        f.write(str(df.describe()))
        f.write("\n\nValue Counts for Categorical Variables:\n")
        f.write(f"Outcome distribution:\n{df['Outcome'].value_counts()}\n")
        f.write(f"Pregnancies distribution:\n{df['Pregnancies'].value_counts().head(10)}\n")
    
    logger.info(f"Statistics saved to {stats_path}")

def main():
    """Main execution function"""
    try:
        logger.info("Starting realistic diabetes dataset generation")
        
        # Generate data
        df = generate_realistic_diabetes_data(n_samples=3_000_000)
        
        # Validate data
        if validate_dataset(df):
            logger.info("Dataset validation passed")
        else:
            logger.error("Dataset validation failed")
            return
        
        # Save to bronze layer (raw generated data)
        bronze_path = "data/bronze/diabetes_realistic_3m.csv"
        save_dataset(df, bronze_path)
        
        # Also backup the original if it exists
        original_path = "data/diabetes.csv"
        if os.path.exists(original_path):
            backup_path = f"data/diabetes_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            logger.info(f"Backing up original dataset to {backup_path}")
            os.rename(original_path, backup_path)
        
        # Replace the main dataset
        save_dataset(df, original_path)
        
        logger.info("Dataset generation completed successfully!")
        
        # Display sample
        print("\nSample of generated data:")
        print(df.head(10))
        print(f"\nDataset shape: {df.shape}")
        print(f"Diabetes prevalence: {df['Outcome'].mean():.3f}")
        
    except Exception as e:
        logger.error(f"Error during dataset generation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
