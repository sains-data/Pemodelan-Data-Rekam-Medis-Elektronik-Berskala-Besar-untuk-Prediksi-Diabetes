#!/usr/bin/env python3
"""
Simplified Diabetes Prediction Model Training Script for Airflow
Runs directly in the Airflow container using pandas and scikit-learn
"""

import pandas as pd
import numpy as np
import argparse
import logging
import pickle
import os
from datetime import datetime
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.metrics import classification_report, confusion_matrix
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data_from_hdfs():
    """
    Load data from local CSV file (simulating HDFS data)
    In a real scenario, this would connect to HDFS
    """
    try:
        # For now, load from local CSV file
        data_path = '/opt/airflow/data/diabetes.csv'
        df = pd.read_csv(data_path)
        logger.info(f"Loaded data with shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def prepare_features(df):
    """
    Prepare features for training
    """
    # Define feature columns (all except the target)
    feature_columns = [col for col in df.columns if col != 'Outcome']
    
    X = df[feature_columns]
    y = df['Outcome']
    
    logger.info(f"Features shape: {X.shape}")
    logger.info(f"Target shape: {y.shape}")
    logger.info(f"Target distribution: {y.value_counts().to_dict()}")
    
    return X, y

def train_model(X, y, model_type='randomforest'):
    """
    Train the specified model
    """
    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Scale the features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train the model
    if model_type == 'randomforest':
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
    elif model_type == 'logistic':
        model = LogisticRegression(
            random_state=42,
            max_iter=1000
        )
    else:
        raise ValueError(f"Unknown model type: {model_type}")
    
    logger.info(f"Training {model_type} model...")
    model.fit(X_train_scaled, y_train)
    
    # Make predictions
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
    
    # Calculate metrics
    metrics = {
        'accuracy': accuracy_score(y_test, y_pred),
        'precision': precision_score(y_test, y_pred),
        'recall': recall_score(y_test, y_pred),
        'f1': f1_score(y_test, y_pred),
        'auc': roc_auc_score(y_test, y_pred_proba)
    }
    
    logger.info(f"Model performance metrics: {metrics}")
    
    return model, scaler, metrics, X.columns.tolist()

def save_model(model, scaler, metrics, feature_names, model_type):
    """
    Save the trained model and metadata
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_dir = f'/opt/airflow/models/{model_type}_{timestamp}'
    os.makedirs(model_dir, exist_ok=True)
    
    # Save model
    model_path = os.path.join(model_dir, 'model.pkl')
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    
    # Save scaler
    scaler_path = os.path.join(model_dir, 'scaler.pkl')
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
    
    # Save metadata
    metadata = {
        'model_type': model_type,
        'timestamp': timestamp,
        'metrics': metrics,
        'feature_names': feature_names,
        'model_path': model_path,
        'scaler_path': scaler_path
    }
    
    metadata_path = os.path.join(model_dir, 'metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Model saved to: {model_dir}")
    return model_dir

def main():
    parser = argparse.ArgumentParser(description='Train diabetes prediction model')
    parser.add_argument('--model-type', default='randomforest', 
                       choices=['randomforest', 'logistic'],
                       help='Type of model to train')
    
    args = parser.parse_args()
    
    try:
        # Load data
        df = load_data_from_hdfs()
        
        # Prepare features
        X, y = prepare_features(df)
        
        # Train model
        model, scaler, metrics, feature_names = train_model(X, y, args.model_type)
        
        # Save model
        model_dir = save_model(model, scaler, metrics, feature_names, args.model_type)
        
        logger.info("Model training completed successfully!")
        logger.info(f"Model saved to: {model_dir}")
        logger.info(f"Final metrics: {metrics}")
        
    except Exception as e:
        logger.error(f"Model training failed: {e}")
        raise

if __name__ == "__main__":
    main()
