#!/usr/bin/env python3
# filepath: /mnt/2A28ACA028AC6C8F/Programming/bigdata/Prediksi_Diabetes/scripts/predict_diabetes.py

import pandas as pd
import numpy as np
import os
import logging
import json
from datetime import datetime
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, accuracy_score, f1_score, precision_score, recall_score
from sklearn.preprocessing import StandardScaler

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/usr/local/airflow/data/logs/predict_diabetes.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('predict_diabetes')

SILVER_PATH = "/usr/local/airflow/data/silver/diabetes_processed.csv"
GOLD_PATH = "/usr/local/airflow/data/gold"


def load_processed_data():
    logger.info("Loading processed data from silver zone (pandas)")
    try:
        df = pd.read_csv(SILVER_PATH)
        logger.info(f"Data loaded successfully. Row count: {len(df)}")
        # Clean: drop rows with any NaN or inf values
        df = df.replace([np.inf, -np.inf], np.nan)
        nan_rows = df.isnull().any(axis=1).sum()
        if nan_rows > 0:
            logger.warning(f"Dropping {nan_rows} rows with NaN or inf values from processed data.")
            df = df.dropna()
        logger.info(f"Row count after dropping NaN/inf: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error loading processed data: {str(e)}")
        raise

def prepare_training_data(df):
    logger.info("Preparing training and testing data (pandas)")
    X = df.drop(["Outcome"], axis=1)
    y = df["Outcome"]
    train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.2, random_state=42)
    logger.info(f"Training set size: {len(train_X)}, Test set size: {len(test_X)}")
    return train_X, test_X, train_y, test_y

def train_and_evaluate_models(train_X, train_y, test_X, test_y):
    logger.info("Training and evaluating models (pandas/sklearn)")
    model_results = []
    # 1. Logistic Regression with GridSearchCV
    logger.info("Training Logistic Regression model")
    lr = LogisticRegression(max_iter=1000)
    param_grid = {
        'C': [0.01, 0.1, 1, 10],
        'penalty': ['l2'],
        'solver': ['lbfgs']
    }
    grid_lr = GridSearchCV(lr, param_grid, cv=3, scoring='roc_auc')
    grid_lr.fit(train_X, train_y)
    lr_pred = grid_lr.predict(test_X)
    lr_auc = roc_auc_score(test_y, grid_lr.predict_proba(test_X)[:, 1])
    model_results.append({
        "model": "LogisticRegression",
        "auc": lr_auc,
        "model_object": grid_lr,
        "pred": lr_pred
    })
    logger.info(f"Logistic Regression AUC: {lr_auc}")
    # 2. Random Forest
    logger.info("Training Random Forest model")
    rf = RandomForestClassifier(n_estimators=20, random_state=42)
    rf.fit(train_X, train_y)
    rf_pred = rf.predict(test_X)
    rf_auc = roc_auc_score(test_y, rf.predict_proba(test_X)[:, 1])
    model_results.append({
        "model": "RandomForest",
        "auc": rf_auc,
        "model_object": rf,
        "pred": rf_pred
    })
    logger.info(f"Random Forest AUC: {rf_auc}")
    # 3. Gradient Boosted Trees
    logger.info("Training Gradient Boosted Trees model")
    gbt = GradientBoostingClassifier(n_estimators=10, random_state=42)
    gbt.fit(train_X, train_y)
    gbt_pred = gbt.predict(test_X)
    gbt_auc = roc_auc_score(test_y, gbt.predict_proba(test_X)[:, 1])
    model_results.append({
        "model": "GradientBoostedTrees",
        "auc": gbt_auc,
        "model_object": gbt,
        "pred": gbt_pred
    })
    logger.info(f"GBT AUC: {gbt_auc}")
    # Select best model
    best_model = sorted(model_results, key=lambda x: x['auc'], reverse=True)[0]
    logger.info(f"Best model: {best_model['model']} with AUC: {best_model['auc']}")
    return best_model, test_X, test_y

def save_predictions(model, X, y, best_model_name):
    logger.info(f"Generating predictions using {best_model_name}")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pred_probs = model.predict_proba(X)[:, 1]
    preds = model.predict(X)
    output_data = X.copy()
    output_data["Outcome"] = y
    output_data["probability"] = pred_probs
    output_data["prediction"] = preds
    output_data["prediction_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_data["model_name"] = best_model_name
    output_path = os.path.join(GOLD_PATH, f"diabetes_predictions_{timestamp}.csv")
    os.makedirs(GOLD_PATH, exist_ok=True)
    output_data.to_csv(output_path, index=False)
    logger.info(f"Predictions saved to: {output_path}")
    # Save sample for quick analysis
    sample_path = os.path.join(GOLD_PATH, f"diabetes_predictions_{timestamp}_sample.csv")
    output_data.sample(frac=0.1, random_state=42).to_csv(sample_path, index=False)
    # Save model metrics
    metrics = {
        "accuracy": accuracy_score(y, preds),
        "f1": f1_score(y, preds),
        "precision": precision_score(y, preds),
        "recall": recall_score(y, preds),
        "auc": roc_auc_score(y, pred_probs),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model": best_model_name,
        "records_processed": len(y)
    }
    metrics_path = os.path.join(GOLD_PATH, f"metrics_{timestamp}.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f)
    logger.info(f"Model metrics: {metrics}")
    return metrics

def main():
    logger.info("Starting prediction process (pandas/sklearn)")
    try:
        df = load_processed_data()
        train_X, test_X, train_y, test_y = prepare_training_data(df)
        best_model_info, test_X, test_y = train_and_evaluate_models(train_X, train_y, test_X, test_y)
        metrics = save_predictions(
            best_model_info['model_object'],
            test_X,
            test_y,
            best_model_info['model']
        )
        logger.info("Prediction process completed successfully")
    except Exception as e:
        logger.error(f"Prediction process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()