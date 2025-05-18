#!/usr/bin/env python3
# filepath: /mnt/2A28ACA028AC6C8F/Programming/bigdata/Prediksi_Diabetes/scripts/predict_diabetes.py

from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.feature import VectorIndexer
from pyspark.sql.functions import col, udf, current_timestamp, lit
from pyspark.sql.types import StringType, TimestampType
import logging
import os
import json
from datetime import datetime

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/data/logs/predict_diabetes.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('predict_diabetes')

def create_spark_session():
    """Inisialisasi SparkSession dengan konfigurasi optimal"""
    logger.info("Inisialisasi Spark Session")
    return (SparkSession.builder
            .appName("Diabetes-Prediction")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "10")
            .getOrCreate())

def load_processed_data(spark):
    """Load data yang sudah diproses dari Silver Zone"""
    logger.info("Loading processed data dari silver zone")
    
    try:
        df = spark.read.parquet("/data/silver/diabetes_processed")
        logger.info(f"Data loaded successfully. Row count: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Error loading processed data: {str(e)}")
        raise

def prepare_training_data(df):
    """Persiapan data untuk training dan testing"""
    logger.info("Preparing training and testing data")
    
    # Feature indexing
    featureIndexer = VectorIndexer(inputCol="features", 
                                  outputCol="indexedFeatures", 
                                  maxCategories=10).fit(df)
    df = featureIndexer.transform(df)
    
    # Split dataset
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    logger.info(f"Training set size: {train.count()}, Test set size: {test.count()}")
    
    return train, test

def train_and_evaluate_models(train_data, test_data):
    """Train berbagai model dan evaluasi performa"""
    logger.info("Training and evaluating models")
    
    # Binary classification evaluator
    evaluator = BinaryClassificationEvaluator(
        labelCol="Outcome", 
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC")
    
    # List untuk menyimpan hasil model
    model_results = []
    
    # 1. Logistic Regression dengan CrossValidation
    logger.info("Training Logistic Regression model")
    lr = LogisticRegression(labelCol="Outcome", featuresCol="indexedFeatures", maxIter=10)
    
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 0.3]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 0.8]) \
        .build()
        
    crossval_lr = CrossValidator(
        estimator=lr,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3)
    
    cv_model_lr = crossval_lr.fit(train_data)
    lr_predictions = cv_model_lr.transform(test_data)
    lr_auc = evaluator.evaluate(lr_predictions)
    
    model_results.append({
        "model": "LogisticRegression",
        "auc": lr_auc,
        "model_object": cv_model_lr
    })
    logger.info(f"Logistic Regression AUC: {lr_auc}")
    
    # 2. Random Forest
    logger.info("Training Random Forest model")
    rf = RandomForestClassifier(labelCol="Outcome", 
                             featuresCol="indexedFeatures",
                             numTrees=20)
    rf_model = rf.fit(train_data)
    rf_predictions = rf_model.transform(test_data)
    rf_auc = evaluator.evaluate(rf_predictions)
    
    model_results.append({
        "model": "RandomForest",
        "auc": rf_auc,
        "model_object": rf_model
    })
    logger.info(f"Random Forest AUC: {rf_auc}")
    
    # 3. Gradient Boosted Trees
    logger.info("Training GBT model")
    gbt = GBTClassifier(labelCol="Outcome", 
                      featuresCol="indexedFeatures",
                      maxIter=10)
    gbt_model = gbt.fit(train_data)
    gbt_predictions = gbt_model.transform(test_data)
    gbt_auc = evaluator.evaluate(gbt_predictions)
    
    model_results.append({
        "model": "GradientBoostedTrees",
        "auc": gbt_auc,
        "model_object": gbt_model
    })
    logger.info(f"GBT AUC: {gbt_auc}")
    
    # Sortir dan pilih model terbaik
    best_model = sorted(model_results, key=lambda x: x['auc'], reverse=True)[0]
    logger.info(f"Best model: {best_model['model']} with AUC: {best_model['auc']}")
    
    return best_model

def save_predictions(model, data, best_model_name):
    """Generate dan simpan prediksi"""
    logger.info(f"Generating predictions using {best_model_name}")
    
    # Add timestamp
    timestamp_udf = udf(lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"), StringType())
    
    # Generate predictions
    predictions = model.transform(data)
    
    # Add metadata columns
    predictions = predictions.withColumn("prediction_time", current_timestamp()) \
                           .withColumn("model_name", lit(best_model_name))
    
    # Select columns untuk output
    output_data = predictions.select(
        "Pregnancies", "Glucose_imputed", "BloodPressure_imputed", 
        "SkinThickness_imputed", "Insulin_imputed", "BMI_imputed",
        "DiabetesPedigreeFunction", "Age", "Outcome", 
        "probability", "prediction", "prediction_time", "model_name"
    )
    
    # Save predictions to gold zone
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"/data/gold/diabetes_predictions_{timestamp}"
    
    output_data.write.mode("overwrite").parquet(output_path)
    logger.info(f"Predictions saved to: {output_path}")
    
    # Save sample untuk analisis cepat
    output_data.sample(fraction=0.1, seed=42).write.mode("overwrite").csv(f"{output_path}_sample.csv")
    
    # Save model metrics
    multiclass_evaluator = MulticlassClassificationEvaluator(
        labelCol="Outcome", predictionCol="prediction")
    
    metrics = {
        "accuracy": multiclass_evaluator.evaluate(output_data, {multiclass_evaluator.metricName: "accuracy"}),
        "f1": multiclass_evaluator.evaluate(output_data, {multiclass_evaluator.metricName: "f1"}),
        "precision": multiclass_evaluator.evaluate(output_data, {multiclass_evaluator.metricName: "weightedPrecision"}),
        "recall": multiclass_evaluator.evaluate(output_data, {multiclass_evaluator.metricName: "weightedRecall"}),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model": best_model_name,
        "records_processed": output_data.count()
    }
    
    with open(f"/data/gold/metrics_{timestamp}.json", "w") as f:
        json.dump(metrics, f)
    
    logger.info(f"Model metrics: {metrics}")
    return metrics

def main():
    """Main prediction process"""
    logger.info("Starting prediction process")
    
    spark = create_spark_session()
    
    try:
        # Load processed data
        df = load_processed_data(spark)
        
        # Prepare data
        train_data, test_data = prepare_training_data(df)
        
        # Train and select best model
        best_model_info = train_and_evaluate_models(train_data, test_data)
        
        # Generate predictions and save results
        metrics = save_predictions(
            best_model_info['model_object'], 
            df,  # Run prediction on entire dataset
            best_model_info['model']
        )
        
        logger.info("Prediction process completed successfully")
        
    except Exception as e:
        logger.error(f"Prediction process failed: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()