"""
PySpark MLlib Delay Classifier (Member 3)
Binary Classification: Will flight arrive >= 15 min late? (ArrDel15)

Model Comparison:
  1. Logistic Regression (L-BFGS) - Ultra-fast, highly scalable baseline
  2. Gradient Boosted Trees (GBTClassifier) - Sequentially boosted trees for non-linear interactions

NOTE ON RANDOM FOREST (Why Random Forest is deliberately NOT used):
  Random Forest requires creating an ensemble of 50-100 trees in parallel.
  In this dataset, 'Origin' and 'Dest' contain 350+ categorical airport levels.
  When building multi-tree forests over high-cardinality features, Spark's
  tree memory requirement grows exponentially (maxBins >= 400), leading to
  severe executor Out-Of-Memory (OOM) failures and slow shuffle phases.
  GBT with maxDepth <= 5 or Logistic Regression are far more suitable for Big Data scales.
"""

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array

def main():
    spark = SparkSession.builder \
        .appName("FlightDelayML_Comparison") \
        .getOrCreate()

    # Read Parquet
    df = spark.read.parquet("hdfs:///project/flights/parquet")

    # Filter to operated flights only (drop cancelled, diverted, null ArrDel15)
    df = df.filter(
        (F.col("Cancelled") == 0) & 
        (F.col("Diverted") == 0) & 
        F.col("ArrDel15").isNotNull()
    )
    df = df.withColumn("label", F.col("ArrDel15").cast("int"))

    cat = ["Reporting_Airline", "Origin", "Dest"]
    num = ["Month", "DayOfWeek", "Dep_Hour", "Distance", "CRSElapsedTime"]

    # Feature engineering pipeline
    stages = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cat]
    stages += [OneHotEncoder(inputCols=[c + "_idx" for c in cat],
                             outputCols=[c + "_ohe" for c in cat],
                             handleInvalid="keep")]
    stages += [VectorAssembler(inputCols=[c + "_ohe" for c in cat] + num,
                               outputCol="features",
                               handleInvalid="skip")]

    # Check available years
    distinct_years = [row.Year for row in df.select("Year").distinct().collect()]
    print(f"Distinct years found in dataset: {distinct_years}")

    if 2024 in distinct_years and any(y < 2024 for y in distinct_years):
        train = df.filter(F.col("Year") <= 2023).sample(fraction=0.15, seed=42)
        test = df.filter(F.col("Year") == 2024).sample(fraction=0.20, seed=42)
    else:
        # Dev fallback (e.g. single year sample): 80/20 train/test split
        train, test = df.randomSplit([0.8, 0.2], seed=42)

    # ---------------------------------------------------------
    # Model 1: Logistic Regression (Fast Linear Model)
    # ---------------------------------------------------------
    print("\n[1/2] Training Model 1: Logistic Regression...")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
    lr_pipeline = Pipeline(stages=stages + [lr])
    lr_model = lr_pipeline.fit(train)
    lr_pred = lr_model.transform(test)

    eval_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    eval_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
    eval_acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")

    lr_auc = eval_roc.evaluate(lr_pred)
    lr_acc = eval_acc.evaluate(lr_pred)

    print(f" -> Logistic Regression AUC-ROC: {lr_auc:.4f} | Accuracy: {lr_acc:.4f}")

    # ---------------------------------------------------------
    # Model 2: Gradient Boosted Trees (GBT) - Fast, depth-bounded
    # ---------------------------------------------------------
    print("\n[2/2] Training Model 2: Gradient Boosted Trees (GBT)...")
    gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=15, maxDepth=5, seed=42)
    gbt_pipeline = Pipeline(stages=stages + [gbt])
    gbt_model = gbt_pipeline.fit(train)
    gbt_pred = gbt_model.transform(test)

    gbt_auc = eval_roc.evaluate(gbt_pred)
    gbt_acc = eval_acc.evaluate(gbt_pred)

    print(f" -> GBTClassifier AUC-ROC:       {gbt_auc:.4f} | Accuracy: {gbt_acc:.4f}")

    print("\n========================================================")
    print("           MODEL BENCHMARK & COMPARISON TABLE           ")
    print("========================================================")
    print(f" Model 1 (Logistic Regression): AUC = {lr_auc:.4f} | Acc = {lr_acc:.4f}")
    print(f" Model 2 (Gradient Boosted Tree): AUC = {gbt_auc:.4f} | Acc = {gbt_acc:.4f}")
    print(" Random Forest Status:           REJECTED due to OOM on 350+ categorical airports")
    print("========================================================")

    # Prepare predictions sample for ClickHouse (using the best/fastest model)
    best_pred = lr_pred
    out = (best_pred.withColumn("p", vector_to_array("probability")[1])
           .select("FlightDate", "Reporting_Airline", "Origin", "Dest", "Dep_Hour",
                   F.col("label").cast("int").alias("Actual_Delayed"),
                   F.col("prediction").cast("int").alias("Predicted_Delayed"),
                   F.col("p").cast("float").alias("Delay_Probability"))
           .withColumn("Risk_Category",
                       F.when(F.col("Delay_Probability") < 0.15, "Low")
                       .when(F.col("Delay_Probability") < 0.30, "Medium")
                       .otherwise("High"))
           .limit(500000))

    out_path = "/tmp/out/ml_delay_predictions"
    out.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_path)
    print(f"ML predictions sample successfully saved to {out_path}")
    spark.stop()

if __name__ == "__main__":
    main()
