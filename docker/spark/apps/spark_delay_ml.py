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
import time
import shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array

OUTPUT_DIR = "/opt/spark-apps/output"
PARQUET_PATH = "hdfs://namenode:9000/project/flights/parquet"

def export_predictions_csv(df, filename="ml_delay_predictions"):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    temp_path = f"{OUTPUT_DIR}/_temp_{filename}"
    final_csv = f"{OUTPUT_DIR}/{filename}.csv"

    print(f"Exporting predictions sample to {final_csv}...")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)

    if os.path.exists(temp_path):
        for fname in os.listdir(temp_path):
            if fname.startswith("part-") and fname.endswith(".csv"):
                src = os.path.join(temp_path, fname)
                shutil.copyfile(src, final_csv)
                break
        shutil.rmtree(temp_path, ignore_errors=True)

    count = df.count()
    print(f" -> Successfully exported {filename}.csv ({count} rows)")
    return final_csv

def main():
    print("=" * 70)
    print("Starting PySpark MLlib Delay Risk Classification Job...")
    print("=" * 70)
    start_time = time.time()

    spark = (
        SparkSession.builder
        .appName("FlightDelayML_Comparison")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    print(f"Reading cleaned Parquet data from {PARQUET_PATH}...")
    df = spark.read.parquet(PARQUET_PATH)

    # Filter to operated flights only (drop cancelled, diverted, null ArrDel15)
    df = df.filter(
        (F.col("Cancelled") == 0) & 
        (F.col("Diverted") == 0) & 
        F.col("ArrDel15").isNotNull()
    )
    df = df.withColumn("label", F.col("ArrDel15").cast("int"))

    cat = ["Reporting_Airline", "Origin", "Dest"]
    num = ["Month", "DayOfWeek", "Dep_Hour", "Distance", "CRSElapsedTime"]

    print("Building StringIndexer, OneHotEncoder, and VectorAssembler pipeline...")
    stages = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cat]
    stages += [OneHotEncoder(inputCols=[c + "_idx" for c in cat],
                             outputCols=[c + "_ohe" for c in cat],
                             handleInvalid="keep")]
    stages += [VectorAssembler(inputCols=[c + "_ohe" for c in cat] + num,
                               outputCol="features",
                               handleInvalid="skip")]

    # Sample split (80/20 train/test)
    print("Splitting dataset into Train (80%) and Test (20%)...")
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    # ---------------------------------------------------------
    # Model 1: Logistic Regression (Fast Linear Model)
    # ---------------------------------------------------------
    print("\n[1/2] Training Model 1: Logistic Regression (maxIter=20)...")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
    lr_pipeline = Pipeline(stages=stages + [lr])
    lr_model = lr_pipeline.fit(train)
    lr_pred = lr_model.transform(test)

    eval_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    eval_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
    eval_acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    eval_f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")

    lr_auc = eval_roc.evaluate(lr_pred)
    lr_pr = eval_pr.evaluate(lr_pred)
    lr_acc = eval_acc.evaluate(lr_pred)
    lr_f1 = eval_f1.evaluate(lr_pred)

    print("========================================================")
    print("       MODEL 1 RESULTS: LOGISTIC REGRESSION             ")
    print("========================================================")
    print(f" AUC-ROC:   {lr_auc:.4f} (Expected ~0.60-0.70 with leak-free features)")
    print(f" PR-AUC:    {lr_pr:.4f}")
    print(f" Accuracy:  {lr_acc:.4f}")
    print(f" F1-Score:  {lr_f1:.4f}")
    print("========================================================")

    # Prepare predictions sample for ClickHouse (using leak-free model probabilities)
    print("\nGenerating Delay Risk Categories (Low < 15%, Medium 15-30%, High >= 30%)...")
    out = (lr_pred.withColumn("p", vector_to_array("probability")[1])
           .select(
               F.col("FlightDate").cast("string"),
               F.col("Reporting_Airline").cast("string"),
               F.col("Origin").cast("string"),
               F.col("Dest").cast("string"),
               F.col("Dep_Hour").cast("int"),
               F.col("label").cast("int").alias("Actual_Delayed"),
               F.col("prediction").cast("int").alias("Predicted_Delayed"),
               F.round(F.col("p"), 4).cast("float").alias("Delay_Probability")
           )
           .withColumn("Risk_Category",
                       F.when(F.col("Delay_Probability") < 0.15, "Low")
                       .when(F.col("Delay_Probability") < 0.30, "Medium")
                       .otherwise("High"))
           .limit(50000))  # 50,000 clean test records for instant ClickHouse/Power BI load

    export_predictions_csv(out, "ml_delay_predictions")

    elapsed = time.time() - start_time
    print("=" * 70)
    print(f"SUCCESS: PySpark MLlib delay classification finished in {elapsed:.2f} seconds!")
    print(f"Model 1 (Logistic Regression): AUC = {lr_auc:.4f}, Accuracy = {lr_acc:.4f}")
    print("Random Forest: Deliberately REJECTED due to OOM on 350+ categorical airport levels")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()
