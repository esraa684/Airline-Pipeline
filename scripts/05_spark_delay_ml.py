"""
PySpark MLlib Delay Classifier (Member 3)
Binary Classification: Will flight arrive >= 15 min late? (ArrDel15)
Strictly leak-free: Only features known before scheduled departure.
Allowed features:
  Categorical: Reporting_Airline, Origin, Dest
  Numerical: Month, DayOfWeek, Dep_Hour, Distance, CRSElapsedTime
"""
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array

def main():
    spark = SparkSession.builder \
        .appName("FlightDelayML") \
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

    stages = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cat]
    stages += [OneHotEncoder(inputCols=[c + "_idx" for c in cat],
                             outputCols=[c + "_ohe" for c in cat],
                             handleInvalid="keep")]
    stages += [VectorAssembler(inputCols=[c + "_ohe" for c in cat] + num,
                               outputCol="features",
                               handleInvalid="skip")]
    stages += [LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)]

    # Check available years
    distinct_years = [row.Year for row in df.select("Year").distinct().collect()]
    print(f"Distinct years found in dataset: {distinct_years}")

    if 2024 in distinct_years and any(y < 2024 for y in distinct_years):
        train = df.filter(F.col("Year") <= 2023).sample(fraction=0.15, seed=42)
        test = df.filter(F.col("Year") == 2024).sample(fraction=0.20, seed=42)
    else:
        # Dev fallback (e.g. single year sample): 80/20 train/test split
        train, test = df.randomSplit([0.8, 0.2], seed=42)

    print("Training Logistic Regression model...")
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(train)

    print("Evaluating model on test dataset...")
    pred = model.transform(test)

    evaluator_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    evaluator_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")

    auc = evaluator_roc.evaluate(pred)
    pr_auc = evaluator_pr.evaluate(pred)
    acc = evaluator_acc.evaluate(pred)
    f1 = evaluator_f1.evaluate(pred)

    print("========================================")
    print("      Model Evaluation Results          ")
    print("========================================")
    print(f"AUC-ROC:  {auc:.4f}  (Expected: 0.60 - 0.70 with leak-free features)")
    print(f"PR-AUC:   {pr_auc:.4f}")
    print(f"Accuracy: {acc:.4f}")
    print(f"F1-Score: {f1:.4f}")
    print("========================================")

    # Prepare predictions sample for ClickHouse (<= 500k rows)
    out = (pred.withColumn("p", vector_to_array("probability")[1])
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
