from sklearn.datasets import fetch_openml
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as F


class MNISTDataLoader:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self):
        mnist_data = fetch_openml("mnist_784", version=1, parser="auto", cache=True)
        X = mnist_data.data[:5000]
        y = mnist_data.target[:5000]
        return X, y


class DataPreprocessor:
    def __init__(self, spark):
        self.spark = spark

    def create_dataframes(self, X, y):
        X_schema = StructType(
            [StructField(f"Pixel{i+1}", IntegerType(), True) for i in range(784)]
        )
        X = self.spark.createDataFrame(X, schema=X_schema)
        y_schema = StructType([StructField("target", IntegerType(), True)])
        y = self.spark.createDataFrame(pd.DataFrame(y.apply(int)), schema=y_schema)
        return X, y

    def add_id_column(self, X, y):
        X = X.withColumn("id", F.monotonically_increasing_id())
        y = y.withColumn("id", F.monotonically_increasing_id())
        return X, y
    
    def join_data(self, X, y):
        full_data = X.join(y, "id", "inner").drop("id")
        return full_data

    def split_data(self, full_data):
        train_data, test_data = full_data.randomSplit([0.7, 0.3], seed=42)
        return train_data, test_data


class ModelTrainer:
    def __init__(self, spark):
        self.spark = spark

    def create_pipeline(self):
        indexer = StringIndexer(inputCol="target", outputCol="indexedLabel")
        DTClassifier = DecisionTreeClassifier(labelCol="indexedLabel")
        feature_cols = [f"Pixel{i+1}" for i in range(784)]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        pipeline = Pipeline(stages=[indexer, assembler, DTClassifier])
        return pipeline

    def create_param_grid(self):
        paramGrid = (
            ParamGridBuilder()
            .addGrid(DecisionTreeClassifier.maxDepth, [5, 10, 15])
            .build()
        )
        return paramGrid

    def create_evaluator(self):
        evaluator = MulticlassClassificationEvaluator(
            labelCol="indexedLabel",
            predictionCol="prediction",
            metricName="weightedPrecision",
        )
        return evaluator

    def train_model(self, pipeline, paramGrid, evaluator, train_data):
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator,
            numFolds=3,
        )
        return cv.fit(train_data)


class ModelEvaluator:
    def __init__(self, spark):
        self.spark = spark

    def print_best_params(self, cv_model):
        bestParams = cv_model.bestModel.stages[-1].extractParamMap()
        for param, value in bestParams.items():
            print(f"{param.name}: {value}")


def main():
    spark = SparkSession.builder.appName("mnist_classification")\
        .config("spark.driver.memory", "4g")\
        .getOrCreate()

    data_loader = MNISTDataLoader(spark)
    X, y = data_loader.load_data()

    data_preprocessor = DataPreprocessor(spark)
    X, y = data_preprocessor.create_dataframes(X, y)
    X, y = data_preprocessor.add_id_column(X, y)
    full_data = data_preprocessor.join_data(X, y)
    train_data, test_data = data_preprocessor.split_data(full_data)

    model_trainer = ModelTrainer(spark)
    pipeline = model_trainer.create_pipeline()
    paramGrid = model_trainer.create_param_grid()
    evaluator = model_trainer.create_evaluator()
    cv_model = model_trainer.train_model(pipeline, paramGrid, evaluator, train_data)

    model_evaluator = ModelEvaluator(spark)
    model_evaluator.print_best_params(cv_model)

    spark.stop()

if __name__ == "__main__":
    main()


