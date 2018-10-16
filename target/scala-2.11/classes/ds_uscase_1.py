from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

from pyspark.ml.linalg import VectorUDT, DenseVector
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DSUseCase1").getOrCreate()


stocks = spark.sql("select * from weblogs5 where create_ts is not null")

assembler = VectorAssembler(inputCols=["minutes"], outputCol="minutes_features")
lr = LinearRegression(featuresCol="minutes_features")
pipeline = Pipeline(stages=[assembler, lr])

evaluator = RegressionEvaluator(metricName="mae")
grid = ParamGridBuilder().addGrid(lr.maxIter, [2,3,4]).addGrid(lr.elasticNetParam, [0., 1.]).build()
lr_cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3)

lrModel = lr_cv.fit(stocks)
detrend_model = lrModel.bestModel

print 'Intercept\t ... %.3f' % detrend_model.stages[-1].intercept
print 'Coefficients\t ... %.3f' % detrend_model.stages[-1].coefficients[0]

stocks = detrend_model.transform(stocks)
print 'rmse\t ... %.3f' % evaluator.evaluate(stocks)
print 'r2\t ... %.3f' % evaluator.evaluate(stocks, {evaluator.metricName: "r2"})

detrended = stocks.withColumn("close_detrend", col("minutes")-col("prediction")).select("create_ts", "req_url","minutes", "number_req", "close_detrend", "prediction")

#Normalizing data

assembler = VectorAssembler(inputCols=["close_detrend"], outputCol="features1")
scaler = StandardScaler(inputCol="features1", outputCol="scaledFeatures1", withStd=True, withMean=True)

pipeline = Pipeline(stages=[assembler, scaler])
scale_model = pipeline.fit(detrended)

detrended = scale_model.transform(detrended)

def to_val(col):
    def to_val_(v):
        return v.toArray().tolist()[0]
    return udf(to_val_, DoubleType())(col)

scaled = detrended.withColumn("close_detrend_scale", to_val(col("scaledFeatures1")))
scaled = scaled.select("create_ts", "minutes", "number_req", "req_url", "close_detrend_scale")

print 'std\t ... %.3f' % scale_model.stages[-1].std[0]
print 'mean\t ... %.3f' % scale_model.stages[-1].mean[0]


spark.stop()