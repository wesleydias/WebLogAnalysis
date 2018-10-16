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

spark = SparkSession.builder.appName("DSUseCase2").getOrCreate()

df7= spark.sql("select cast(request_processing_time as double),cast(backend_processing_time as double),cast(response_processing_time as double),cast(elb_status_code as double),cast(backend_status_code as double),cast(received_bytes as double),cast(sent_bytes as double),req_url,user_agent,ssl_cipher,ssl_protocol,num_unique_url_access,total_time_session from weblogs6 where client_ip is not null and request_processing_time is not null and backend_processing_time is not null and response_processing_time is not null and elb_status_code is not null and backend_status_code is not null and received_bytes is not null and sent_bytes is not null and req_url is not null and user_agent is not null and ssl_cipher is not null and ssl_protocol is not null and num_unique_url_access is not null and total_time_session is not null ")

train, test = df7.randomSplit([0.9, 0.1], seed=12345)

from  pyspark.ml.feature import StringIndexer

indexer1 = (StringIndexer()
                   .setInputCol("ssl_cipher")
                   .setOutputCol("ssl_cipher_index")
                   .fit(train))

indexer2 = (StringIndexer()
                   .setInputCol("ssl_protocol")
                   .setOutputCol("ssl_protocol_index")
                   .fit(train))

indexer3 = (StringIndexer()
                   .setInputCol("req_url")
                   .setOutputCol("req_url_index")
                   .fit(train))

indexer4 = (StringIndexer()
                   .setInputCol("user_agent")
                   .setOutputCol("user_agent_index")
                   .fit(train))

from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler()
vecAssembler.setInputCols(["request_processing_time","backend_processing_time","response_processing_time","elb_status_code","backend_status_code","received_bytes","sent_bytes","req_url_index","user_agent_index","ssl_cipher_index","ssl_protocol_index"])
vecAssembler.setOutputCol("features")
print vecAssembler.explainParams()

from pyspark.ml.regression import LinearRegression

aft = LinearRegression()
aft.setLabelCol("num_unique_url_access")
#aft.setMaxDepth(30)

print aft.explainParams()

from pyspark.ml import Pipeline

# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline()

# Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
lrPipeline.setStages([indexer1,indexer2,indexer3,indexer4,vecAssembler, aft])

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipelineModel = lrPipeline.fit(train)

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipeline = lrPipelineModel.transform(test)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrPipelineModel.stages[-1].coefficients))
print("Intercept: %s" % str(lrPipelineModel.stages[-1].intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrPipelineModel.stages[-1].summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)


spark.stop()