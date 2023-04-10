import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofweek
from pyspark.sql.functions import *

# create a SparkSession
#spark = SparkSession.builder.appName("PostgreSQL").getOrCreate()
spark = SparkSession.builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.jar', "postgresql-42.2.14.jar") \
    .getOrCreate()


# Set database connection properties
host="host.docker.internal"
database="data6300"
user="aqidb"
password="mypassword"
port="5432"

# Create a JDBC URL
url = f"jdbc:postgresql://{host}:{port}/{database}"

# Set the connection properties
properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}


# Importing historic data
jdbcDF = spark.read.format("jdbc"). \
options(
         url=url, # jdbc:postgresql://<host>:<port>/<database>
         dbtable="historic_gas_info",
         user="aqidb",
         password="mypassword",
         driver='org.postgresql.Driver').\
load()


# importing current data
currentDF = spark.read.format("jdbc"). \
options(
         url=url, # jdbc:postgresql://<host>:<port>/<database>
         dbtable="current_gas_info",
         user="aqidb",
         password="mypassword",
         driver='org.postgresql.Driver').\
load()


# PREPROCESSING CONVERT DATE COLUMN TO DAY,MONTH,YEAR
newcurrentDF = currentDF.withColumn('dayOfWeek', dayofweek(col('date')))
newcurrentDF = newcurrentDF.withColumn('month', month(col('date')))
newcurrentDF = newcurrentDF.withColumn('year', year(col('date')))

jdbcDF = jdbcDF.withColumn('dayOfWeek', dayofweek(col('date')))
jdbcDF = jdbcDF.withColumn('month', month(col('date')))
jdbcDF = jdbcDF.withColumn('year', year(col('date')))

# selecting data relevant columns
model_data = jdbcDF.select("dayOfWeek","month","year","co", 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'aqi','coord_key')
assembler = VectorAssembler(inputCols=["dayOfWeek","month","year","co", 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3','coord_key'], outputCol='features')
model_data = assembler.transform(model_data)


#Split data into training and test set
train_data, test_data = model_data.randomSplit([0.7, 0.3])

rf = RandomForestClassifier(featuresCol='features', labelCol='aqi')
model = rf.fit(train_data)

predictions = model.transform(test_data)

#Evaluate the performance of the model
evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='aqi', metricName='accuracy')

# Add additional evaluation metrics
evaluator = evaluator.setMetricName('weightedPrecision').setMetricName('weightedRecall').setMetricName('f1')

# Calculate the evaluation metrics on the test set
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
weightedPrecision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
weightedRecall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

print("Accuracy = %g" % accuracy)
print("Weighted Precision = %g" % weightedPrecision)
print("Weighted Recall = %g" % weightedRecall)
print("F1 Score = %g" % f1)


current_data = newcurrentDF.select("dayOfWeek","month","year","co", 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'aqi','coord_key')
current_data = assembler.transform(current_data)

current_data_prediction = model.transform(current_data)

#Evaluate the performance of the model

evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='aqi', metricName='accuracy')

# Add additional evaluation metrics
evaluator = evaluator.setMetricName('weightedPrecision').setMetricName('weightedRecall').setMetricName('f1')

# Calculate the evaluation metrics on the test set
accuracy = evaluator.evaluate(current_data_prediction, {evaluator.metricName: "accuracy"})
weightedPrecision = evaluator.evaluate(current_data_prediction, {evaluator.metricName: "weightedPrecision"})
weightedRecall = evaluator.evaluate(current_data_prediction, {evaluator.metricName: "weightedRecall"})
f1 = evaluator.evaluate(current_data_prediction, {evaluator.metricName: "f1"})

print("Accuracy = %g" % accuracy)
print("Weighted Precision = %g" % weightedPrecision)
print("Weighted Recall = %g" % weightedRecall)
print("F1 Score = %g" % f1)


current_data_prediction = current_data_prediction.withColumn("date", F.current_date())



# upload prediction to forecast table
current_data_prediction.select("date","dayOfWeek","month","year","co", 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'aqi','coord_key','prediction').write.format("jdbc") \
        .options(
         url=url, # jdbc:postgresql://<host>:<port>/<database>
         dbtable='forecast',
         user='aqidb',
         password='mypassword',
         driver='org.postgresql.Driver').mode("append").save()



# upload current data to historic table
currentDF.select('*').write.format("jdbc") \
        .options(
         url=url,
         dbtable='historic_gas_info',
         user='aqidb',
         password='mypassword',
         driver='org.postgresql.Driver').mode("append").save()










