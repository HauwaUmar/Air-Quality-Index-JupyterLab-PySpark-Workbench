import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

# create a SparkSession
spark = SparkSession.builder.appName("PostgreSQL").getOrCreate()

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

# load data from a PostgreSQL table into a PySpark DataFrame
df = spark.read.jdbc(url=url, table="current_gas_info", properties=properties)

# Load the trained model
model = RandomForestClassificationModel.load("./model/rf_model")

# Select the features column
data = df.select("co", 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'aqi', 'lon', 'lat',)

assembler = VectorAssembler(inputCols=["co", 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'aqi', 'lon', 'lat',], outputCol='features')
data = assembler.transform(data)

# Make predictions
predictions = model.transform(data)

# Show the predictions
predictions.show()

# Write predictions and data to the PostgreSQL database

predictions.select("co", 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'aqi', 'lon', 'lat', 'prediction').write.format("jdbc") \
        .options(
         url=url, # jdbc:postgresql://<host>:<port>/<database>
         dbtable='forecast',
         user='aqidb',
         password='mypassword',
         driver='org.postgresql.Driver').mode("append").save()
