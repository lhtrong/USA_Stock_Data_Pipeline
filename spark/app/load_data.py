import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import requests
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types
import logging

# credentials_location = './credentials/service-account-key.json'


conf = SparkConf() \
    .setAppName('SparkUploadData') \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") 
    # .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .master("spark://spark:7077") \
    .config(conf=sc.getConf()) \
    .getOrCreate()



symbol = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'META', 'NFLX', 'NVDA']

url = 'https://api.twelvedata.com/time_series'

schema = types.StructType([
    types.StructField("close", types.FloatType(), True),
    types.StructField("datetime", types.TimestampType(), True),
    types.StructField("high", types.FloatType(), True),
    types.StructField("low", types.FloatType(), True),
    types.StructField("open", types.FloatType(), True),
    types.StructField("volume", types.IntegerType(), True),
    types.StructField("symbol", types.StringType(), True)
])

df_res = spark.createDataFrame([], schema = schema)
try: 
    for sym in symbol:
        params = {'symbol': sym, 'interval': '1h','date':'2019-01-10', 'apikey': '2efb4e0c80a041b794abaf4369e76869', 'timezone':'America/New_York'}

        response = requests.get(url, params=params)

        if response.status_code == requests.codes.ok:
            data = response.json()
            print(data)
            df_temp = spark.read.json(sc.parallelize([data['values']]))
            df_temp = df_temp.withColumn("symbol", F.lit(sym))
            
            # df = df.withColumn("datetime", F.to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))
    
            df_res = df_res.unionAll(df_temp)

    #         df = spark.createDataFrame(df.rdd, schema=schema)
            # Write the data to a Parquet file partitioned by date
        else:
            print('Error:', response.status_code, response.text)
    col_order = ["datetime", "symbol", "open", "high", "low", "close", "volume"]
    df_res = df_res.select(*col_order)
    df_res.coalesce(1).write.mode("overwrite").parquet("gs://dtc-de-382609_bucket/stock_data/2019/01")
except:
    logging.error('Something error')

df = spark.read.format("parquet").load("gs://dtc-de-382609_bucket/stock_data/2019/*")
df.show()

