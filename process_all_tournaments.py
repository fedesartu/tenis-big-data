
import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import  (StructType, StructField, DateType, BooleanType, DoubleType, IntegerType, StringType, TimestampType)
from pyspark.sql.functions import col, unix_timestamp, to_date, when, lit, udf, coalesce
import datetime

def extract_data():
    spark = SparkSession.builder.master("local[1]").appName("tenis-matches").getOrCreate()
    
    raw_file_path = os.path.join(os.path.abspath(os.path.pardir), "dataset", "raw", "all_tournaments.csv")
    tor = spark.read.csv("file:///" + raw_file_path, header = True)
    
    tor = tor.drop("prize_money").drop("currency").drop("masters")
    
    tor = removeCharacter(tor, "tournament", '"')
    
    tor = tor.withColumn('start_date', to_date(unix_timestamp(col('start_date'), 'yyyy-mm-dd').cast("timestamp"))).withColumn('end_date', to_date(unix_timestamp(col('end_date'), 'yyyy-mm-dd').cast("timestamp")))
    tor = tor.withColumn("start_date", when(col("start_date").isNull(), lit(datetime.datetime(1974, 1, 1))).otherwise(col("start_date")))
    tor = tor.withColumn('start_date', to_date(unix_timestamp(col('start_date'), 'yyyy-mm-dd').cast("timestamp"))).withColumn('end_date', to_date(unix_timestamp(col('end_date'), 'yyyy-mm-dd').cast("timestamp")))

    tor = tor.withColumn("end_date", coalesce(tor["end_date"], tor["start_date"]))
    
    return tor

def removeCharacter(df, column, char):
    removeFn = udf(lambda x: x.replace(char, ''), StringType())
    return df.withColumn(column, removeFn(col(column)))
    
if __name__ == '__main__':
    df = extract_data()
    processed_file_path = os.path.join(os.path.abspath(os.path.pardir), "dataset", "processed", "all_tournaments.csv")
    df.write.format("csv").option("header", True).mode('overwrite').save("file:///" + processed_file_path)
