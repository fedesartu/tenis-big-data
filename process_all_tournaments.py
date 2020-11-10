
import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import  (StructType, StructField, DateType, BooleanType, DoubleType, IntegerType, StringType, TimestampType)
from pyspark.sql.functions import col, udf

def extract_data():
    spark = SparkSession.builder.master("local[1]").appName("tenis-matches").getOrCreate()
    
    raw_file_path = os.path.join(os.path.abspath(os.path.pardir), "dataset", "raw", "all_tournaments.csv")
    tor = spark.read.csv("file:///" + raw_file_path, header = True)
    
    tor = tor.drop("prize_money").drop("currency").drop("masters")
    
    return tor
    
if __name__ == '__main__':
    df = extract_data()
    processed_file_path = os.path.join(os.path.abspath(os.path.pardir), "dataset", "processed", "all_tournaments.csv")
    df.write.format("csv").option("header", True).mode('overwrite').save("file:///" + processed_file_path)
