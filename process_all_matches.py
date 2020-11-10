import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import  (StructType, StructField, DateType, BooleanType, DoubleType, IntegerType, StringType, TimestampType)
from pyspark.sql.functions import col, udf
import os

def extract_data():
    spark = SparkSession.builder.master("local[1]").appName("tenis-matches").getOrCreate()
    
    raw_file_path = os.path.join(os.path.abspath(os.path.pardir), "dataset", "raw", "all_matches.csv")
    matches = spark.read.csv("file:///" + raw_file_path, header = True)
    
    matches = matches.drop("end_date")\
                .drop("location")\
                .drop("prize_money")\
                .drop("currency")\
                .drop("round")\
                .drop("serve_rating")\
                .drop("aces")\
                .drop("double_faults")\
                .drop("first_serve_made")\
                .drop("first_serve_attempted")\
                .drop("first_serve_points_made")\
                .drop("first_serve_points_attempted")\
                .drop("second_serve_points_made")\
                .drop("second_serve_points_attempted")\
                .drop("break_points_saved")\
                .drop("break_points_against")\
                .drop("service_games_won")\
                .drop("return_rating")\
                .drop("first_serve_return_points_made")\
                .drop("first_serve_return_points_attempted")\
                .drop("second_serve_return_points_made")\
                .drop("second_serve_return_points_attempted")\
                .drop("break_points_made")\
                .drop("break_points_attempted")\
                .drop("return_games_played")\
                .drop("service_points_won")\
                .drop("service_points_attempted")\
                .drop("return_points_won")\
                .drop("return_points_attempted")\
                .drop("total_points_won")\
                .drop("total_points")\
                .drop("duration")\
                .drop("seed")\
                .drop("masters")\
                .drop("round_num")\
                .drop("nation")\
                .drop("player_name")\
                .drop("opponent_name")
    
    matches = matches.na.drop()
        
    boolean_columns = ["player_victory", "retirement", "won_first_set", "doubles"]
    matches = parse_boolean_columns(matches, boolean_columns)
    
    return matches

def parse_boolean_columns(df, columns):
    boolParse = udf(lambda x: True if x == "t" else False, BooleanType())
    for column in columns:
        df = df.withColumn(column, boolParse(col(column)))
    return df

if __name__ == '__main__':
    df = extract_data()
    processed_file_path = os.path.join(os.path.abspath(os.path.pardir), "dataset", "processed", "all_matches.csv")
    df.write.format("csv").option("header", True).mode('overwrite').save("file:///" + processed_file_path)
