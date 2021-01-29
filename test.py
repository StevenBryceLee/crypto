from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField 
from pyspark.sql.types import IntegerType, TimestampType

import os


if __name__ == "__main__":
    datasets = [file for file in os.listdir('data')]
    print(datasets)
    spark = (SparkSession
        .builder
        .appName("CryptoMiner")
        .getOrCreate())

    schema = StructType([
    StructField("Date", TimestampType(), True), 
    StructField("Open", IntegerType(), True), 
    StructField("High", IntegerType(), True), 
    StructField("Low", IntegerType(), True), 
    StructField("Close", IntegerType(), True), 
    StructField("Volume", IntegerType(), True), 
    StructField("Market Cap", IntegerType(), True)])

    print("success")