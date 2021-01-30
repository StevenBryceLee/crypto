from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField 
from pyspark.sql.types import DoubleType, StringType, TimestampType
from pyspark.sql.functions import unix_timestamp

import pandas as pd

import os


if __name__ == "__main__":
    filePaths = [''.join(["data/",file]) for file in os.listdir('data')]
    print(filePaths)
    spark = (SparkSession
        .builder
        .appName("CryptoMiner")
        .getOrCreate())

    schema = StructType([
    StructField("Date", StringType(), True), 
    StructField("Open", DoubleType(), True), 
    StructField("High", DoubleType(), True), 
    StructField("Low", DoubleType(), True), 
    StructField("Close", DoubleType(), True), 
    StructField("Volume", DoubleType(), True), 
    StructField("Market_Cap", DoubleType(), True)])
    # pandaTest = pd.read_csv(absPath) 
    # print(pandaTest.head())
    cryptoDFs = []
    fieldNames = ["Open", "High", "Low", "Close", "Volume", "market_cap"]
    for filePath in filePaths:
        coinName = filePath.split('.')[0].split('/')[-1] # Get just 'bitcoin'
        print(coinName)
        # Get individual field names for joining later
        schema = StructType([StructField("Date", StringType(), True)] +
                            [StructField('_'.join([coinName, field]), DoubleType(), True)
                            for field in fieldNames])
        cryptoDFs.append((spark.read.csv(filePath, header=True, schema = schema))
                .withColumn("Date", (unix_timestamp("Date", "MMM-dd-yyyy")
                .cast(TimestampType()))))
    # print(cryptoDFs[0].take(2))
    # print(cryptoDFs[0].schema)
    # print(type(cryptoDFs[0]))

    
    print("great success!")