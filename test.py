from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField 
from pyspark.sql.types import DoubleType, StringType, TimestampType, LongType
from pyspark.sql.functions import unix_timestamp, udf
from pyspark.sql.functions import udf, col, pandas_udf, unix_timestamp
import pandas as pd

import os

from volatility import hi_low_diff, intraday_volatility

if __name__ == "__main__":
    filePaths = [''.join(["data/",file]) for file in os.listdir('data')]
    print(filePaths)
    spark = (SparkSession
        .builder
        .appName("CryptoMiner")
        .getOrCreate())

    # spark.udf.register("squared", squared, LongType())
    # cubed_udf = pandas_udf(pandas_cubed, returnType=LongType())
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
        coin_DF = (spark.read.csv(filePath, header=True, schema = schema)
                .withColumn("Date", (unix_timestamp("Date", "MMM-dd-yyyy")
                .cast(TimestampType()))))
        test = (coin_DF.withColumn("x", col(coinName + "_Open")))
        print(test.take(2))
        # with_Intraday_DF = (coin_DF
        # .withColumn("Intraday_Volatility", 
        #                 intraday_volatility(col(coinName + "_Open"), 
        #                 col(coinName + "_High"), col(coinName + "_Low"))))

        # print(with_Intraday_DF.take(2))
    # print(cryptoDFs[0].take(2))
    # print(cryptoDFs[0].schema)
    # print(type(cryptoDFs[0]))


    # print("great success!")