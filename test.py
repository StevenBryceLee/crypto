from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField 
from pyspark.sql.types import DoubleType, StringType, TimestampType, LongType
from pyspark.sql.functions import unix_timestamp, udf
from pyspark.sql.functions import udf, col, pandas_udf, unix_timestamp, lit
import pandas as pd

import os

from volatility import daily_drawdown, intraday_volatility, overall_drawdown 

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
    fieldNames = ["Open", "High", "Low", "Close", "Volume", "Market Cap"]
    for filePath in filePaths:
        coinName = filePath.split('.')[0].split('/')[-1] # Get just 'bitcoin'
        print(coinName)
        # Get individual field names for joining later
        schema = StructType([StructField("Date", StringType(), True)] +
                        [StructField(field, DoubleType(), True)
                        for field in fieldNames])
        coin_DF = (spark.read.csv(filePath, header=True, schema = schema)
                .withColumn("Date", (unix_timestamp("Date", "MMM-dd-yyyy")
                .cast(TimestampType()))))

        with_Intraday_DF = (coin_DF
                        .withColumn("Intraday_Volatility", 
                        intraday_volatility(col("Open"), col("High"), col("Low"))))

        (with_Intraday_DF.withColumn("Daily_Drawdown", 
                        daily_drawdown(col("High"), col("Low")))
                        .registerTempTable("Drawdown"))

        max_daily_drawdown = (spark.sql("SELECT MAX(Daily_Drawdown) as max FROM Drawdown")
                            .take(1)[0].__getitem__("max"))

        print(f"Maximum daily drawdown is: {str(max_daily_drawdown)}")

        # max_overall_drawdown = (overall_drawdown(col("High"), col("Low"))).toArray()
        max_overall_drawdown = (coin_DF
                            .select(overall_drawdown(col("High"), col("Low")))
                            .head()[0])

        print(f"Maximum drawdown is {max_overall_drawdown}")

