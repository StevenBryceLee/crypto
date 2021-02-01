from pyspark.sql.functions import udf, col, pandas_udf
from pyspark.sql.types import DoubleType, StringType, TimestampType, LongType
from pyspark.sql import SparkSession

import pandas as pd

# spark = (SparkSession
#     .builder
#     .appName("CryptoMiner")
#     .getOrCreate())


def squared_func(s):
    return s * s

squared = pandas_udf(squared_func, returnType=LongType())
# spark.udf.register("squared", squared, LongType())

def pandas_cubed(a: pd.Series) -> pd.Series:
    return a * a * a

cubed = pandas_udf(pandas_cubed, returnType=LongType())

def hi_low_diff_func(hi: pd.Series, lo: pd.Series) -> pd.Series:
    return hi - lo
hi_low_diff = pandas_udf(hi_low_diff_func, returnType=DoubleType())

def intraday_volatility_func(open: pd.Series, hi: pd.Series, lo: pd.Series) -> pd.Series:
    return (open - hi).abs() + (open - lo).abs()
intraday_volatility = pandas_udf(intraday_volatility_func, returnType=DoubleType())
# print(hi_low_diff_func(pd.Series([1,2]), pd.Series([0,0])))