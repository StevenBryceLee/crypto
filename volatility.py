from pyspark.sql.functions import udf, col, pandas_udf
from pyspark.sql.types import DoubleType, StringType, TimestampType, LongType
from pyspark.sql import SparkSession

import pandas as pd
import numpy as np

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

def daily_drawdown_func(hi: pd.Series, lo: pd.Series) -> pd.Series:
    '''Calculates maximum loss given high and low values'''
    return (lo - hi) / hi
daily_drawdown = pandas_udf(daily_drawdown_func, returnType=DoubleType())

def intraday_volatility_func(open: pd.Series, hi: pd.Series, lo: pd.Series) -> pd.Series:
    return (open - hi).abs() + (open - lo).abs()
intraday_volatility = pandas_udf(intraday_volatility_func, returnType=DoubleType())

def overall_drawdown_func(hi: pd.Series, lo: pd.Series) -> np.double:
    '''Calculates maximum loss given high and low values as a scalar of maximum total loss'''
    return (max(hi) - min(lo)) / max(hi)
overall_drawdown = pandas_udf(overall_drawdown_func, returnType=DoubleType())