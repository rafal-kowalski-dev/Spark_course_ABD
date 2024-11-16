from typing import List

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day 03").master("local").getOrCreate()

data: List[str] = [
    "agnieszka",
    "bartosz",
    "celina",
    "dawid",
    "elzbieta",
    "filip",
    "grzegorz",
    "hanna",
]

names_rdd = spark.sparkContext.parallelize(data)

names_sum = names_rdd.reduce(lambda x, y: x + y)

print(names_sum)
