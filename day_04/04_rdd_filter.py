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

names_gt_5 = names_rdd.filter(lambda x: len(x) > 5)

print(names_gt_5.take(10))
