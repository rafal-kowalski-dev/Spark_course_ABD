from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, round

spark = SparkSession.builder.appName('Day 08 - task 01').master('local').getOrCreate()

data = spark.read.option('header', True).csv('./day_04/pizza_sales.csv')

medium_pizzas = data.filter(data.pizza_size.contains('M'))

medium_pizzas.select(
    max(medium_pizzas.total_price).alias('max_price'),
    round(avg(medium_pizzas.total_price), 2).alias('avg_price'),
    min(medium_pizzas.total_price).alias('min_price'),
).show()
