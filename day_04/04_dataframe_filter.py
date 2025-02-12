from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName('Day 04 - DF').master('local').getOrCreate()

data: DataFrame = spark.read.option('header', True).csv('./day_04/pizza_sales.csv')

pizza_with_salami = data.filter(data.pizza_ingredients.contains('Salami'))

print(pizza_with_salami.count())
print(data.count())
