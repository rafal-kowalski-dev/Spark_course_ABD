from typing import List, Tuple

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Day 4 - tasks').master('local').getOrCreate()

input_data: List[Tuple[str, str, str, int]] = [
    ('James', 'Smith', 'M', 34),
    ('Maria', 'Garcia', 'F', 28),
    ('Robert', 'Johnson', 'M', 45),
    ('Lisa', 'Brown', 'F', 31),
    ('Michael', 'Davis', 'M', 29),
    ('Jennifer', 'Miller', 'F', 38),
    ('William', 'Wilson', 'M', 42),
    ('Emma', 'Anderson', 'F', 25),
    ('David', 'Taylor', 'M', 36),
    ('Sarah', 'Thomas', 'F', 33),
    ('John', 'Martinez', 'M', 41),
    ('Jessica', 'Robinson', 'F', 27),
    ('Daniel', 'White', 'M', 39),
    ('Michelle', 'Lee', 'F', 32),
]

data = spark.sparkContext.parallelize(input_data)

only_men = data.filter(lambda x: x[2] == 'M')
only_men_age = only_men.map(lambda x: x[3])

print(f'Sum of men age: {only_men_age.sum()}')


only_women = data.filter(lambda x: x[2] == 'F')
only_women_age = only_women.map(lambda x: x[3])

print(f'Sum of women age: {only_women_age.sum()}')

data_age = data.map(lambda x: x[3])

print(f'Min age: {data_age.min()}')
print(f'Max age: {data_age.max()}')
