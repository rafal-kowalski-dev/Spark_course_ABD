from typing import List, Tuple

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkApp").master("local").getOrCreate()

data: List[Tuple[str, str, int]] = [
    ("Agnieszka", "a", 1),
    ("Bartosz", "a", 2),
    ("Celina", "a", 3),
    ("Dawid", "a", 4),
    ("Elzbieta", "a", 5),
    ("Filip", "a", 6),
    ("Grzegorz", "a", 7),
    ("Hanna", "a", 8),
]

df_data = spark.createDataFrame(data=data, schema=["first_name", "last_name", "age"])

df_data.show()
df_data.select("first_name", "age").show()
