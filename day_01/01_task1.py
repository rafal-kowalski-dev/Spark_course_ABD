from typing import List, Tuple

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Day 1 - Task 1').getOrCreate()

data: List[Tuple[str, str, str, int, str, float]] = [
    ('Anna', 'Kowalska', '82010112345', 42, 'K', 6500.50),
    ('Piotr', 'Nowak', '79052223456', 45, 'M', 8200.00),
    ('Maria', 'Wiśniewska', '90071334567', 34, 'K', 7100.75),
    ('Jan', 'Wójcik', '85090445678', 39, 'M', 9300.25),
    ('Ewa', 'Lewandowska', '88112556789', 36, 'K', 5900.00),
    ('Tomasz', 'Kamiński', '75030667890', 49, 'M', 11200.50),
    ('Katarzyna', 'Zielińska', '92122778901', 32, 'K', 6800.25),
    ('Marek', 'Szymański', '81040889012', 43, 'M', 8900.00),
    ('Agnieszka', 'Dąbrowska', '87060990123', 37, 'K', 7600.75),
    ('Krzysztof', 'Kozłowski', '83091001234', 41, 'M', 10100.00),
]

data_header = ('first_name', 'last_name', 'PESEL', 'age', 'gender', 'monthly_income')

df = spark.createDataFrame(data, schema=data_header)

df.show()
df.select(['last_name', 'gender', 'monthly_income']).show()
