"""Task 01 after day 09."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf
from pyspark.sql.types import IntegerType


def count_words(text: str) -> int:
    try:
        return len(text.split(' '))
    except AttributeError:
        return 0


def count_chars_without_spaces(text: str) -> int:
    try:
        return len(text.replace(' ', ''))
    except AttributeError:
        return 0


spark = SparkSession.builder.appName('Day 09').master('local').getOrCreate()

data = spark.read.option('header', True).csv('./day_07/netflix_titles.csv')

count_words_udf = udf(lambda x: count_words(x), IntegerType())
count_chars_without_spaces_udf = udf(lambda x: count_chars_without_spaces(x), IntegerType())

new_data = data.select(
    col('title'),
    count_words_udf(col('description')).alias('words'),
    count_chars_without_spaces_udf(col('description')).alias('chars'),
)

new_data = new_data.sort(desc(col('words')), desc(col('chars')))

new_data.show(truncate=False)
