from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DecimalType, FloatType


def calculate_compound_interest(start_saldo: float, interest: int, years_amount: int, yearly_adding: int) -> float:
    saldo = 0
    try:
        for i in range(years_amount):
            if i == 0:
                saldo = start_saldo
            else:
                saldo += yearly_adding

            saldo = saldo * (1 + interest / 100)
        return saldo

    except AttributeError:
        return 0


print_udf = udf(lambda x: len(str(x)))

calculate_compound_interest_udf = udf(lambda x, y, z, a: calculate_compound_interest(x, y, z, a), FloatType())

spark = SparkSession.builder.appName('Day 10 - task 01').master('local').getOrCreate()

data = [
    ('John', 10000, 5, 2000),
    ('Anna', 12000, 7, 3000),
    ('Mike', 13000, 8, 4000),
    ('Sara', 14000, 2, 5000),
    ('Tom', 15000, 9, 6000),
]

df = spark.createDataFrame(data, ['name', 'start_saldo', 'interest', 'yearly_adding'])

result_df = (
    df.withColumn(
        '10years',
        calculate_compound_interest_udf(col('start_saldo'), col('interest'), lit(10), col('yearly_adding')).cast(
            DecimalType(10, 2)
        ),
    )
    .withColumn(
        '20years',
        calculate_compound_interest_udf(col('start_saldo'), col('interest'), lit(20), col('yearly_adding')).cast(
            DecimalType(10, 2)
        ),
    )
    .withColumn(
        '30years',
        calculate_compound_interest_udf(col('start_saldo'), col('interest'), lit(30), col('yearly_adding')).cast(
            DecimalType(10, 2)
        ),
    )
    .withColumn(
        '40years',
        calculate_compound_interest_udf(col('start_saldo'), col('interest'), lit(40), col('yearly_adding')).cast(
            DecimalType(10, 2)
        ),
    )
    .withColumn(
        '50years',
        calculate_compound_interest_udf(col('start_saldo'), col('interest'), lit(50), col('yearly_adding')).cast(
            DecimalType(10, 2)
        ),
    )
    .withColumn(
        '60years',
        calculate_compound_interest_udf(col('start_saldo'), col('interest'), lit(60), col('yearly_adding')).cast(
            DecimalType(10, 2)
        ),
    )
    .withColumn(
        '1year',
        calculate_compound_interest_udf(col('start_saldo'), col('interest'), lit(1), col('yearly_adding')).cast(
            DecimalType(10, 2)
        ),
    )
)

result_df.show()
