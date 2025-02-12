"""Tasks after day 7."""

from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, desc, explode, split
from pyspark.sql.types import Row

"""
1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)
2. Zbadaj strukturę danych (schema, liczba rekordów itd)
3. Zamień nulle na napisy „NULL”
4. Zbadaj ile jest filmów z podziałem na rodzaje (kolumna type)
5. Zbadaj ile tytułów nakręcili poszczególni directorzy (kolumna director)
6. Zrób statystyki z podziałem na lata – kiedy nakręcono ile filmów (wyświetl w kolejności chronologicznej).
7. Określ ile jest filmów przypisanych do poszczególnych gatunków (listed_in)
"""

spark = SparkSession.builder.appName('Day 07').master('local').getOrCreate()

# ad. 1
data: DataFrame = spark.read.option('header', True).csv('./day_07/netflix_titles.csv')

# ad. 2
print('Schemat danych:')
data.printSchema()


print(f'Ilość wpisów: {data.count()}')

print('Podgląd danych:')
data.show(5)

# ad. 3
print('Zmiana typu NULL na tekst')
data_null = data.fillna('NULLaa')
data_null.show(5)

# ad. 4
print('Określenie ilości filmów wg rodzaju')
data_type = data.groupBy('type').count()
data_type.show()

# ad. 5
print('Określenie ilości filmów wg rezyserów')
data_director = data.groupBy('director').count()
data_director = data_director.sort(desc(col('count')))
data_director.show()


# ad. 6
def realese_year_to_int(value: Row, column_name: str) -> Dict:
    value: Dict = value.asDict()

    if value[column_name] is None:
        return value

    try:
        value[column_name] = int(value[column_name])
    except ValueError:
        try:
            dt = value[column_name]
            value[column_name] = datetime.strptime(dt, '%B %d, %Y').year
        except ValueError:
            value[column_name] = None

    return value


column = 'release_year'
data_years = data.rdd.map(lambda x: realese_year_to_int(x, column)).toDF()
data_years = data_years.groupBy(column).count().sort(desc(column))
data_years = data_years.dropna('any')
data_years.show()

# ad. 7
print('Określenie ilości filmów wg kategorii')
data_listed_in = data.withColumn('listed_in', split(col('listed_in'), ', '))
data_listed_in = data_listed_in.withColumn('listed_in', explode(col('listed_in')))
data_listed_in = data_listed_in.groupBy('listed_in').count()
data_listed_in = data_listed_in.sort(desc(col('count')))

data_listed_in.show()
