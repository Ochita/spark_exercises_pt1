from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DateType, IntegerType, StructField, StructType
from pyspark_test import assert_pyspark_df_equal
from pytest_mock import MockerFixture

from exercises.exercise1 import pipeline as ex1
from exercises.exercise2 import pipeline as ex2
from exercises.exercise3 import pipeline as ex3
from exercises.exercise4 import generate_dates as ex4_generate
from exercises.exercise4 import pipeline as ex4
from exercises.exercise5 import pipeline as ex5
from exercises.exercise6 import pipeline as ex6
from exercises.exercise7 import pipeline as ex7
from exercises.exercise8 import pipeline as ex8
from exercises.exercise9 import pipeline as ex9
from exercises.exercise10 import pipeline as ex10


def test_exercise1(spark_session: SparkSession) -> None:
    expected_data = [
        {
            'w': 'five',
            'ids': [2, 4, 5]
        },
        {
            'w': 'one',
            'ids': [1, 2, 3, 5]
        },
        {
            'w': 'seven',
            'ids': [3]
        },
        {
            'w': 'six',
            'ids': [5]
        }
    ]
    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('ids', f.col('ids').cast('array<integer>'))

    input_df = spark_session.read.csv('data/exercise1.csv',
                                      header='true',
                                      inferSchema='true')

    result = ex1(input_df)

    assert_pyspark_df_equal(expected_df, result, order_by=['w'])


def test_exercise2(spark_session: SparkSession) -> None:
    expected_data = [
        {'name': 'Warsaw', 'country': 'Poland', 'population': '1 764 615'},
        {'name': 'Paris', 'country': 'France', 'population': '2 206 488'},
        {'name': 'Chicago IL', 'country': 'United States',
         'population': '2 716 000'},
        {'name': 'Vilnius', 'country': 'Lithuania', 'population': '580 020'},
        {'name': 'Stockholm', 'country': 'Sweden', 'population': '972 647'},
    ]
    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    input_df = spark_session.read.csv('data/exercise2.csv',
                                      header='true',
                                      inferSchema='true')

    result = ex2(input_df)

    assert_pyspark_df_equal(expected_df, result, order_by=['name'])


def test_exercise3(spark_session: SparkSession, mocker: MockerFixture) -> None:
    curr_date = date(2019, 4, 17)

    mock = mocker.patch.object(f, 'current_date', autospec=True)
    mock.return_value = f.lit(curr_date)

    expected_data = [
        {'date_string': '08/11/2015', 'to_date': curr_date, 'diff': 1256},
        {'date_string': '09/11/2015', 'to_date': curr_date, 'diff': 1255},
        {'date_string': '09/12/2015', 'to_date': curr_date, 'diff': 1225},
    ]

    input_data = [x['date_string'] for x in expected_data]

    input_df = spark_session.sparkContext.parallelize(input_data) \
        .map(lambda x: (x,)).toDF(['date_string'])

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('diff', f.col('diff').cast('integer'))

    result = ex3(input_df)

    assert_pyspark_df_equal(expected_df, result, order_by=['diff'])


def test_exercise4(spark_session: SparkSession, mocker: MockerFixture) -> None:
    curr_date = date(2020, 1, 1)

    mock = mocker.patch('exercises.exercise4.date', autospec=True)
    mock.today.return_value = curr_date

    range_df = ex4_generate(spark_session)

    input_df = spark_session.read.csv('data/exercise4.csv',
                                      header='true',
                                      inferSchema='true')

    result = ex4(input_df, range_df) \
        .orderBy(f.col('year_month').desc()).limit(7)

    expected_data = [
        {'year_month': '202001', 'amount': 1100},
        {'year_month': '201912', 'amount': 100},
        {'year_month': '201911', 'amount': 0},
        {'year_month': '201910', 'amount': 300},
        {'year_month': '201909', 'amount': 400},
        {'year_month': '201908', 'amount': 0},
        {'year_month': '201907', 'amount': 0},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    assert_pyspark_df_equal(expected_df, result)


def test_exercise5(spark_session: SparkSession) -> None:
    schema = StructType([
        StructField('number_of_days', IntegerType()),
        StructField('date', DateType())])
    input_df = spark_session.read.csv('data/exercise5.csv',
                                      schema=schema)

    expected_data = [
        {'number_of_days': 0, 'date': date(2016, 1, 1),
         'future': date(2016, 1, 1)},
        {'number_of_days': 1, 'date': date(2016, 2, 2),
         'future': date(2016, 2, 3)},
        {'number_of_days': 2, 'date': date(2016, 3, 22),
         'future': date(2016, 3, 24)},
        {'number_of_days': 3, 'date': date(2016, 4, 25),
         'future': date(2016, 4, 28)},
        {'number_of_days': 4, 'date': date(2016, 5, 21),
         'future': date(2016, 5, 25)},
        {'number_of_days': 5, 'date': date(2016, 6, 1),
         'future': date(2016, 6, 6)},
        {'number_of_days': 6, 'date': date(2016, 3, 21),
         'future': date(2016, 3, 27)},
    ]

    result = ex5(input_df)

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('number_of_days',
                           f.col('number_of_days').cast('integer'))

    assert_pyspark_df_equal(expected_df, result)


def test_exercise6(spark_session: SparkSession) -> None:
    input_df = spark_session.read.csv('data/exercise6.csv',
                                      header='true',
                                      inferSchema='true')

    expected_data = [
        {'group': 0, 'max_id': 4},
        {'group': 1, 'max_id': 3}
    ]

    result = ex6(input_df)

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('group', f.col('group').cast('integer')) \
        .withColumn('max_id', f.col('max_id').cast('integer'))

    assert_pyspark_df_equal(expected_df, result, order_by=['group'])


def test_exercise7(spark_session: SparkSession) -> None:
    input_df = spark_session.read.csv('data/exercise7.csv',
                                      header='true',
                                      inferSchema='true')

    expected_data = [
        {'id': 9, 'name': 'Owen Boone', 'department': 'HR',
         'salary': 27, 'diff': 0},
        {'id': 1, 'name': 'Hunter Fields', 'department': 'IT',
         'salary': 15, 'diff': 60},
        {'id': 5, 'name': 'Earl Walton', 'department': 'IT',
         'salary': 40, 'diff': 35},
        {'id': 6, 'name': 'Alan Hanson', 'department': 'IT',
         'salary': 24, 'diff': 51},
        {'id': 10, 'name': 'Max McBride', 'department': 'IT',
         'salary': 75, 'diff': 0},
        {'id': 2, 'name': 'Leonard Lewis', 'department': 'Support',
         'salary': 81, 'diff': 9},
        {'id': 3, 'name': 'Jason Dawson', 'department': 'Support',
         'salary': 90, 'diff': 0},
        {'id': 4, 'name': 'Andre Grant', 'department': 'Support',
         'salary': 25, 'diff': 65},
        {'id': 7, 'name': 'Clyde Matthews', 'department': 'Support',
         'salary': 31, 'diff': 59},
        {'id': 8, 'name': 'Josephine Leonard', 'department': 'Support',
         'salary': 1, 'diff': 89},
    ]

    result = ex7(input_df)

    print(result.show())

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('id', f.col('id').cast('integer')) \
        .withColumn('salary', f.col('salary').cast('integer')) \
        .withColumn('diff', f.col('diff').cast('integer'))

    assert_pyspark_df_equal(expected_df, result, order_by=['id'])


def test_exercise8(spark_session: SparkSession) -> None:
    input_df = spark_session.read.csv('data/exercise8.csv',
                                      header='true',
                                      inferSchema='true')

    expected_data = [
        {'time': 9, 'department': 'HR',
         'items_sold': 27, 'running_total': 27},
        {'time': 1, 'department': 'IT',
         'items_sold': 15, 'running_total': 15},
        {'time': 5, 'department': 'IT',
         'items_sold': 40, 'running_total': 55},
        {'time': 6, 'department': 'IT',
         'items_sold': 24, 'running_total': 79},
        {'time': 10, 'department': 'IT',
         'items_sold': 75, 'running_total': 154},
        {'time': 2, 'department': 'Support',
         'items_sold': 81, 'running_total': 81},
        {'time': 3, 'department': 'Support',
         'items_sold': 90, 'running_total': 171},
        {'time': 4, 'department': 'Support',
         'items_sold': 25, 'running_total': 196},
        {'time': 7, 'department': 'Support',
         'items_sold': 31, 'running_total': 227},
        {'time': 8, 'department': 'Support',
         'items_sold': 1, 'running_total': 228},
    ]

    result = ex8(input_df)

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('time', f.col('time').cast('integer')) \
        .withColumn('items_sold', f.col('items_sold').cast('integer'))

    assert_pyspark_df_equal(expected_df, result, order_by=['time'])


def test_exercise9(spark_session: SparkSession) -> None:
    input_df = spark_session.read.csv('data/exercise9.csv',
                                      header='true',
                                      inferSchema='true')

    expected_data = [
        {'time': 9, 'department': 'HR',
         'items_sold': 27, 'running_total': 27, 'diff': 27},
        {'time': 1, 'department': 'IT',
         'items_sold': 15, 'running_total': 15, 'diff': 15},
        {'time': 5, 'department': 'IT',
         'items_sold': 40, 'running_total': 55, 'diff': 40},
        {'time': 6, 'department': 'IT',
         'items_sold': 24, 'running_total': 79, 'diff': 24},
        {'time': 10, 'department': 'IT',
         'items_sold': 75, 'running_total': 154, 'diff': 75},
        {'time': 2, 'department': 'Support',
         'items_sold': 81, 'running_total': 81, 'diff': 81},
        {'time': 3, 'department': 'Support',
         'items_sold': 90, 'running_total': 171, 'diff': 90},
        {'time': 4, 'department': 'Support',
         'items_sold': 25, 'running_total': 196, 'diff': 25},
        {'time': 7, 'department': 'Support',
         'items_sold': 31, 'running_total': 227, 'diff': 31},
        {'time': 8, 'department': 'Support',
         'items_sold': 1, 'running_total': 228, 'diff': 1},
    ]

    result = ex9(input_df)

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('time', f.col('time').cast('integer')) \
        .withColumn('items_sold', f.col('items_sold').cast('integer')) \
        .withColumn('running_total', f.col('running_total').cast('integer')) \
        .withColumn('diff', f.col('diff').cast('integer'))

    assert_pyspark_df_equal(expected_df, result, order_by=['time'])


def test_exercise10(spark_session: SparkSession) -> None:
    input_df = spark_session.read.csv('data/exercise10.csv',
                                      header='true',
                                      inferSchema='true')
    # task expected data seems wrong
    expected_data = [
        {'Employee': 'George', 'Salary': 80, 'Percentage': 'High'},
        {'Employee': 'George', 'Salary': 65, 'Percentage': 'High'},
        {'Employee': 'George', 'Salary': 64, 'Percentage': 'High'},
        {'Employee': 'George', 'Salary': 62, 'Percentage': 'Average'},
        {'Employee': 'George', 'Salary': 60, 'Percentage': 'Average'},
        {'Employee': 'George', 'Salary': 50, 'Percentage': 'Average'},
        {'Employee': 'George', 'Salary': 48, 'Percentage': 'Average'},
        {'Employee': 'George', 'Salary': 45, 'Percentage': 'Low'},
        {'Employee': 'George', 'Salary': 42, 'Percentage': 'Low'},
        {'Employee': 'George', 'Salary': 35, 'Percentage': 'Low'}
    ]

    result = ex10(input_df)
    print(result.show())

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('Salary', f.col('Salary').cast('integer')) \

    assert_pyspark_df_equal(expected_df, result, order_by=['Salary'])
