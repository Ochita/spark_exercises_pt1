import logging
import os
from typing import Any

import pytest
from pyspark.sql import SparkSession

os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME') \
    or '/usr/local/opt/openjdk@8'
os.environ['JAVA_OPTS'] = '-Dio.netty.tryReflectionSetAccessible=true'


@pytest.fixture(scope='session')
def spark_session(request: Any) -> SparkSession:
    """Fixture for creating a spark context."""

    spark = SparkSession \
        .builder \
        .master('local[4]') \
        .appName('pytest-pyspark-local-testing') \
        .getOrCreate()

    request.addfinalizer(lambda: spark.stop())

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
    return spark
