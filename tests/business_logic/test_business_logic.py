from pyspark import Row
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as f
from pyspark.sql.window import Window

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
import pytest
from exampleenginepythonqiyhbwvw.io.init_values import InitValues
from exampleenginepythonqiyhbwvw.common import input as i
from exampleenginepythonqiyhbwvw.common import constants as c


@pytest.fixture(scope="module")
def transform():
    transform = BusinessLogic()
    yield transform


@pytest.fixture(scope="module")
def init_values():
    init_values = InitValues()
    yield init_values


@pytest.fixture(scope="module")
def clients_df(spark_test, init_values):
    params = {"clients_path": "resources/data/input/clients.csv",
              "clients_schema": "resources/schemas/clients_schema.json"}
    clients_df = init_values.get_input_df(params, "clients_path", "clients_schema")
    yield clients_df


@pytest.fixture(scope="module")
def contracts_df(spark_test, init_values):
    data = [Row("111", "A"), Row("111", "B"), Row("111", "C"),
            Row("112", "A"), Row("112", "B"), Row("112", "C"), Row("112", "D"),
            Row("113", "A"), Row("113", "B")]
    schema = StructType([
        StructField("cod_client", StringType()),
        StructField("cod_producto", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)


def test_filter_by_age_and_vip(transform, clients_df):
    # GIVEN
    # WHEN
    output_df = transform.filter_by_age_and_vip(clients_df)
    # THEN
    assert output_df.filter(i.edad() < c.THIRTY_NUMBER).count() == 0
    assert output_df.filter(i.edad() > c.FIFTY_NUMBER).count() == 0
    assert output_df.filter(i.vip() != c.TRUE_VALUE).count() == 0


def test_filter_by_number_of_contracts(transform, contracts_df):
    # GIVEN
    # WHEN
    output_df = transform.filter_by_number_of_contracts(contracts_df)
    # THEN
    assert output_df\
        .select(*output_df.columns,
                f.count(i.cod_client()).over(Window.partitionBy(i.cod_client())).alias(c.COUNT_COLUMN))\
        .filter(f.col(c.COUNT_COLUMN) <= c.THREE_NUMBER)\
        .count() == 0


def test_hash_columns(transform, contracts_df):
    # GIVEN
    # WHEN
    output_df = transform.hash_columns(contracts_df)
    # THEN
    assert "hash" in output_df.columns
    assert len(output_df.columns) == len(contracts_df.columns) + 1
