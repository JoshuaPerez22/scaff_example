import sys
from behave import Given, Then, When
from behave.runner import Context
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from testAT.features.environment import init_spark_session
from worker import main


@Given(u'a config file {config_file}')
def set_config(context: Context, config_file):
    conf_path = "%s/%s" % (context.resources_dir, config_file)
    context.config_file = conf_path


@When(u'execute example app file in PySpark')
def execute_app(context: Context):
    sys.argv = ["local", context.config_file]
    context.return_code = main()


@Then(u'result should be {exit_code}')
def check_exit(context: Context, exit_code):
    assert context.return_code == int(exit_code)


@Given(u'an output dataFrame located at path {output_path}')
def set_output_path(context: Context, output_path):
    context.output_path = output_path


@When(u'read the output dataFrame')
def execute_app(context: Context):
    spark = SparkSession.builder.getOrCreate()
    spark.stop()
    spark = init_spark_session()
    context.output_df = spark.read.parquet(context.output_path)


@Then(u'total columns should be equal to {tot_columns}')
def check_total_columns(context: Context, tot_columns):
    assert len(context.output_df.columns) == int(tot_columns)


@Then(u'{column_name} column should {comparator} {value}')
def check_column_values(context: Context, column_name, comparator, value):
    if comparator == ">=":
        assert context.output_df.filter(col(column_name) < float(value)).count() == 0
    if comparator == "<=":
        assert context.output_df.filter(col(column_name) > float(value)).count() == 0
    if comparator == "==":
        if value == "True":
            value = True
        elif value == "False":
            value = False
        assert context.output_df.filter(col(column_name) != value).count() == 0
    if comparator == "like":
        assert context.output_df.filter(~col(column_name).rlike(value)).count() == 0

