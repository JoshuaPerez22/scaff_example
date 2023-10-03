from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment


class TestApp(TestCase):
    def test_run_experiment(self):
        parameters = {
            'products_schema': 'resources/schemas/products_schema.json',
            'clients_path': 'resources/data/input/clients.csv',
            'products_path': 'resources/data/input/products.csv',
            'contracts_path': 'resources/data/input/contracts.csv',
            'contracts_schema': 'resources/schemas/contracts_schema.json',
            'clients_schema': 'resources/schemas/clients_schema.json',
            'output_path': 'resources/data/output/final_table',
            'output_schema': 'resources/schemas/output_schema.json'}

        experiment = DataprocExperiment()
        experiment.run(**parameters)

        spark = SparkSession.builder \
            .appName("unittest_job") \
            .master("local[*]") \
            .getOrCreate()

        out_df = spark.read.parquet(parameters["output_path"])

        self.assertIsInstance(out_df, DataFrame)
