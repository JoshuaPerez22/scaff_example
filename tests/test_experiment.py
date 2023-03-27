from unittest import TestCase
import glob
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment


class TestApp(TestCase):
    """
    Test class for Dataproc Pyspark job entrypoint execution
    """

    def test_run_experiment(self):
        """
        Test app entrypoint execution with empty config file
        """
        parameters = {"clients_path": "resources/data/input/clients.csv",
                      "clients_schema": "resources/schemas/clients_schema.json",
                      "contracts_path": "resources/data/input/contracts.csv",
                      "contracts_schema": "resources/schemas/contracts_schema.json",
                      "products_path": "resources/data/input/products.csv",
                      "products_schema": "resources/schemas/products_schema.json",
                      "output_path": "resources/data/output",
                      "output_schema": "resources/schemas/output_schema.json"}

        experiment = DataprocExperiment()
        experiment.run(**parameters)

        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("unittest_job") \
            .master("local[*]") \
            .getOrCreate()

        out_df = spark.read.parquet(parameters["output_path"])

        self.assertIsInstance(out_df, DataFrame)
