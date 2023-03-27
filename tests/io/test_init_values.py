from unittest import TestCase

from dataproc_sdk import DatioSchema
from pyspark.sql.dataframe import DataFrame

from exampleenginepythonqiyhbwvw.io.init_values import InitValues


class TestInitValues(TestCase):
    """
    Test class for Dataproc job entrypoint script
    """

    def test_initialize_inputs(self):
        """
        Test getProcessId is returning the correct method name.
        """

        parameters = {"clients_path": "resources/data/input/clients.csv",
                      "clients_schema": "resources/schemas/clients_schema.json",
                      "contracts_path": "resources/data/input/contracts.csv",
                      "contracts_schema": "resources/schemas/contracts_schema.json",
                      "products_path": "resources/data/input/products.csv",
                      "products_schema": "resources/schemas/products_schema.json",
                      "output_path": "resources/data/output",
                      "output_schema": "resources/schemas/output_schema.json"}

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_file, output_schema = \
            init_values.initialize_inputs(parameters)

        self.assertEqual(type(clients_df), DataFrame)
        self.assertEqual(type(contracts_df), DataFrame)
        self.assertEqual(type(products_df), DataFrame)
        self.assertEqual(type(output_file), str)
        self.assertEqual(type(output_schema), DatioSchema)
