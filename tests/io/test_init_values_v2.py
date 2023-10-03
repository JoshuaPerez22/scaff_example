from unittest import TestCase
from unittest.mock import MagicMock

import pytest
from dataproc_sdk import DatioSchema
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.common.input import cod_producto, desc_producto
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


class TestApp(TestCase):
    @pytest.fixture(autouse=True)
    def spark_session(self, spark_test):
        self.spark = spark_test

    def test_initialize_inputs(self):

        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")

        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "EnvironmentVarsPM"
        parameters = get_params_from_runtime(runtime_context, root_key)

        init_values = InitValues()

        clients_df, contracts_df, products_df, output_file, output_schema = init_values.initialize_inputs(parameters)

        self.assertEqual(type(clients_df), DataFrame)
        self.assertEqual(type(contracts_df), DataFrame)
        self.assertEqual(type(products_df), DataFrame)
        self.assertEqual(type(output_file), str)
        self.assertEqual(type(output_schema), DatioSchema)
        self.assertEqual(products_df.columns, [cod_producto.name, desc_producto.name])

