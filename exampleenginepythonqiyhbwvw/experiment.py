
from typing import Dict, final
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.session import SparkSession

import exampleenginepythonqiyhbwvw.common.constants as c
from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__spark = SparkSession\
            .builder\
            .getOrCreate()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        self.__logger.info("Executing experiment...")

        init_values = InitValues()
        init_values.initialize_inputs(parameters)
        logic = BusinessLogic()

        clients_df = self.read_csv(c.CLIENTS_PATH, parameters)
        contracts_df = self.read_csv(c.CONTRACTS_PATH, parameters)
        products_df = self.read_csv(c.PRODUCTS_PATH, parameters)

        filtered_clients_df: DataFrame = logic.filter_by_age(clients_df)

        joined_df: DataFrame = logic.join_tables(filtered_clients_df, contracts_df, products_df)

        filtered_by_contracts_df: DataFrame = logic.filter_by_number_contracts(joined_df)

        hashed_df: DataFrame = logic.hash_columns(filtered_by_contracts_df)

        final_df: DataFrame = hashed_df \
            .withColumn(
                "hash",
                f.when(f.col("activo") == "false", f.lit("0"))
                    .otherwise(f.col("hash"))
            ) \
            .filter(f.col("hash") == "0")

        final_df \
            .write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy(["cod_producto", "activo"]) \
            .option("partitionOverwriteMode", "dynamic") \
            .save(str(parameters[c.OUTPUT_PATH]))


    def read_csv(self, table_id: str, parameters: dict) -> DataFrame:
        return self.__spark \
            .read \
            .option(c.HEADER, True) \
            .format(c.CSV) \
            .load(str(parameters[table_id]))


    def read_parquet(self, table_id: str, parameters: dict) -> DataFrame:
        return self.__spark \
            .read \
            .format(c.PARQUET) \
            .load(str(parameters[table_id]))
