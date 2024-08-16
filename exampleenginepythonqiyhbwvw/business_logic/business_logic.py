from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.window import Window, WindowSpec

import exampleenginepythonqiyhbwvw.common.constants as c


class BusinessLogic:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(BusinessLogic.__qualname__)


    def filter_by_age(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter by age and vip status")
        return df.filter(
            (f.col("edad") >= c.THIRTY_NUMBER) &
            (f.col("edad") <= c.FIFTY_NUMBER) &
            (f.col("vip") == True)
        )


    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Applying join process")
        return clients_df \
            .join(
            contracts_df,
            f.col("cod_client") == f.col("cod_titular"),
            c.INNER_JOIN
        ) \
            .join(
            products_df,
            ["cod_producto"],
            c.INNER_JOIN
        )


    def filter_by_number_contracts(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by number of contracts")

        window: WindowSpec = Window \
            .partitionBy(f.col("cod_client"))

        return df \
            .select(
                *df.columns,
                f.count("cod_client").over(window).alias(c.COUNT)
            ) \
            .filter(f.col("count") > c.THREE_NUMBER) \
            .drop(f.col("count"))


    def hash_columns(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Generating hash column")
        return df\
            .select(
                *df.columns,
                f.sha2(
                    f.concat_ws("||", *df.columns),
                    256
                ).alias("hash")
            )
