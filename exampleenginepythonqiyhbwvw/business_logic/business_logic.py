from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType, DateType, BooleanType, IntegerType
from exampleenginepythonqiyhbwvw.common import input as i, output as o
from exampleenginepythonqiyhbwvw.common import constants as c


class BusinessLogic:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def filter_by_age_and_vip(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter by age and vip status")
        return df.filter((i.edad() >= c.THIRTY_NUMBER) & (i.edad() <= c.FIFTY_NUMBER) & (i.vip() == c.TRUE_VALUE))

    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Applying join process")
        return clients_df.join(contracts_df, i.cod_client() == i.cod_titular(), c.INNER_TYPE)\
            .join(products_df, [i.cod_producto.name], c.INNER_TYPE)

    def filter_by_number_of_contracts(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by number of contracts")
        return df.select(*df.columns, f.count(i.cod_client())
                         .over(Window.partitionBy(i.cod_client())).alias(c.COUNT_COLUMN))\
            .filter(f.col(c.COUNT_COLUMN) > c.THREE_NUMBER)\
            .drop(c.COUNT_COLUMN)

    def hash_columns(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Generating hash column")
        return df.select(*df.columns,
                         f.sha2(f.concat_ws(c.CONCAT_SEPARATOR, *df.columns), c.SHA_KEY).alias(o.hash.name))

    def select_all_columns(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Selecting all columns")
        return df.select(
            o.cod_producto().cast(StringType()),
            o.cod_iuc().cast(StringType()),
            o.cod_titular().cast(StringType()),
            o.fec_alta().cast(DateType()),
            o.activo().cast(BooleanType()),
            o.cod_client().cast(StringType()),
            o.nombre().cast(StringType()),
            o.edad().cast(IntegerType()),
            o.provincia().cast(StringType()),
            o.cod_postal().cast(IntegerType()),
            o.vip().cast(BooleanType()),
            o.desc_producto().cast(StringType()),
            o.hash().cast(StringType())
        )
