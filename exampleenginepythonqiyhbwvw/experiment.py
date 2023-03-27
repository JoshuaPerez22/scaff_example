import sys
from typing import Dict

from dataproc_sdk import DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import BooleanType

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.common.input import activo, vip, fec_alta
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
        self.__spark = SparkSession.builder.getOrCreate()
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        self.__logger.info("Executing experiment")
        io = InitValues()
        clients_df, contracts_df, products_df, output_file, output_schema =\
            io.initialize_inputs(parameters)

        clients_df.show()
        clients_df.printSchema()
        print("COUNT", clients_df.count())

        clients_df.limit(2)

        print("COLLECT", clients_df.collect())
        print("HEAD", clients_df.head())
        print("TAKE", clients_df.take(5))
        print("FIRST", clients_df.first())



        sys.exit()
        #clients_df = self.read_csv("clients", parameters)
        #contracts_df = self.read_csv("contracts", parameters)
        #products_df = self.read_csv("products", parameters)

        logic = BusinessLogic()
        filtered_clients_df: DataFrame = logic.filter_by_age_and_vip(clients_df)
        joined_df: DataFrame = logic.join_tables(filtered_clients_df, contracts_df, products_df)
        filtered_by_contracts_df: DataFrame = logic.filter_by_number_of_contracts(joined_df)
        hashed_df: DataFrame = logic.hash_columns(filtered_by_contracts_df)

        drop_col = ["vip"]
        #print(set(hashed_df.columns).difference(set(list("vip"))))
        hashed_df.printSchema()
        final_df = hashed_df\
            .select(*list(set(hashed_df.columns).difference(drop_col)),
                    when(activo() == lit("false"), lit("false").cast(BooleanType())).otherwise(vip()).alias("vip"))\
            .select(hashed_df.columns)\
            .filter((vip() == "false") | (fec_alta() < "2013-01-01"))
        final_df.show(20, False)

        #hashed_df.write.mode("append").parquet("resources/data/output/table_out")
        #hashed_df.write.partitionBy("cod_producto", "activo").mode("overwrite").parquet("resources/data/output/table_out")
        #final_df.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")\
            #.partitionBy("cod_producto", "activo").parquet(str(parameters["output"]))

        self.__datio_pyspark_session.write().mode("overwrite")\
            .option("partitionOverwriteMode", "dynamic")\
            .partition_by(["cod_producto", "activo"])\
            .datio_schema(output_schema).parquet(final_df, output_file)

    """
    def read_csv(self, table_id, parameters):
        return self.__spark.read\
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(str(parameters[table_id]))
    """
