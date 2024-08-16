from dataproc_sdk import DatioPysparkSession, DatioSchema
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import exampleenginepythonqiyhbwvw.common.constants as c


class InitValues:

    def __init__(self):
        self.__logger = get_user_logger(InitValues.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()


    def initialize_inputs(self, parameters):
        self.__logger.info("Using given configuration")

        clients_path = parameters[c.CLIENTS_PATH]
        clients_schema = DatioSchema \
            .getBuilder() \
            .fromURI(parameters[c.CLIENTS_SCHEMA]) \
            .build()

        clients_df = self.__datio_pyspark_session.read().datioSchema(clients_schema) \
            .option(c.HEADER, c.TRUE) \
            .option(c.DELIMETER, c.COMMA) \
            .csv(clients_path)

        clients_df.printSchema()
        clients_df.show()
