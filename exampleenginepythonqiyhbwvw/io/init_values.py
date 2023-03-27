from dataproc_sdk import DatioSchema, DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from exampleenginepythonqiyhbwvw.common import constants as c


class InitValues:
    """
    InitValues initializes needed values for current PySpark example
    """
    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(InitValues.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def initialize_inputs(self, parameters):
        """
        Read values and schemas from file if there is an application.conf.
        An example of config file needed for this example process can be found in resources/application.conf
        Also needed input tables examples can be found in resources/clients.csv, contracts.csv and products.csv.
        Schemas can be found in resources/clients_schema.json, contracts_schema.json, products_schema.csv and
        output_schema.json.
        Create DF manually otherwise.
        :param parameters: the parameter configuration
        :return: clients_df, contracts_df, products_df and output_schema
        """
        self.__logger.info("Using given configuration...")

        clients_df = self.get_input_df(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)
        contracts_df = self.get_input_df(parameters, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)
        products_df = self.get_input_df(parameters, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)

        output_file, output_schema = self.get_config_by_name(parameters, "output_path", "output_schema")

        self.__logger.info("Data has been loaded successfully")

        return clients_df, contracts_df, products_df, output_file, output_schema

    def get_input_df(self, parameters, key_path, key_schema):
        self.__logger.info("Reading path and schema from " + key_path)
        input_path, input_schema = self.get_config_by_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read().datioSchema(input_schema)\
            .option("delimiter", ",")\
            .option("header", "true")\
            .csv(input_path)

    def get_config_by_name(self, parameters, key_path, key_schema):
        self.__logger.info("Get output path from " + key_path)
        input_path = parameters[key_path]
        input_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        return input_path, input_schema
