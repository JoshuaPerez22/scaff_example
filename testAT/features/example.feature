Feature: TestAT basic example

    Scenario Outline: Test example process exits successfully
      Given a config file <config_file>
      When execute example app file in PySpark
      Then result should be <exit_code>

      Scenarios:
        | config_file      | exit_code |
        | application.conf | 0         |


    Scenario Outline: Test output dataFrame columns
      Given an output dataFrame located at path <output_path>
      When read the output dataFrame
      Then total columns should be equal to <tot_columns>

      Scenarios:
        | output_path           | tot_columns |
        | resources/data/output | 13          |


    Scenario Outline: Test output dataFrame columns
      Given an output dataFrame located at path <output_path>
      When read the output dataFrame
      Then <column_name> column should <comparator> <value>

      Scenarios:
        | output_path           | column_name | comparator  | value |
        | resources/data/output | edad        | >= | 30    |
        | resources/data/output | edad        | <= | 50    |
        | resources/data/output | vip         | ==       | True  |
        | resources/data/output | fec_alta    | like     | [0-9]{4}-[0-9]{2}-[0-9]{2} |
