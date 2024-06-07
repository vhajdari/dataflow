# dataflow

Dependencies: Added SnakeYAML, MySQL, and PostgreSQL dependencies in pom.xml.

Configuration Class: Defined nested classes DBConfig and Config for parsing the YAML configuration.

Main Method: Reads command line arguments, loads the configuration, and initializes the database connections.

ExecutorService: Used for parallelizing table transfers.

Configuration Loading: Utilizes SnakeYAML to load configuration from the specified YAML file.

Data Transfer Method: transferTableData reads data from the source table, constructs the SQL for insertion, and performs the data transfer in batches.

Running the Program:
To run this program, use the following command:

```
export ROSETTA_DRIVERS=/path/to/jdbc/drivers
java -cp target/DatabaseTransfer-1.0-SNAPSHOT-jar-with-dependencies.jar DatabaseTransfer main.conf 10000 source target table1 table2 table3

```
Make sure the main.conf file is properly configured, and the JDBC drivers for MySQL and PostgreSQL are included in your classpath.
