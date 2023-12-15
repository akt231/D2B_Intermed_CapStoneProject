# Data2Bots Intermediate Class Capstone Project
###  FinnHub>Kafka>Spark>Snowflake
![Project Architecture](./docs/Class59_opensource-milestone-project.jpg)

## Project Overview
An End to End Data Engineering Project getting Data from a Data Source, ingesting Said Data in dockerised applications, using Kafka to broadcast output messages , streaming the data using Pyspark and housing the data in a Snowflake tanle

This project is the intermediate Module Project submitted to the Data2Bots academy.
This project will demonstrate the use of streaming pipeline development and architecture emphasizing the respective aspects of pipeline design in low latency, scalability & availability.

The system design is a data pipeline streaming real-time trades for US stocks, forex and crypto from the Finnhub website api. 
Data is initially obtained using websockets from the Finnhub API. The data obtained using the Finnhub API websockets is converted into avro format and streamed via Kafka.
The avro serialized data is deserialized by Pyspark and transformation is applied to the data. The transformed data is then streamed in micro batches to Snowflake where it is used to populate Snowflake tables.

## Data Pipeline Architecture
The diagram above is the schematic diagram depicting the pipeline's architecture.
{picture of project as a layered application}
All applications are containerized into Docker containers.
Data ingestion layer – A containerized Python application called FinnhubProducer connects to Finnhub.io websocket. The application retrieves trade data via websocket and encodes the data into Avro format as specified by the schema contained in the trades.avsc file. This data is ingested by the Kafka broker.
Message broker layer - messages from FinnhubProducer are consumed by the Kafka broker. “./kafka/scripts/kafka_create_topic.sh” script is initiated at the kafka container startup, this script creates the required topic used to stream the data. The Zookeeper container is launched before Kafka as it is required for its metadata management.
Stream processing layer - a Spark docker container is initiated for processing of the data from Kafka. A PySpark application called SparkX is submitted into Spark cluster manager via local mode, the setup can be extended/scaled via docker containers to use cluster mode that can delegate worker(s) for more intensive applications. This application connects to the Kafka broker to retrieve messages, transform them using Spark Structured Streaming, and loads into Snowflake tables. 
The first query is used to transform the trades data into a dataframe which is streamed continuously into snowflake while the second query aggregates the data and streams the data using a timed trigger
Serving database layer - a Snowflake database stores & persists data for the two queries from the Spark application. 

## Tech. Stack
•	Github: To host our source code as well as for CI/CD with Github Actions
•	Docker: To containerize our code
•	Kafka:  used to build real-time streaming data pipelines and applications that adapt to the data streams.
•	Spark: used for fast, interactive computation that runs in memory, used here specifically for data processing (data transformation step), Spark is chosen because of its parellel processing capabilities. Should the amount of data proliferate to 100x, more worker nodes can be added to the spark cluster to scale out.
•	Snowflake: used for data storage, processing, and analytic solutions 

## Project Requirements
Required data for running the finnhub producer, kafka and snowflake connections are listed below. This data is required to run the pipeline. The data is required to be written in an .env file placed at the root folder. A config server like AWS Parameter Store, Google Secrets Manager, HashiCorp Vault or GitHub secrets can also be used as an alternative  for storing the secrets/config data.
Pem public and private keys need to be generated for programmatic access to the Snowflake user account.

## FINNHUB SECRETS
|Secret Variable Name       |Variable Description                                           |
|:--------------------------|:--------------------------------------------------------------|
|FINNHUB SECRETS            |                                                               |
|d2b_finnhubio_user_name    |#user name used when registering a finnhub account             |
|d2b_finnhubio_user_email   |#user email used when registering a finnhub account            |
|d2b_token_finnhubio	    |#user token provided by finnhub on registering finnhub account |
|d2b_tickers_finnhubio	    |#List of Trading pairs to obtain data for i.e['BINANCE:BTCUSDT', 'BINANCE:ETHUSDT', 'BINANCE:XRPUSDT', 'BINANCE:DOGEUSDT' ]|
|KAFKA SECRETS              |                                                               |
|d2b_kafka_server|          |#name of  client/listener in kafka docker container i.e. localhost or broker|
|d2b_kafka_port	            |#Kafka server port used for listening for streamed data        |
|d2b_kafka_producer_topic   |#kafka topic used for streamed data                            |
|SNOWFLAKE SECRETS          |                                                               |	
|sf_account                 |Snowflake account identifier i.e. 'vbctxic-tq01322'            |
|sf_url                     |url of snowflake account i.e. 'https://vbctxic-tq01322.snowflakecomputing.com/' |
|sf_database                |	Snowflake Database Name                                     |
|sf_warehouse               | 	Snowflake Warehouse Name                                    |                                   
|sf_schema                  |	Snowflake Schema Name                                       |
|sf_table_detailedDFX       |	Snowflake table for detailed reporting                      |
|sf_table_finalSummaryDFX   | 	Snowflake table for summary reporting                       |    
|sf_int_stage               | 	Snowflake temporary stage                                   |
|sf_role                    |	Snowflake Role                                              |    
|sf_username                |	Snowflake user name                                         |
|sf_password                |	Snowflake user password                                     |    
|sf_pem_pass                |	Password key for encrypted snowflake pem file               |    
