# perfsonar_collector

[![Builds perfsonar collector image](https://github.com/opensciencegrid/perfsonar_collector/actions/workflows/main.yaml/badge.svg?branch=main)](https://github.com/opensciencegrid/perfsonar_collector/actions/workflows/main.yaml)

This project hosts a new perfSONAR collector to help support the IRIS-HEP/OSG-LHC transition of the network data pipeline from the use of a RabbitMQ message bus
to using the new HTTP archiver to send data directly to Logstash.   Because we are removing the RabbitMQ bus the existing perfSONAR collector needs to be revised.

The new code will be capable of querying selected perfSONAR hosts for their data, transforming the data appropriate for the target destination and sending 
that data to Elasticsearch, either directly or via Logstash.
