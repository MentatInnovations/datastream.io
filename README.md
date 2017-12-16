# datastream.io
An open-source framework for real-time anomaly detection using Python, ElasticSearch and Kibana

## INTRODUCTION

We will offer the following functionality for v0.1:

### File listener
A ulitity that can listen to a folder, either local or in HDFS, and track new files or additions to existing files, which it then submits for ingestion to another service.

### Re-streaming
A utility that can take an offline batch CSV file, and re-stream it at realistic speeds to an ingestion service. 

### Anomaly Detector Class Interface
An abstract interface for anomaly detection on CSV files with a set of assumptions about the data to be made explicit, and generalised further in subsequent versions.

### Basic Pipeline
- Setup Data Sample > Kibana dashboard generation
- Data Ingest > forked to:
	- DSIO > generates alerts > ES > Kibana
	- raw data write to ES > Kibana

## DEEP-DIVE ON ANOMALY DETECTION FRAMEWORK

The basic methods that an anomaly detector implements are the following:

`score: pandas DataFrame of n rows and p columns >> pandas Series of n rows`

In addition, the


