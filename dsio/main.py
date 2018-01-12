"""
1. reads an input data stream that consists of sensor data
2. generates a Kibana dashboard to visualize the data stream
3. applies scores to the incoming sensor data, using different models
4. restreams input data and scores to ElasticSearch
"""

import argparse
import sys
import time

import dateparser
import elasticsearch
import numpy as np
import pandas as pd

from .restream.elastic import elasticsearch_batch_restreamer
from .helpers import detect_time
from .anomaly_detectors import AnomalyDetector


def main():
    """ Main function """

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--detector", help="anomaly detector",
                        default="Quantile1D")
    parser.add_argument("--modules",
                        help="python modules that define additional anomaly detectors",
                        nargs='+', default=[])
    parser.add_argument("--es-uri", help="output elasticsearch uri",
                        default="http://localhost:9200/")
    parser.add_argument("--kibana-uri", help="Kibana uri",
                        default="http://localhost:5601/app/kibana")
    parser.add_argument("--es-index", help="Elasticsearch index name")
    parser.add_argument("--entry-type", help="entry type name",
                        default="measurement")
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    parser.add_argument("-s", '--sensors', help="select specific sensor names",
                        nargs='+')
    parser.add_argument("-t", "--timefield",
                        help="name of the column in the data that defines the time",
                        default="")
    parser.add_argument('input', help='input file or stream')
    args = parser.parse_args()

    for module in args.modules:
        if module.endswith('.py'):
            code = open(module).read()
        else:
            code = 'import %s' % module
        try:
            exec(code)
        except Exception as exc:
            print('Failed to load module %s. Exception: %r', (module, exc))

    # Load anomaly detector
    for Detector in AnomalyDetector.__subclasses__():
        if Detector.__name__.lower() == args.detector.lower():
            break
    else:
        print('Invalid anomaly detector: %s' % args.detector)
        sys.exit()

    print('Loading the data...')
    dataframe = pd.read_csv(args.input, sep=',')
    print('Done.\n')

    columns = set(dataframe.columns)
    available_sensor_names = columns.copy()

    # Get timefield from args
    timefield = args.timefield
    if timefield and timefield not in columns:
        print('Missing time column in data, aborting.')
        sys.exit()

    if not timefield: # Try to auto detect timefield
        timefield, unix = detect_time(dataframe)

    # If no timefield can be detected treat input as timeseries
    if not timefield:
        # Add time dimension with fixed intervals starting from now
        timefield = 'time'
        unix = True
        start = int(time.time())
        dataframe[timefield] = pd.Series(
            range(start, start+dataframe.shape[0]),
            index=dataframe.index
        )
    else:
        available_sensor_names.remove(timefield)

    if not unix:
        dataframe['time'] = pd.to_datetime(
            dataframe[timefield],
            infer_datetime_format=True
        ).values.astype(np.int64) // 10 ** 9
        timefield = 'time'
        unix = True

    # Get selected sensors from args
    if args.sensors:
        sensor_names = set(args.sensors)
    else: # Get all sensors if none selected
        sensor_names = available_sensor_names

    # Check that all sensor names given in config file are in data file
    if sensor_names.issubset(available_sensor_names):
        print('Right sensors data available')
    else:
        print('Missing sensors, aborting.')
        sys.exit()

    for sensor in sensor_names.copy():
        if dataframe[sensor].dtype == np.dtype('O'):
            sensor_names.remove(sensor)

    min_time = dataframe[timefield][0]
    max_time = dataframe[timefield][dataframe[timefield].size-1]

    print('NB: data found from {} to {}'\
          .format(dateparser.parse(str(min_time)),
                  dateparser.parse(str(max_time))))

    if unix:
        print('Converting to milliseconds ...')
        dataframe[timefield] = np.floor(dataframe[timefield]*1000).astype('int')
        print('Done')

    ### Create alert streams
    df_scored = dataframe[[timefield] + list(sensor_names)].copy()

    for sensor in sensor_names:
        detector = Detector()
        values = df_scored[sensor]
        detector.train(values)
        detector.update(values)
        df_scored['SCORE_{}'.format(sensor)] = detector.anomalyScore(values)

    # Get ES index name and type from args or generate from input name
    index_name = args.es_index
    if not index_name and args.input:
        index_name = args.input.split('/')[-1].split('.')[0].split('_')[0]
    if not index_name:
        index_name = 'dsio'

    ### Adding index name and type for all events:
    df_scored['_index'] = index_name
    df_scored['_type'] = args.entry_type

    # init ElasticSearch
    es_conn = elasticsearch.Elasticsearch(args.es_uri)
    try:
        es_conn.info()
    except elasticsearch.ConnectionError:
        print('Cannot connect to Elasticsearch at %s' % args.es_uri)
        sys.exit()

    # Steam to Elasticsearch
    elasticsearch_batch_restreamer(
        df_scored, timefield, es_conn, index_name,
        sensor_names, args.kibana_uri, redate=True, sleep=True
    )


if __name__ == '__main__':
    main()
