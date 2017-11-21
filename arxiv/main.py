"""
1. reads an input data stream that consists of sensor data
2. generates a Kibana dashboard to visualize the data stream
3. applies scores to the incoming sensor data, using different models
4. restreams input data and scores to ElasticSearch
"""

from __future__ import print_function

import argparse
import sys
import time

import dateparser
import elasticsearch as ES
import numpy as np

import pandas as pd
from utils_esk import elasticsearch_batch_restreamer


def batch_score(X, q=0.99):
    """ Score function """
    tup = np.percentile(X, q)
    tdown = np.percentile(X, 1-q)
    scores = np.logical_or(X < tdown, X > tup)*0.9 + 0.1
    return scores


def main():
    """ Main function """

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--es-uri", help="output elasticsearch uri",
                        default="http://localhost:9200/")
    parser.add_argument("--es-index", help="Elasticsearch index name",
                        default="dsio")
    parser.add_argument("--entry-type", help="entry type name",
                        default="measurement")
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    parser.add_argument("-s", '--sensors', help="select specific sensor names",
                        nargs='+')
    parser.add_argument("-t", "--timefield", help="name of the column in the data that defines the time",
                        default="")
    parser.add_argument("-u", "--timeunit", help="time unit",
                        default="")
    parser.add_argument('input', help='input file or stream')
    args = parser.parse_args()

    print('Loading the data...')
    dataframe = pd.read_csv(args.input, sep=',')
    print('Done.\n')

    columns = set(dataframe.columns)

    # Get timefield & timeunit from args
    timefield = args.timefield
    timeunit = args.timeunit
    if timefield and timefield not in columns:
        print('Missing time column in data, aborting.')
        sys.exit()

    if not timefield: # try to auto detect timefield
        for tfname in ['time', 'datetime', 'date', 'timestamp']:
            if tfname in columns:
                prev = current = None
                for i in dataframe[tfname][:10]:
                    try:
                        current = dateparser.parse(unicode(i))
                        # timefield needs to be parsable and always increasing
                        if not current or (prev and prev > current):
                            tfname = ''
                            break
                    except TypeError:
                        tfname = ''
                        break
                prev = current
                if tfname:
                    timefield = tfname
                    if isinstance(i, float) and not timeunit:
                        timeunit = 's'
                    break

    # If no timefield can be detected treat input as timeseries
    if not timefield:
        # Add time dimension with fixed intervals starting from now
        timefield = 'time'
        timeunit = 's'
        start = int(time.time())
        dataframe[timefield] = pd.Series(
            range(start, start+dataframe.shape[0]),
            index=dataframe.index
        )

    available_sensor_names = set(dataframe.columns).copy()
    available_sensor_names.remove(timefield)

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

    # Get ES index name and type from args
    index_name = args.es_index
    _type = args.entry_type

    min_time = dataframe[timefield][0]
    max_time = dataframe[timefield][dataframe[timefield].size-1]

    print('NB: data found from {} to {}'\
          .format(dateparser.parse(unicode(min_time)),
                  dateparser.parse(unicode(max_time))))

    if timeunit == 's':
        print('Converting to milliseconds ...')
        dataframe[timefield] = np.floor(dataframe[timefield]*1000).astype('int')
        print('Done')

    ### Create alert streams
    df_scored = dataframe[[timefield] + list(sensor_names)].copy()

    for sensor in sensor_names:
        try:
            df_scored['SCORE_{}'.format(sensor)] = batch_score(df_scored[sensor].values)
        except TypeError: # Map string values to int category codes
            values = df_scored[sensor].astype('category').cat.codes
            df_scored['SCORE_{}'.format(sensor)] = batch_score(values)

    ### Adding index name and type for all events:
    df_scored['_index'] = index_name
    df_scored['_type'] = _type

    # TODO generate dashboard with selected fields and scores
    # TODO save dashboard to ES and provide Kibana URL

    ### STREAM TO ELASTIC
    # init ElasticSearch
    # TODO get ES auth credentials from args
    elasticsearch_batch_restreamer(
        X=df_scored, timefield=timefield,
        es=ES.Elasticsearch(args.es_uri),
        index_name=index_name, redate=True, sleep=True
    )


if __name__ == '__main__':
    main()
