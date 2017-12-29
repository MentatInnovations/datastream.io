""" Helper functions """

import argparse
import time

import dateparser

import numpy as np
import pandas as pd

from .exceptions import SensorsNotFoundError, TimefieldNotFoundError


def parse_arguments():
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument("--detector", help="Anomaly detector",
                        default="Gaussian1D")
    parser.add_argument("--modules",
                        help="Python modules that define additional anomaly detectors",
                        nargs='+', default=[])
    parser.add_argument("--es-uri", help="Output Elasticsearch URI",
                        default="http://localhost:9200/")
    parser.add_argument("--kibana-uri", help="Kibana URI",
                        default="http://localhost:5601/app/kibana")
    parser.add_argument("--bokeh-port", help="Bokeh server port",
                        default="5000")
    parser.add_argument("--es-index", help="Elasticsearch index name")
    parser.add_argument("--entry-type", help="Entry type name",
                        default="measurement")
    parser.add_argument("-v", "--verbose", help="Increase output verbosity",
                        action="store_true")
    parser.add_argument("-s", '--sensors', help="Select specific sensor names",
                        nargs='+')
    parser.add_argument("-t", "--timefield",
                        help="name of the column in the data that defines the time",
                        default="")
    parser.add_argument('input', help='input file or stream')
    return parser.parse_args()


def detect_time(dataframe):
    """ Attempt to detect the time dimension in a dataframe"""
    columns = set(dataframe.columns)
    timefield = unix = None
    for tfname in ['time', 'datetime', 'date', 'timestamp']:
        if tfname in columns:
            prev = current = None
            unix = True # Assume unix timestamp format unless proven otherwise
            for i in dataframe[tfname][:10]: # FIXME this seems arbitrary
                try:
                    current = dateparser.parse(str(i))
                    # timefield needs to be parsable and always increasing
                    if not current or (prev and prev > current):
                        tfname = ''
                        break
                    if unix and not (isinstance(i, float) or
                                     isinstance(i, int)):
                        unix = False
                except TypeError:
                    tfname = ''
                    break
            prev = current
            if tfname:
                timefield = tfname
                if isinstance(i, float) or isinstance(i, int):
                    unix = True
                break

    return timefield, unix


def normalize_timefield(dataframe, timefield):
    available_sensor_names = set(dataframe.columns)

    # Get timefield from args
    if timefield and timefield not in set(dataframe.columns):
        raise TimefieldNotFoundError(timefield)

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

    min_time = dataframe[timefield][0]
    max_time = dataframe[timefield][dataframe[timefield].size-1]

    print('data found from {} to {}'\
            .format(dateparser.parse(str(min_time)),
                    dateparser.parse(str(max_time))))

    if unix:
        print('Converting to milliseconds ...')
        dataframe[timefield] = np.floor(dataframe[timefield]*1000).astype('int')
        print('Done')

    return timefield, available_sensor_names


def select_sensors(dataframe, sensors, available_sensor_names, timefield):
    # Get selected sensors from args
    if sensors:
        sensor_names = set(sensors)
    else: # Get all sensors if none selected
        sensor_names = available_sensor_names

    # Check that all sensor names given in config file are in data file
    if not sensor_names.issubset(available_sensor_names):
        raise SensorsNotFoundError(sensor_names)

    for sensor in sensor_names.copy():
        if dataframe[sensor].dtype == np.dtype('O'):
            sensor_names.remove(sensor)

    ### Copy selected sensors to new dataframe
    df_copy = dataframe[[timefield] + list(sensor_names)].copy()

    return df_copy, sensor_names
