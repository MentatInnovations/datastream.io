"""
1. reads an input data stream that consists of sensor data
2. generate a Kibana dashboard to visualize stream
3. applies a score to each sensor value
4. restreams input data and scores to ElasticSearch
"""

from __future__ import print_function

import sys
import datetime
import argparse

import pandas as pd
import numpy as np

import elasticsearch as ES

from utils_esk import DSIO2ES_batchRestreamer


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
    parser.add_argument("-i", "--input", help="input csv file",
                        default='../static/data/cardata_sample.csv')
    parser.add_argument("-o", "--output", help="output elasticsearch uri",
                        default="http://localhost:9200/")
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    args = parser.parse_args()

    print('Loading the data...')
    D = pd.read_csv(args.input, sep=',')
    print('Done.\n')

    Dcol = set(D.columns)

    # TODO get sensor names from arg or from data
    sensor_names = set({
        'accelerator_pedal_position',
        'torque_at_transmission',
        'steering_wheel_angle',
        'brake_pedal_status',
        'vehicle_speed',
        'transmission_gear_position'
    })

    # Check that all sensor names given in config file are in data file:
    if sensor_names.issubset(Dcol):
        print('Right sensors data available')
    else:
        print('Missing sensors, aborting.')
        sys.exit()

    # TODO get timefield_name * time_unit from args or from data
    timefield_name = 'time'
    time_unit = 's' # only seconds (s) and milliseconds (ms) supported

    # TODO get index name & type from args or generate from data
    index_name = 'tele-check'
    _type = 'car'

    print('Done.\n')

    if timefield_name not in Dcol:
        print('Missing time column in data, aborting.')
        sys.exit()

    if time_unit is None:
        print('No time unit given, aborting.')
        sys.exit()
    elif time_unit == 's':
        print('Converting to milliseconds ...')
        D[timefield_name] = np.floor(D[timefield_name]*1000).astype('int')
        print('Done')

    min_time = np.min(D[timefield_name])
    max_time = np.max(D[timefield_name])

    print('NB: data found from {} to {}'\
          .format(datetime.datetime.fromtimestamp(min_time/1000.),
                  datetime.datetime.fromtimestamp(max_time/1000.)))

    ### CREATE ALERT STREAMS
    Dpp = D[[timefield_name] + list(sensor_names)].copy()

    for sensor in sensor_names:
        Dpp['SCORE_{}'.format(sensor)] = batch_score(Dpp[sensor].values)

    ### Adding index name and type for all events:
    Dpp['_index'] = index_name
    Dpp['_type'] = _type

    # TODO generate dashboard with selected fields
    # TODO save dashboard to ES and provide Kibana URL

    ### STREAM TO ELASTIC
    # init ElasticSearch
    # TODO get ES url & auth credentials from args
    DSIO2ES_batchRestreamer(
        X=Dpp, timefield_name=timefield_name,
        es=ES.Elasticsearch(args.output),
        index_name=index_name, reDate=True, sleep=True
    )


if __name__ == '__main__':
    main()
