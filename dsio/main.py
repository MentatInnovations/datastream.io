"""
1. Reads an input data stream that consists of sensor data
2. Generates Kibana and/or Bokeh dashboards to visualize the data stream
3. Applies scores to the incoming sensor data, using selected anomaly detector
4. Restreams input data and scores to ElasticSearch and/or Bokeh server
"""

import sys
import datetime
import time
import threading
import math
import webbrowser

from queue import Queue

import numpy as np
import pandas as pd

from bokeh.plotting import curdoc

from .restream.elastic import init_elasticsearch
from .restream.elastic import upload_dataframe
from .dashboard.kibana import generate_dashboard as generate_kibana_dashboard
from .dashboard.bokeh import generate_dashboard as generate_bokeh_dashboard

from .helpers import parse_arguments, normalize_timefield
from .helpers import select_sensors, init_detector_models, load_detector

from .exceptions import DsioError

MAX_BATCH_SIZE = 1000
doc = curdoc()


def restream_dataframe(
        dataframe, detector, sensors=None, timefield=None,
        speed=10, es_uri=None, kibana_uri=None, index_name='',
        entry_type='', bokeh_port=5001, cols=3):
    """
        Restream selected sensors & anomaly detector scores from an input
        pandas dataframe to an existing Elasticsearch instance and/or to a
        built-in Bokeh server.

        Generates respective Kibana & Bokeh dashboard apps to visualize the
        stream in the browser
    """

    dataframe, timefield, available_sensors = normalize_timefield(
        dataframe, timefield, speed
    )

    dataframe, sensors = select_sensors(
        dataframe, sensors, available_sensors, timefield
    )

    if es_uri:
        es_conn = init_elasticsearch(es_uri)
        # Generate dashboard with selected fields and scores
        generate_kibana_dashboard(es_conn, sensors, index_name)
        webbrowser.open(kibana_uri+'#/dashboard/%s-dashboard' % index_name)
    else:
        es_conn = None

    # Queue to communicate between restreamer and dashboard threads
    update_queue = Queue()
    if bokeh_port:
        generate_bokeh_dashboard(
            sensors, title=detector.__name__, cols=cols,
            port=bokeh_port, update_queue=update_queue
        )

    restream_thread = threading.Thread(
        target=threaded_restream_dataframe,
        args=(dataframe, sensors, detector, timefield, es_conn,
              index_name, entry_type, bokeh_port, update_queue)
    )
    restream_thread.start()


def threaded_restream_dataframe(dataframe, sensors, detector, timefield,
                                es_conn, index_name, entry_type, bokeh_port,
                                update_queue, interval=3, sleep_interval=1):
    """ Restream dataframe to bokeh and/or Elasticsearch """
    # Split data into batches
    batches = np.array_split(dataframe, math.ceil(dataframe.shape[0]/MAX_BATCH_SIZE))

    # Initialize anomaly detector models, train using first batch
    models = init_detector_models(sensors, batches[0], detector)

    first_pass = True
    for batch in batches:
        for sensor in sensors: # Apply the scores
            batch['SCORE_{}'.format(sensor)] = models[sensor].score_anomaly(batch[sensor])
            batch['FLAG_{}'.format(sensor)] = models[sensor].flag_anomaly(batch[sensor])

        end_time = np.min(batch[timefield])
        recreate_index = first_pass

        while end_time < np.max(batch[timefield]):
            start_time = end_time
            end_time += interval*1000
            if not recreate_index:
                while np.round(time.time()) < end_time/1000.:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                    time.sleep(sleep_interval)

            ind = np.logical_and(batch[timefield] <= end_time,
                                 batch[timefield] > start_time)
            print('\nWriting {} rows dated {} to {}'
                    .format(np.sum(ind),
                            datetime.datetime.fromtimestamp(start_time/1000.),
                            datetime.datetime.fromtimestamp(end_time/1000.)))

            if bokeh_port:
                update_queue.put(batch.loc[ind])

            if es_conn: # Stream batch to Elasticsearch
                upload_dataframe(es_conn, batch.loc[ind], index_name, entry_type,
                                 recreate=recreate_index)
            recreate_index = False

        if first_pass:
            for sensor in sensors:  # Fit the models
                models[sensor].fit(batch[sensor])
        else:
            for sensor in sensors:  # Update the models
                models[sensor].update(batch[sensor])
        first_pass = False


def main():
    """ Main function """
    args = parse_arguments()

    try:
        detector = load_detector(args.detector, args.modules)

        # Generate index name from input filename
        index_name = args.input.split('/')[-1].split('.')[0].split('_')[0]
        if not index_name:
            index_name = 'dsio'

        print('Loading the data...')
        dataframe = pd.read_csv(args.input, sep=',')
        print('Done.\n')

        restream_dataframe(
            dataframe=dataframe, detector=detector,
            sensors=args.sensors, timefield=args.timefield,
            speed=int(float(args.speed)), es_uri=args.es and args.es_uri,
            kibana_uri=args.kibana_uri, index_name=index_name,
            entry_type=args.entry_type, bokeh_port=int(args.bokeh_port),
            cols=int(args.cols)
        )

    except DsioError as exc:
        print(repr(exc))
        sys.exit(exc.code)


if __name__ == '__main__':
    main()
