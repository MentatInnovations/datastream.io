"""
1. reads an input data stream that consists of sensor data
2. generates a Kibana dashboard to visualize the data stream
3. applies scores to the incoming sensor data, using different models
4. restreams input data and scores to ElasticSearch
"""

import sys
import time
import math
import webbrowser

import dateparser
import elasticsearch
import numpy as np
import pandas as pd

from .restream.elastic import init_elasticsearch
from .restream.elastic import elasticsearch_batch_restreamer
from .dashboard.kibana import generate_dashboard as generate_kibana_dashboard

from .helpers import detect_time, parse_arguments, normalize_timefield, select_sensors
from .anomaly_detectors import AnomalyDetector

from .exceptions import DsioError, ModuleLoadError, DetectorNotFoundError
from .exceptions import TimefieldNotFoundError, SensorsNotFoundError

MAX_BATCH_SIZE = 1000


def load_detector(name, modules):
    """ Evaluate modules as Python code and load selecter anomaly detector """
    # Try to load modules
    for module in modules:
        if module.endswith('.py'):
            code = open(module).read()
        else:
            code = 'import %s' % module
        try:
            exec(code)
        except Exception as exc:
            raise ModuleLoadError('Failed to load module %s. Exception: %r', (module, exc))

    # Load selected anomaly detector
    for detector in AnomalyDetector.__subclasses__():
        if detector.__name__.lower() == name.lower():
            return detector

    raise DetectorNotFoundError("Can't find detector: %s" % name)


def init_detector_models(sensors, training_set, detector):
    models = {}
    for sensor in sensors:
        models[sensor] = detector()
        models[sensor].train(training_set[sensor])

    return models


def main():
    """ Main function """
    args = parse_arguments()
    try:
        detector = load_detector(args.detector, args.modules)

        print('Loading the data...')
        dataframe = pd.read_csv(args.input, sep=',')
        print('Done.\n')

        timefield, available_sensors = normalize_timefield(dataframe,
                                                           args.timefield)
        dataframe, sensors = select_sensors(dataframe, args.sensors,
                                            available_sensors, timefield)

        if args.es_uri:
            es_conn, index_name, dataframe = init_elasticsearch(
                args.es_uri, args.es_index, args.entry_type,
                args.input, dataframe
            )
        else:
            es_conn = index_name = None

        # Split data into batches
        batches = np.array_split(dataframe, math.ceil(dataframe.shape[0]/MAX_BATCH_SIZE))

        # Initialize anomaly detector models
        models = init_detector_models(sensors, batches[0], detector)

        if es_conn: # Generate dashboard with selected fields and scores
            generate_kibana_dashboard(es_conn, sensors, index_name)
            webbrowser.open(args.kibana_uri+'#/dashboard/%s-dashboard' % index_name)

        first_pass = True
        for batch in batches:
            for sensor in sensors: # Apply the scores
                batch['SCORE_{}'.format(sensor)] = models[sensor].score(batch[sensor])

            if es_conn: # Stream batch to Elasticsearch
                elasticsearch_batch_restreamer(
                    batch, timefield, es_conn, index_name,
                    redate=True, sleep=True, first_pass=first_pass
                )

            if not first_pass:
                for sensor in sensors: # Update the models
                    models[sensor].update(batch[sensor])

            first_pass = False

    except DsioError as exc:
        print(repr(exc))
        sys.exit(exc.code)



if __name__ == '__main__':
    main()
