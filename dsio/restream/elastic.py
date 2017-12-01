"""
Elasticsearch batch re-streamer
"""
import sys
import time
import datetime
import webbrowser

import numpy as np
from elasticsearch import helpers

from dsio.dashboard.kibana import generate_dashboard


def batchRedater(X, timefield, hz=10):
    # send 10 datapoints a second
    now = np.int(np.round(time.time()))
    X[timefield] = (now*1000 + X.index._data*hz)
    return X


def df2es(Y, index_name, es, index_properties=None, recreate=False,
          chunk_size=100, raw=False, doc_ids=None):
    """
    Upload dataframe to Elasticsearch
    """
    # Making sure previous indices with similar name are erased and creating a new index
    if recreate:
        # init index
        try:
            es.indices.delete(index_name)
            print('Deleting existing index {}'.format(index_name))
        except elasticsearch.exceptions.TransportError:
            pass

        # Creating the mapping
        if index_properties is not None:
            body = {"mappings": {index_name: {"properties": index_properties}}}

        print('Creating index {}'.format(index_name))
        es.indices.create(index_name, body=body)

    # Formatting the batch to upload as a tuple of dictionnaries
    list_tmp = tuple(Y.fillna(0).T.to_dict().values())

    # Exporting to ES
    out = helpers.bulk(es, list_tmp)

    return out


def elasticsearch_batch_restreamer(dataframe, timefield, es_conn, index_name,
                                   sensor_names, kibana_uri,
                                   redate=True, everyX=10, sleep=True):
    """
    Replay input stream into Elasticsearch
    """
    if redate:
        dataframe = batchRedater(dataframe, timefield)

    if not sleep:
        everyX = 200

    virtual_time = np.min(dataframe[timefield])
    first_pass = True
    while virtual_time < np.max(dataframe[timefield]):
        start_time = virtual_time
        virtual_time += everyX*1000
        end_time = virtual_time
        if sleep and not first_pass:
            while np.round(time.time()) < end_time/1000.:
                print('z')
                time.sleep(1)

        ind = np.logical_and(dataframe[timefield] <= end_time,
                             dataframe[timefield] > start_time)
        print('Writing {} rows dated {} to {}'
              .format(np.sum(ind),
                      datetime.datetime.fromtimestamp(start_time/1000.),
                      datetime.datetime.fromtimestamp(end_time/1000.)))

        index_properties = {"time" : {"type": "date"}}
        df2es(dataframe.loc[ind], index_name, es_conn, index_properties,
              recreate=first_pass)
        if first_pass:
            es_conn.index(index='.kibana', doc_type="index-pattern",
                          id=index_name,
                          body={
                              "title": index_name,
                              "timeFieldName": "time"
                          })

            # Generate dashboard with selected fields and scores
            dashboard = generate_dashboard(es_conn, sensor_names, index_name)
            if not dashboard:
                print('Cannot connect to Kibana at %s' % kibana_uri)
                sys.exit()

            # Open Kibana dashboard in browser
            webbrowser.open(kibana_uri+'#/dashboard/%s-dashboard' % index_name)

            first_pass = False
