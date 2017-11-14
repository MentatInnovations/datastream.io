"""
Elasticsearch batch re-streamer
"""
from __future__ import print_function

import time
import datetime

import numpy as np

from elasticsearch import helpers


def batchRedater(X, timefield_name, hz = 10):
    # send 10 datapoints a second
    now = np.int(np.round(time.time()))
    X[timefield_name] = (now*1000 + X.index._data*hz)
    return X


def df2es(Y, index_name, es, index_properties=None, recreate=True,
          chunk_size=100, raw=False, doc_ids=None):
    """
    Upload dataframe to Elasticsearch
    """
    # Creating the mapping
    if index_properties is not None:
        body = {"mappings": {index_name: {"properties": index_properties}}}
    print(body)

    # Making sure previous indices with similar name are erased and creating a new index
    if recreate:
        # init index
        try:
            es.indices.delete(index_name)
            print('Deleting existing index {}'.format(index_name))
        except:
            pass
        print('Creating index {}'.format(index_name))
        es.indices.create(index_name, body = body)

    # Formatting the batch to upload as a tuple of dictionnaries
    list_tmp = tuple(Y.fillna(0).T.to_dict().values())
    # Exporting to ES
    out = helpers.bulk(es, list_tmp)


# X = features
def elasticsearch_batch_restreamer(X, timefield_name, es, index_name,
                                   reDate=True, everyX=10, sleep=True):
    """
    Replay input stream into Elasticsearch
    """
    if reDate:
        X = batchRedater(X, timefield_name)

    if not sleep:
        everyX = 200

    virtual_time = np.min(X[timefield_name])
    recreate = True
    while virtual_time < np.max(X[timefield_name]):
        start_time = virtual_time
        virtual_time += everyX*1000
        end_time = virtual_time
        if sleep:
            while np.round(time.time()) < end_time/1000.:
                print('z')
                time.sleep(1)

        ind = np.logical_and(X[timefield_name] <= end_time, X[timefield_name] > start_time)
        print('Writing {} rows dated {} to {}'
              .format(np.sum(ind),
                      datetime.datetime.fromtimestamp(start_time/1000.),
                      datetime.datetime.fromtimestamp(end_time/1000.)))

        index_properties = {"time" : {"type": "date"}}
        df2es(X.loc[ind], index_name, es=es,
              index_properties=index_properties, recreate=recreate)
        recreate = False
