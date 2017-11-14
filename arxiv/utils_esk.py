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


def df2es(Y, index_name, es = None, bodyNow = None, recreate = True, chunk_size = 100, raw=False, doc_ids = None):
    """ """
    # Creating the mapping
    if bodyNow is not None:
        body = {"mappings": {index_name: {"properties": bodyNow}}}
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
def DSIO2ES_batchRestreamer(X, timefield_name, es = None, index_name = 'tele_full', reDate = True, everyX = 10, sleep = True):
    if reDate:
        X = batchRedater(X, timefield_name)

    if not sleep:
        everyX = 200

    virtualTime = np.min(X[timefield_name])
    recreate = True
    while virtualTime < np.max(X[timefield_name]):
        startTime = virtualTime
        virtualTime += everyX*1000
        endTime = virtualTime
        if sleep:
            while np.round(time.time()) < endTime/1000.:
                print('z')
                time.sleep(1)

        ind = np.logical_and(X[timefield_name] <= endTime, X[timefield_name] > startTime)
        print('Writing {} rows dated {} to {}'
              .format(np.sum(ind),
                      datetime.datetime.fromtimestamp(startTime/1000.),
                      datetime.datetime.fromtimestamp(endTime/1000.)))

        bodyNow = {"time" : {"type": "date"}}
        df2es(
            X.loc[ind],
            index_name,
            es=es,
            bodyNow=bodyNow,
            recreate=recreate
            )
        recreate = False
