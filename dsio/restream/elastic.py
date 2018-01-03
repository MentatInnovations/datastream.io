"""
Elasticsearch batch re-streamer
"""
import time
import datetime
import sys

import numpy as np

import elasticsearch

from elasticsearch.helpers import bulk

from ..exceptions import ElasticsearchConnectionError


def init_elasticsearch(uri):
    # init ElasticSearch
    es_conn = elasticsearch.Elasticsearch(uri)
    try:
        es_conn.info()
    except elasticsearch.ConnectionError:
        raise ElasticsearchConnectionError(uri)

    return es_conn


def batch_redater(dataframe, timefield, frequency=10):
    """ send 10 datapoints a second """
    now = np.int(np.round(time.time()))
    dataframe[timefield] = (now*1000 + dataframe.index._data*frequency)
    return dataframe


def upload_dataframe(es_conn, dataframe, index_name, entry_type,
                     recreate=False, chunk_size=100):
    """ Upload dataframe to Elasticsearch """
    # Make sure previous indices with similar name are erased and create a new index
    if recreate:
        try:
            es_conn.indices.delete(index_name)
            print('Deleting existing index {}'.format(index_name))
        except elasticsearch.TransportError:
            pass

        print('Creating index {}'.format(index_name))
        es_conn.indices.create(index_name, body={
            "mappings": {
                index_name: {
                    "properties": {
                        "time" : {
                            "type": "date"
                        }
                    }
                }
            }
        })

    ### Adding index name and type for all events:
    dataframe.insert(1, '_index', index_name)
    dataframe.insert(1, '_type', entry_type)

    # Format the batch to upload as a tuple of dictionaries
    list_tmp = tuple(dataframe.fillna(0).T.to_dict().values())

    # Export to ES
    out = bulk(es_conn, list_tmp, chunk_size=chunk_size)

    return out


def elasticsearch_batch_restreamer(dataframe, timefield, es_conn, index_name,
                                   interval=10, first_pass=True):
    """
    Replay input stream into Elasticsearch
    """
    end_time = np.min(dataframe[timefield])
    while end_time < np.max(dataframe[timefield]):
        start_time = end_time
        end_time += interval*1000
        if not first_pass:
            while np.round(time.time()) < end_time/1000.:
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(1)

        ind = np.logical_and(dataframe[timefield] <= end_time,
                             dataframe[timefield] > start_time)
        print('\nWriting {} rows dated {} to {}'
              .format(np.sum(ind),
                      datetime.datetime.fromtimestamp(start_time/1000.),
                      datetime.datetime.fromtimestamp(end_time/1000.)))

        index_properties = {"time" : {"type": "date"}}
        upload_dataframe(dataframe.loc[ind], index_name, es_conn,
                         index_properties, recreate=first_pass)
        first_pass = False
