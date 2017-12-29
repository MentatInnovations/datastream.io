"""
Elasticsearch batch re-streamer
"""
import time
import datetime

import numpy as np

import elasticsearch

from elasticsearch.helpers import bulk

from ..exceptions import ElasticsearchConnectionError


def init_elasticsearch(uri, index_name, entry_type, input, dataframe):
    # Get ES index name and type from args or generate from input name
    if not index_name and input:
        index_name = input.split('/')[-1].split('.')[0].split('_')[0]
    if not index_name:
        index_name = 'dsio'

    ### Adding index name and type for all events:
    dataframe['_index'] = index_name
    dataframe['_type'] = entry_type

    # init ElasticSearch
    es_conn = elasticsearch.Elasticsearch(uri)
    try:
        es_conn.info()
    except elasticsearch.ConnectionError:
        raise ElasticsearchConnectionError(uri)

    return es_conn, index_name, dataframe


def batch_redater(dataframe, timefield, frequency=10):
    """ send 10 datapoints a second """
    now = np.int(np.round(time.time()))
    dataframe[timefield] = (now*1000 + dataframe.index._data*frequency)
    return dataframe


def upload_dataframe(dataframe, index_name, es_conn, index_properties=None,
                     recreate=False, chunk_size=100):
    """ Upload dataframe to Elasticsearch """
    # Make sure previous indices with similar name are erased and create a new index
    if recreate:
        try:
            es_conn.indices.delete(index_name)
            print('Deleting existing index {}'.format(index_name))
        except elasticsearch.TransportError:
            pass

        # Create the mapping
        if index_properties is not None:
            body = {"mappings": {index_name: {"properties": index_properties}}}

        print('Creating index {}'.format(index_name))
        es_conn.indices.create(index_name, body=body)

    # Format the batch to upload as a tuple of dictionaries
    list_tmp = tuple(dataframe.fillna(0).T.to_dict().values())

    # Export to ES
    out = bulk(es_conn, list_tmp, chunk_size=chunk_size)

    return out


def elasticsearch_batch_restreamer(dataframe, timefield, es_conn, index_name,
                                   redate=True, interval=10,
                                   sleep=True, first_pass=True):
    """
    Replay input stream into Elasticsearch
    """
    if redate:
        dataframe = batch_redater(dataframe, timefield)

    if not sleep:
        interval = 200

    virtual_time = np.min(dataframe[timefield])
    while virtual_time < np.max(dataframe[timefield]):
        start_time = virtual_time
        virtual_time += interval*1000
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
        upload_dataframe(dataframe.loc[ind], index_name, es_conn,
                         index_properties, recreate=first_pass)
        first_pass = False
