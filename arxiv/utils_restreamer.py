import os
import math
import functools
import logging
import sys
import csv
import fcntl
import pickle
import json
import time
import pytz
from time import mktime as mktime
from datetime import datetime, timedelta

import pandas as pd

import numpy as np

import pika

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from boto.s3.connection import S3Connection

from filechunkio import FileChunkIO


LOGGER = logging.getLogger(__name__)
# Quick and dirty solution of producing re-streamed files (played again using
# timestamps that are close to the current running timestamps)
# from the original file used as a template
# TODO export this configuration to a file.
gconf = {
    'allignTimestampsToNowTime': False,
    'liveMode': False,
    'timezone': 'UTC',
    # Data input configuration
    'dataTemplateFile': '../static/data/kddcup_sample.csv',
    'datasetHasTimeField': False,
    'header': [
        "duration", "protocol_type", "service", "flag", "src_bytes",
        "dst_bytes", "land", "wrong_fragment", "urgent", "hot",
        "num_failed_logins", "logged_in", "num_compromised", "root_shell",
        "su_attempted", "num_root", "num_file_creations", "num_shells",
        "num_access_files", "num_outbound_cmds", "is_host_login",
        "is_guest_login", "count", "srv_count", "serror_rate",
        "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate",
        "diff_srv_rate", "srv_diff_host_rate", "dst_host_count",
        "dst_host_srv_count", "dst_host_same_srv_rate",
        "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
        "dst_host_srv_diff_host_rate", "dst_host_serror_rate",
        "dst_host_srv_serror_rate", "dst_host_rerror_rate",
        "dst_host_srv_rerror_rate", "truelabel"
        ],
    'renameMapping': {
        'STARTUTCINMS': 'timestamp'
    },
    'normalizeField': '',
    'outputFolder': '/home/kostas/dev/data/kdd/restreamed/',
    # Write to file configurations
    'writeToFile': False,
    'compression': 'gzip',
    'includeHeader': False,
    'chunkFilePrefix': 'kdd_99_',
    'dateTimePatternInProducedFiles': 'YYYY-mm-dd',
    'numberOfDaysToReplay': 2,
    'restreamFromLastSuccessfullChunkOnly': False,
    'chunkSize': 50,
    'sleepBetweenChunksSeconds':5,
    'slowdownafter':30000,
    'separator': ',',
    'removeQuotes': False,
    'removeNewlineChars': False,
    # Write to Elastic configurations
    'writeToElastic': False,
    'index_name_prefix': "kdd_99_",
    'index_date_format': "%Y-%m-%d"
    ,
    'delete_previous_es_data': False,
    'esColumnsTokeep': {
        "DEVICE_ID": "sourceId",
        "BYTESREADUPSTREAM": "sumOf",
        "APP": "key2",
        "SOURCE_IP": "sourceIp",
        "DESTINATION_IP": "destinationIp"
    },
    'columnsToIndex': [
        "sourceId",
        "sumOf",
        "key2",
        "sourceIp",
        "destinationIp",
        "timestamp"
    ],
    # Write to rabbit configurations
    'writeToRabbit': True,
    'rabbitmqHost': '88.198.121.88 ',
    'rabbitmqPort': 5672,
    'rabbitmqUser': 'guest',
    'rabbitmqPassword': 'guest123!',
    'rabbitmqExchange': 'demo_security',
    'rabbitmqQueue': 'demo_security',
    'rabbitmqRoutingKey': 'demo_security',

    'writeToS3': False,
    'AWS_ACCESS_KEY_ID': 'AKIAISS4RSFCYVDLYQBA',
    'AWS_SECRET_ACCESS_KEY': '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',
    'AWS_S3_BUCKET': 'dsio-datasets'
}

# keep metadata for the last file and chunk processed
# last_chunk_of_last_file_processed = None
# last_file_path_read = None
# last_size_of_last_chunk_file_processed = None

METADATA_FILE = './kdd99_last_file_processed_metadata.info'

if gconf['writeToElastic']:
    es = Elasticsearch({'localhost:9200'})

if gconf['writeToRabbit']:
    # es = Elasticsearch(["seed1", "seed2"], sniff_on_start=True)
    rabbitmq_connection = pika.BlockingConnection(
        parameters=pika.connection.ConnectionParameters(host=gconf[
            'rabbitmqHost'], port=gconf['rabbitmqPort'], credentials=
                                                        pika.credentials.PlainCredentials(
                                                            username=gconf[
                                                                'rabbitmqUser'],
                                                            password=gconf[
                                                                'rabbitmqPassword'])
                                                        ))
    rabbitmq_channel = rabbitmq_connection.channel()

if gconf['writeToS3']:
    conn = S3Connection(gconf['AWS_ACCESS_KEY_ID'],
                        gconf['AWS_SECRET_ACCESS_KEY'])
    s3_bucket = conn.get_bucket(gconf['AWS_S3_BUCKET'])

LOG_FORMAT = ('%(levelname) -10s    %(asctime)s %(name) -5s %(funcName) '
              '-5s %(lineno) -5d: %(message)s')


def timing(func):
    @functools.wraps(func)
    def newfunc(*args, **kwargs):
        startTime = time.time()
        to_return = func(*args, **kwargs)
        elapsedTime = time.time() - startTime
        LOGGER.info('function [{}] finished in {} ms'.format(
            func.__name__, int(elapsedTime * 1000)))
        return to_return

    return newfunc


@timing
def write_to_elastic_search(dict_to_es):
    LOGGER.info('Writing to elasticsearch')
    # index_name = get_index_name(time)
    # # es.bulk_index( doc_type='raw',
    # #               docs=list_tmp)
    helpers.bulk(es, dict_to_es, chunk_size=gconf['chunkSize'])


@timing
def write_to_rabbit(writable_list):
    LOGGER.info('Writing to rabbitmq')
    exchange = gconf['rabbitmqExchange']
    routing_key = gconf['rabbitmqRoutingKey']
    for element in writable_list:
        rabbitmq_channel.basic_publish(exchange=exchange,
                                       routing_key=routing_key,
                                       body=json.dumps(element))


@timing
def write_to_rabbit_csv(final_data_frame):
    LOGGER.info('Writing to rabbitmq csv')
    exchange = gconf['rabbitmqExchange']
    routing_key = gconf['rabbitmqRoutingKey']
    for num in range(final_data_frame.shape[0]):
        row = final_data_frame.iloc[num]  # this a dataframe that
        # corresponds to the num line of the big dataframe
        csv_string=final_data_frame[num:(num+1)].to_csv(path_or_buf=None,
                                                     index=False,
                                                        header=False)
        # csv_string = StringIO()
        # row.to_csv(csv_string, header=False)
        # csv_final_string=csv_string.getvalue()
        rabbitmq_channel.basic_publish(exchange=exchange,
                                       routing_key=routing_key,
                                       body=csv_string)


@timing
def read_template_file_in_chunks(gconf, config_dict):
    LOGGER.info('Reading file %s', gconf['dataTemplateFile'])

    # the Data frame that will host the part of the file we have
    # supports compression out-of-the-box for gzip
    current_chunk = 0
    # set to global variable
    last_file_path_read = config_dict['last_file_path_read']
    last_size_of_last_chunk_file_processed = config_dict[
        'last_size_of_last_chunk_file_processed']
    last_chunk_of_last_file_processed = config_dict[
        'last_chunk_of_last_file_processed']
    for chunk in pd.read_csv(last_file_path_read,
                             header=None,
                             sep=gconf['separator'],
                             names=gconf['header'],
                             chunksize=last_size_of_last_chunk_file_processed,
                             skiprows=int(1)):

        if 0 <= current_chunk <= last_chunk_of_last_file_processed:
            LOGGER.info("Skipping chunk {0} file and reading from previous "
                        "file: {1} with chunk size {2}".format(current_chunk,
                                                               last_file_path_read,
                                                               last_size_of_last_chunk_file_processed))
            current_chunk += 1
            continue
        LOGGER.info('Reading chunk:{0}'.format(current_chunk))
        read_template_file_and_produce_chunks(chunk)
        last_chunk_of_last_file_processed = current_chunk
        persist_last_state(last_file_path_read,
                           last_size_of_last_chunk_file_processed,
                           last_chunk_of_last_file_processed)
        current_chunk += 1
        if gconf['sleepBetweenChunksSeconds']>0:
            time.sleep(gconf['sleepBetweenChunksSeconds'])



def normalize_app(app):
    if ':' in app:
        return app.split(':', 1)[0].strip()
    elif '(' in app:
        return app.split('(', 1)[0].strip()
    else:
        return app.strip()


@timing
def apply_tranformations_on_writable_df(dataframe_to_process):
    field_to_transform = gconf['normalizeField']
    dataframe_to_process[field_to_transform] = \
        dataframe_to_process[field_to_transform].apply(
            lambda x: normalize_app(
                x))
    return dataframe_to_process


def read_template_file_and_produce_chunks(original_data_frame):
    # do the renaming in the absolutely needed fields
    # need to use. The time
    if gconf['datasetHasTimeField']:
        rename_time_field(original_data_frame)
        sort_chunk_by_time_asc(original_data_frame)

    dataframe_to_process = original_data_frame
    if gconf['allignTimestampsToNowTime']:
        LOGGER.info('Now aligning timestamps ...')
        aligned_timestamps_df = align_timestamps(original_data_frame)
        max_time = pd.Timestamp(
            pd.to_datetime(max(aligned_timestamps_df['timestamp']),
                           unit='ms'),
            tz=gconf['timezone'])

        tz = pytz.timezone(gconf['timezone'])

        now = datetime.now(tz)
        if gconf['liveMode']:
            while max_time > now:
                LOGGER.warn("Re-streaming in live mode and max hour exceeds "
                            "current, so going back to sleep for 5 secs")
                time.sleep(5)
                now = datetime.now(tz)

        dataframe_to_process = aligned_timestamps_df

    LOGGER.info('Ready to stream...')

    if gconf['normalizeField']!='':
        final_df_to_write = apply_tranformations_on_writable_df(
            dataframe_to_process)
    else:
        final_df_to_write = dataframe_to_process

    if gconf['writeToFile']:
        if gconf['compression'] is None:
            file_name = gconf['outputFolder'] + gconf['chunkFilePrefix'] \
                        + time.strftime('%Y-%m-%d-%H%M%S') + '.csv'
        else:
            file_name = gconf['outputFolder'] + gconf['chunkFilePrefix'] \
                        + time.strftime('%Y-%m-%d-%H%M%S') + '.gz'
        LOGGER.info("Writing to file %s", file_name)

        file_name = write_to_file(file_name, final_df_to_write)
        if gconf['writeToS3']:
            write_to_s3(file_name)

    if gconf['writeToElastic'] or gconf['writeToRabbit']:
        write_to_external_systems(final_df_to_write.copy(deep=True))


def persist_last_state(last_file_path_read,
                       last_size_of_last_chunk_file_processed,
                       last_chunk_of_last_file_processed):
    dict = {
        'last_chunk_of_last_file_processed': last_chunk_of_last_file_processed,
        'last_file_path_read': last_file_path_read,
        'last_size_of_last_chunk_file_processed': last_size_of_last_chunk_file_processed
    }
    with open(METADATA_FILE, 'wb') as handle:
        pickle.dump(dict, handle)


@timing
def write_to_file(file_name, frame_for_time_range):
    if gconf['removeQuotes']:
        frame_for_time_range.ix[:,
        frame_for_time_range.dtypes == object].apply(
            lambda s: s.str.replace('"', ""))
    if gconf['removeNewlineChars']:
        frame_for_time_range.ix[:,
        frame_for_time_range.dtypes == object].apply(
            lambda s: s.str.replace('\n', ""))
    frame_for_time_range.to_csv(path_or_buf=file_name, index=False,
                                quotechar='"',
                                quoting=csv.QUOTE_ALL,
                                sep=',',
                                header=gconf['includeHeader'],
                                compression='gzip')
    return file_name


@timing
def align_timestamps(data_frame):
    LOGGER.info("Alligning timestamps to current time")
    max_time = pd.Timestamp(
        pd.to_datetime(max(data_frame['timestamp']), unit='ms'),
        tz=gconf['timezone'])
    now_time = pd.Timestamp(
        pd.to_datetime(int(np.floor(time.time()) * 1000), unit='ms'),
        tz=gconf['timezone'])
    min_time = pd.Timestamp(
        pd.to_datetime(min(data_frame['timestamp']), unit='ms'),
        tz=gconf['timezone'])
    if max_time > now_time:
        raise AssertionError("Max time of data frame is after the current "
                             "time")

    # add delta in days to all points of data frame
    max_delta_in_days = np.abs(now_time.date() - max_time.date())
    min_delta_in_days = np.abs(now_time.date() - min_time.date())
    diff_of_max_min_deltas = abs(max_delta_in_days.days - \
                                 min_delta_in_days.days)

    if diff_of_max_min_deltas > 1:
        raise AssertionError("Cannot support data frames with 2 days diff")
    elif diff_of_max_min_deltas == 1:

        floor_date = now_time.date() - min_delta_in_days
        floor_date_millis = int(mktime(floor_date.timetuple()) * 1000)
        # butch dataframe into two pieces and add the different deltas
        first_day_df = data_frame.loc[data_frame.timestamp <=
                                      floor_date_millis]
        second_day_df = data_frame.loc[
            data_frame.timestamp > floor_date_millis]
        first_day_df.timestamp += int(timedelta(days=max_delta_in_days.days)
                                      .total_seconds() * 1000)
        second_day_df.timestamp += int(timedelta(days=min_delta_in_days.days)
                                       .total_seconds() * 1000)
        return pd.concat([first_day_df, second_day_df])
    else:
        if max_time + max_delta_in_days > now_time:
            data_frame.timestamp += int(
                timedelta(days=max_delta_in_days.days - 1)
                .total_seconds() * 1000)
        else:
            data_frame.timestamp += int(timedelta(days=max_delta_in_days.days)
                                        .total_seconds() * 1000)
    return data_frame


@timing
def sort_chunk_by_time_asc(D):
    D.sort('timestamp')


@timing
def rename_time_field(D):
    D.rename(columns=gconf['renameMapping'], inplace=True)


@timing
def get_index_name(time):
    return gconf['index_name_prefix'] + time.strftime(gconf[
                                                          'index_date_format']
                                                      )


def to_es_dict(copy_of_df):
    index_prefix = gconf['index_name_prefix']
    index_date_format = gconf['index_date_format']
    copy_of_df['_type'] = "raw"
    copy_of_df['_index'] = \
        copy_of_df.timestamp.apply(lambda line:
                                   index_prefix +
                                   datetime.
                                   fromtimestamp(
                                       line / 1000).strftime(
                                       index_date_format))

    return copy_of_df.fillna(0).T.to_dict().values()


def write_to_s3(file_path):
    # The
    # rule
    # enforced
    # by
    # S3 is that
    # all
    # parts except the
    # last
    # part
    # must
    # be >= 5
    # MB.If
    # the
    # first
    # part is also
    # the
    # last
    # part, this
    # rule
    # isn
    # 't violated and S3 accepts the small file as a multipart upload.
    LOGGER.info("Writing to amazon s3 %s", file_path)
    source_size = os.stat(file_path).st_size
    mp = s3_bucket.initiate_multipart_upload(os.path.basename(file_path))
    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / float(chunk_size)))
    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(file_path, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)
    mp.complete_upload()


@timing
def write_to_external_systems(data_frame):
    LOGGER.info('Writing to external systems ')
    try:
        write_to_rabbit_csv(data_frame)
        # data_frame.rename(columns=gconf['esColumnsTokeep'],
        #                   inplace=True)
        # final_data_frame = data_frame[gconf['columnsToIndex']]
        #
        # if gconf['writeToElastic']:
        #     es_dict = to_es_dict(final_data_frame.copy())
        #     # first write to ES
        #     write_to_elastic_search(es_dict)
        # if gconf['writeToRabbit']:
        #     writable_dict = to_writable_dict(final_data_frame)
        #     # then we write to Rabbit using pika
        #     write_to_rabbit(writable_dict)
    except Exception as e:
        LOGGER.error("An error occurred", e)


@timing
def to_writable_dict(final_data_frame):
    writable_dict = final_data_frame.fillna(0).T.to_dict().values()
    return writable_dict


def validate_conf(gconf):
    if not os.path.isfile(gconf['dataTemplateFile']):
        raise Exception("File {0} does not exist , check dataTemplateFile "
                        "config"
                        .format(gconf['dataTemplateFile']))
    if gconf['writeToFile']:
        if not os.path.isdir(gconf['outputFolder']):
            raise Exception("Path {0} is not a valid dir, check outputFolder "
                            "config"
                            .format(gconf['outputFolder']))


def file_is_locked(file_path):
    global file_handle
    file_handle = open(file_path, 'w')
    try:
        fcntl.lockf(file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False
    except IOError:
        return True


file_path = './pip.lock'

if file_is_locked(file_path):
    LOGGER.info("another instance is running exiting now")
    sys.exit(0)
else:
    LOGGER.info("no other instance is running")
    for i in range(5):
        time.sleep(1)


# def read_conf_from_file(filename, sep='\n'):
#     with open(filename, "r") as f:
#         dict = {}
#         for line in f:
#             values = line.split(sep)
#             dict[values[0]] = {int(x) for x in values[1:len(values)]}
#         return (dict)


def remove_lock_file():
    os.remove(file_path)


def read_or_init_restreamed_metadata(file_path):
    if gconf['restreamFromLastSuccessfullChunkOnly']:
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as f:
                result = pickle.loads(f.read())

                if 'last_chunk_of_last_file_processed' in result:
                    if result['last_chunk_of_last_file_processed'] is None:
                        result['last_chunk_of_last_file_processed'] = 0
                else:
                    result['last_chunk_of_last_file_processed'] = 0

                if 'last_file_path_read' in result:
                    if result['last_file_path_read'] is None:
                        result['last_file_path_read'] = gconf['dataTemplateFile']
                else:
                    result['last_file_path_read'] = gconf['dataTemplateFile']

                if 'last_size_of_last_chunk_file_processed' in result:
                    if result['last_size_of_last_chunk_file_processed'] is None:
                        result['last_size_of_last_chunk_file_processed'] = gconf[
                            'chunkSize']
                else:
                    result['chunkSize'] = gconf['chunkSize']

                return result

        else:
            result = {
                'last_chunk_of_last_file_processed': 0,
                'last_file_path_read': gconf['dataTemplateFile'],
                'last_size_of_last_chunk_file_processed': gconf['chunkSize']
            }
            with open(file_path, 'wb') as handle:
                pickle.dump(result, handle)
            return result
    else :
        result = {
            'last_chunk_of_last_file_processed': 0,
            'last_file_path_read': gconf['dataTemplateFile'],
            'last_size_of_last_chunk_file_processed': gconf['chunkSize']
        }
        return result


def main():
    if file_is_locked(file_path):
        raise Exception("An other instance of the process is running")
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    validate_conf(gconf)
    config_dict = read_or_init_restreamed_metadata(
        METADATA_FILE)
    try:
        number_of_executions = gconf['numberOfDaysToReplay']
        for i in range(number_of_executions):
            LOGGER.info("==================Executing the %s ITERATION",
                        str(i))
            read_template_file_in_chunks(gconf, config_dict)
    except KeyboardInterrupt:
        LOGGER.warn('Ctrl+c was issued raising exception')
        remove_lock_file()
        raise
    except Exception as e:
        LOGGER.error("An unexpected error occurred", e)
        sys.exit()


if __name__ == '__main__':
    main()
