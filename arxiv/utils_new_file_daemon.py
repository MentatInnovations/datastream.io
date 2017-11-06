import sched, time
import logging
import functools
# from hdfs import InsecureClient
# from hdfs.ext.kerberos import KerberosClient
# from hdfs import InsecureClient
from snakebite.client import Client
import sys
import sched, time
import pickle
from threading import Timer
import pandas as pd

SUCCESS_FIRED_FILES_P = "success_fired_files.p"

scheduler = sched.scheduler(time.time, time.sleep)

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s    %(asctime)s %(name) -5s %(funcName) '
              '-5s %(lineno) -5d: %(message)s')

gconf = {
    'kerberos_security': False,
    'hdfs_name_node_url': 'hdp-ex1',
    'hdfs_name_node_port': 8020,
    'hdfs_paths_to_monitor': ['/user/zep'],
    'hdfs_file_prefix': 'heartbeats_v2_',
    'hdfs_file_suffix': '.csv',
    'hdfs_folder_poll_interval': 5
}

# In case hdfs server is kerberized choose kerberos

# if gconf['kerberos_security']:
#     client = KerberosClient(gconf['hdfs_name_node_url'])
# else:
#     client = InsecureClient(gconf['hdfs_name_node_url'], user='ann')

# Using snakebite
# https://snakebite.readthedocs.io/en/latest/client.html

if gconf['kerberos_security']:
    # consult the way the hdfs_hook of airflow works
    # https://github.com/apache/incubator-airflow/blob/master/airflow/hooks/hdfs_hook.py
    client = Client(gconf['hdfs_name_node_url'], gconf['hdfs_name_node_port'],
                    use_trash=False,
                    hdfs_namenode_principal=gconf['hdfs_namenode_principal'])
else:
    client = Client(gconf['hdfs_name_node_url'], gconf['hdfs_name_node_port'],
                    use_trash=False)
'''
Utility function to measure the time a function took to finish'''


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


''' validate the configuration object'''


def validate_conf(gconf):
    # TODO provide validation rules
    pass


'''
load previously saved pickled file that holds already fired files
'''


def load_previously_stored_pickled_file(run_context):
    try:
        LOGGER.info(
            "Re-loading saved pickle file:{0}".format(SUCCESS_FIRED_FILES_P))
        run_context['success_fired_files'] = pickle.load(
            open(SUCCESS_FIRED_FILES_P, "rb"))

    except IOError:
        LOGGER.warn("File {0} does not exist, creating empty one".format(
            SUCCESS_FIRED_FILES_P))
        run_context['success_fired_files'] = {}
        store_success_fired_files_path(run_context, None, None)


'''
Store dict to file holding all
'''


def store_success_fired_files_path(
        run_context, metadata_of_file_triggered=None,
        file_path=None):
    if metadata_of_file_triggered is not None and file_path is not None:
        run_context['success_fired_files'][
            file_path] = metadata_of_file_triggered

    pickle.dump(run_context['success_fired_files'],
                open(SUCCESS_FIRED_FILES_P, "wb"))


''' This should hold the file processing'''


@timing
def start_file_processing(file_path, run_context):
    # TODO canagnos implement
    # choose between dask
    # http://matthewrocklin.com/blog/work/2016/02/22/dask-distributed-part-2
    # copy to client.copyToLocal(file_path,"./file_path.tmp")
    # E.g copier=client.copyToLocal([
    # "/user/zep/transactions_v2_UtilitiesTaxesGovernment.csv"],"/home/kostas/data/")
    # for file in copier:print file
    # and then process with pd.read_csv("./file_path.tmp")
    # OR we might need to check if the
    # http://hdfscli.readthedocs.io/en/latest/api.html hdfs.ext.dataframe.read_dataframe
    # in conjuction with pandas can do the job directly without the need for
    # manually downloading the file

    return True


'''Store the file in dict and trigger other types of processing here'''


def trigger_new_file_found(file, run_context):
    LOGGER.info("Found file that matches criteria:{0} ".format(file['path']))
    metadata_of_file_triggered = {'size': file[
        'length'], 'modification_time': file['modification_time'],
                                  'status': 'START_PROCESSING'}
    store_success_fired_files_path(run_context, metadata_of_file_triggered,
                                   file['path'])
    run_success = start_file_processing(file['path'], run_context)

    # in case Successful processing change status
    if run_success:
        metadata_of_file_triggered['status'] = 'SUCCESS_PROCESSING'
        store_success_fired_files_path(run_context,
                                       metadata_of_file_triggered,
                                       file['path'])


def check_folder_for_new_files(run_context):
    LOGGER.info("Scaning hdfs path:{0} for new files".format(
        gconf['hdfs_paths_to_monitor']))
    for file in client.ls(gconf['hdfs_paths_to_monitor']):
        file_path = file['path']
        file_name = get_filename_from_unix_path(file_path)

        # check only files starting and ending with the configured
        # prefixes and suffixes
        if (file_name.startswith(gconf['hdfs_file_prefix']) and
                file_name.endswith(gconf['hdfs_file_suffix'])):
            if file_path in run_context['success_fired_files'] and \
                            run_context['success_fired_files'][file_path][
                                'status'] == 'SUCCESS_PROCESSING':
                # do nothing in case we have successfully fired for this new
                # file
                pass
            else:
                # check if file is being
                # written by the time we are checking it
                # based
                # on
                # file
                # length
                # TODO: ADD? CHECK FOR LAST MODIFICATION TIME and
                # modification_time

                init_size = file['length']
                time.sleep(1)
                file_generator = client.ls([file['path']],
                                           include_toplevel=False,
                                           include_children=True)
                # we should only iterate once since the list
                # should hold only one file inside
                for filein in file_generator:
                    if filein['length'] == init_size:
                        # trigger new file found event
                        trigger_new_file_found(filein, run_context)
                    elif filein['length'] > init_size:
                        # do nothing this file is still being written
                        pass
                    elif filein['length'] < init_size:
                        # TODO : check what we should do in case file
                        # is modified
                        raise AssertionError(
                            " The length of the file seems to "
                            "be smaller than the initial check"
                            "This can  be an update of the "
                            "existing after it was originally "
                            "put there")
    # recursively call your self using interval
    Timer(gconf['hdfs_folder_poll_interval'],
          check_folder_for_new_files(run_context),
          ()).start()


def get_filename_from_unix_path(file_path):
    index_of_last_slash = file_path.rindex('/')
    file_name = file_path[index_of_last_slash + 1:]
    return file_name


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    validate_conf(gconf)
    try:
        # initialize empty context dict
        run_context = {}
        load_previously_stored_pickled_file(run_context)
        LOGGER.info('Start polling for hdfs path')
        check_folder_for_new_files(run_context)
    except KeyboardInterrupt:
        LOGGER.warn('Ctrl+c was issued raising exception')
        # make sure we persist one last time before process dies
        store_success_fired_files_path(run_context)
        raise
    except Exception as e:
        LOGGER.error("An unexpected error occurred:{0}".format(e.message),
                     e)
        sys.exit()


if __name__ == '__main__':
    main()
