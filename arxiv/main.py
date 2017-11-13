import sys
import json
import datetime

import pandas as pd
import numpy as np

# from pyelasticsearch import *
import elasticsearch as ES

from utils_esk import DSIO2ES_batchRestreamer


# Creating score function:
def batchScore(X, q = 0.99):
    tup = np.percentile(X,q)
    tdown = np.percentile(X, 1-q)
    scores = np.logical_or(X<tdown, X>tup)*0.9 + 0.1
    return scores

# Config:
print('Creating configuration file ... ')
fname = '../static/data/cardata_sample.csv'
sensorNames = set({'accelerator_pedal_position',
  'torque_at_transmission',
  'steering_wheel_angle',
  'brake_pedal_status',
  'vehicle_speed',
  'transmission_gear_position'})


tName = 'time'
index_name = 'tele-check'
_type = 'car'
tUnit = 's' # only seconds (s) and milliseconds (ms) supported
print('Done.\n')

# Checking:
print('Loading the data...')
D = pd.read_csv(fname, sep=',')
print('Done.\n')

Dcol = set(D.columns)

# Check that all sensor names given in config file are in data file:
if sensorNames.issubset(Dcol):
    print('Right sensors data available')
else:
    print('Missing sensors, aborting.')
    sys.exit()

if tName not in Dcol:
    print('Missing time column in data, aborting.')
    sys.exit()

if tUnit is None:
    print('No time unit given, aborting.')
    sys.exit()
elif tUnit == 's':
    print('Converting to milliseconds ...')
    D[tName] = np.floor(D[tName]*1000).astype('int')
    print('Done')

minTime = np.min(D[tName])
maxTime = np.max(D[tName])

print('NB: data found from {} to {}'.format(datetime.datetime.fromtimestamp(minTime/1000.), 
                                                datetime.datetime.fromtimestamp(maxTime/1000.)))

### CREATE ALERT STREAMS
Dpp = D[[tName]+ list(sensorNames)].copy()

for s in sensorNames:
    Dpp['SCORE_{}'.format(s)] = batchScore(Dpp[s].values)

### Adding index name and type for all events:
Dpp['_index'] = index_name
Dpp['_type'] = _type

### STREAM TO ELASTIC

# init ElasticSearch
es = ES.Elasticsearch('http://localhost:9200/')
DSIO2ES_batchRestreamer(X = Dpp, tName = tName, es = es, index_name = index_name, reDate = True, sleep = True)
