"""
1. reads an input data stream that consists of sensor data
2. generates a Kibana dashboard to visualize the data stream
3. applies scores to the incoming sensor data, using different models
4. restreams input data and scores to ElasticSearch
"""

import argparse
import sys
import time
import webbrowser

import dateparser
import elasticsearch as ES
import numpy as np
import pandas as pd

from kibana_dashboard_api import Visualization, Dashboard
from kibana_dashboard_api import VisualizationsManager, DashboardsManager

from .restream.elastic import elasticsearch_batch_restreamer


def batch_score(X, q=0.99):
    """ Score function """
    tup = np.percentile(X, q)
    tdown = np.percentile(X, 1-q)
    scores = np.logical_or(X < tdown, X > tup)*0.9 + 0.1
    return scores


def detect_time(dataframe, timeunit):
    """ Attempt to detect the time dimension in a dataframe """
    columns = set(dataframe.columns)
    for tfname in ['time', 'datetime', 'date', 'timestamp']:
        if tfname in columns:
            prev = current = None
            for i in dataframe[tfname][:10]:
                try:
                    current = dateparser.parse(str(i))
                    # timefield needs to be parsable and always increasing
                    if not current or (prev and prev > current):
                        tfname = ''
                        break
                except TypeError:
                    tfname = ''
                    break
            prev = current
            if tfname:
                timefield = tfname
                if isinstance(i, float) and not timeunit:
                    timeunit = 's'
                break

    return timefield, timeunit


def generate_dashboard(es_conn, sensor_names, df_scored, index_name):
    dashboards = DashboardsManager(es_conn)
    dashboard = Dashboard()
    dashboard.id = "%s-dashboard" % index_name
    dashboard.title = "%s dashboard" % index_name
    dashboard.panels = []
    dashboard.options = {"darkTheme": True}
    dashboard.time_from = "now-15m"
    dashboard.search_source = {
        "filter": [{
            "query": {
                "query_string": {
                    "analyze_wildcard": True,
                    "query":"*"
                }
            }
        }]
    }
    visualizations = VisualizationsManager(es_conn)
    # list all visualizations
    #vis_list = visualizations.get_all()
    panels = []
    i = 0
    for sensor in sensor_names:
        viz_id = "%s-%s" % (index_name, sensor)
        vizualization = Visualization()
        vizualization.id = viz_id
        vizualization.title = "%s-%s" % (index_name, sensor)
        vizualization.search_source = {
            "index": index_name,
            "query":{
                "query_string":{
                    "analyze_wildcard": True,
                    "query":"*"
                }
            },
            "filter":[]
        }
        vizualization.vis_state = {
            "title":"%s-%s" % (index_name, sensor),
            "type":"line",
            "params":{
                "addLegend": True,
                "addTimeMarker": True,
                "addTooltip": True,
                "defaultYExtents": True,
                "drawLinesBetweenPoints": True,
                "interpolate": "linear",
                "radiusRatio": 9,
                "scale": "linear",
                "setYExtents": False,
                "shareYAxis": True,
                "showCircles": True,
                "smoothLines": True,
                "times":[],
                "yAxis":{}
            },
            "aggs": [
                {
                    "id": "1",
                    "type": "avg",
                    "schema":"metric",
                    "params": {
                        "field": sensor,
                        "customLabel": sensor.replace('_', ' ')
                    }
                }, {
                    "id": "2",
                    "type": "max",
                    "schema":"radius",
                    "params":{
                        "field":"SCORE_%s" % sensor
                    }
                }, {
                    "id": "3",
                    "type": "date_histogram",
                    "schema": "segment",
                    "params":{
                        "field": "time",
                        "interval": "custom",
                        "customInterval": "5s",
                        "min_doc_count":1,
                        "extended_bounds":{}
                    }
                }
            ],
            "listeners": {}
        }
        try:
            viz = visualizations.add(vizualization)
        except ES.exceptions.ConflictError:
            pass # Visualization already exists, let's not overwrite it

        panel = {
            "id": viz_id,
            "panelIndex": i,
            "row": i,
            "col": i,
            "size_x": 7,
            "size_y": 4,
            "type": "visualization"
        }
        panels.append(panel)
        ret = dashboard.add_visualization(vizualization)
        i += 1

    try:
        ret = dashboards.add(dashboard)
    except ES.exceptions.ConflictError:
        pass # Dashboard already exists, let's not overwrite it

    return ret


def main():
    """ Main function """

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--es-uri", help="output elasticsearch uri",
                        default="http://localhost:9200/")
    parser.add_argument("--kibana-uri", help="Kibana uri",
                        default="http://localhost:5601/app/kibana")
    parser.add_argument("--es-index", help="Elasticsearch index name")
    parser.add_argument("--entry-type", help="entry type name",
                        default="measurement")
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    parser.add_argument("-s", '--sensors', help="select specific sensor names",
                        nargs='+')
    parser.add_argument("-t", "--timefield",
                        help="name of the column in the data that defines the time",
                        default="")
    parser.add_argument("-u", "--timeunit", help="time unit",
                        default="")
    parser.add_argument('input', help='input file or stream')
    args = parser.parse_args()

    print('Loading the data...')
    dataframe = pd.read_csv(args.input, sep=',')
    print('Done.\n')

    columns = set(dataframe.columns)

    # Get timefield & timeunit from args
    timefield = args.timefield
    timeunit = args.timeunit
    if timefield and timefield not in columns:
        print('Missing time column in data, aborting.')
        sys.exit()

    if not timefield: # Try to auto detect timefield
        timefield, timeunit = detect_time(dataframe, timeunit)

    # If no timefield can be detected treat input as timeseries
    if not timefield:
        # Add time dimension with fixed intervals starting from now
        timefield = 'time'
        timeunit = 's'
        start = int(time.time())
        dataframe[timefield] = pd.Series(
            range(start, start+dataframe.shape[0]),
            index=dataframe.index
        )

    available_sensor_names = set(dataframe.columns).copy()
    available_sensor_names.remove(timefield)

    # Get selected sensors from args
    if args.sensors:
        sensor_names = set(args.sensors)
    else: # Get all sensors if none selected
        sensor_names = available_sensor_names

    # Check that all sensor names given in config file are in data file
    if sensor_names.issubset(available_sensor_names):
        print('Right sensors data available')
    else:
        print('Missing sensors, aborting.')
        sys.exit()

    # Get ES index name and type from args or generate from input name
    index_name = args.es_index
    if not index_name and args.input:
        index_name = args.input.split('/')[-1].split('.')[0].split('_')[0]
    if not index_name:
        index_name = 'dsio'
    _type = args.entry_type

    min_time = dataframe[timefield][0]
    max_time = dataframe[timefield][dataframe[timefield].size-1]

    print('NB: data found from {} to {}'\
          .format(dateparser.parse(str(min_time)),
                  dateparser.parse(str(max_time))))

    if timeunit == 's':
        print('Converting to milliseconds ...')
        dataframe[timefield] = np.floor(dataframe[timefield]*1000).astype('int')
        print('Done')

    ### Create alert streams
    df_scored = dataframe[[timefield] + list(sensor_names)].copy()

    for sensor in sensor_names:
        try:
            df_scored['SCORE_{}'.format(sensor)] = batch_score(df_scored[sensor].values)
        except TypeError: # Map string values to int category codes
            values = df_scored[sensor].astype('category').cat.codes
            df_scored['SCORE_{}'.format(sensor)] = batch_score(values)

    ### Adding index name and type for all events:
    df_scored['_index'] = index_name
    df_scored['_type'] = _type

    # init ElasticSearch
    es_conn = ES.Elasticsearch(args.es_uri)
    try:
        es_conn.info()
    except ES.ConnectionError:
        print('Cannot connect to Elasticsearch at %s' % args.es_uri)
        sys.exit()

    # Generate dashboard with selected fields and scores
    dashboard = generate_dashboard(es_conn, sensor_names, df_scored, index_name)
    webbrowser.open(args.kibana_uri+'#/dashboard/%s-dashboard' % index_name)
    # Steam to Elasticsearch
    elasticsearch_batch_restreamer(
        X=df_scored, timefield=timefield,
        es=es_conn, index_name=index_name, redate=True, sleep=True
    )


if __name__ == '__main__':
    main()
