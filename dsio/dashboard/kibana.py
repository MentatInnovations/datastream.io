import elasticsearch

from kibana_dashboard_api import Visualization, Dashboard
from kibana_dashboard_api import VisualizationsManager, DashboardsManager

from ..exceptions import KibanaConfigNotFoundError


def generate_dashboard(es_conn, sensor_names, index_name, timefield='time',
                       update=True):
    """ Generate a Kibana dashboard given a list of sensor names """

    es_conn.index(index='.kibana', doc_type="index-pattern",
                    id=index_name,
                    body={
                        "title": index_name,
                        "timeFieldName": "time"
                    })

    dashboards = DashboardsManager(es_conn)
    dashboard = Dashboard()
    dashboard.id = "%s-dashboard" % index_name
    dashboard.title = "%s dashboard" % index_name
    dashboard.panels = []
    dashboard.options = {"darkTheme": True}
    dashboard.time_from = "now-15m"
    dashboard.refresh_interval_value = 5000
    dashboard.search_source = {
        "filter": [{
            "query": {
                "query_string": {
                    "analyze_wildcard": True,
                    "query": "*"
                }
            }
        }]
    }
    visualizations = VisualizationsManager(es_conn)
    vis_list = visualizations.get_all() # list all visualizations
    panels = []
    i = 0
    for sensor in sensor_names:
        viz_id = "%s-%s" % (index_name, sensor)

        # Check if visualization exists
        viz = next((v for v in vis_list if v.id == viz_id), None)
        if not viz: # If not, create it
            viz = Visualization()
            viz.id = viz_id
            viz.title = "%s-%s" % (index_name, sensor)
            viz.search_source = {
                "index": index_name,
                "query":{
                    "query_string":{
                        "analyze_wildcard": True,
                        "query":"*"
                    }
                },
                "filter":[]
            }
            viz.vis_state = {
                "title": "%s-%s" % (index_name, sensor),
                "type": "line",
                "params": {
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
                        "params": {
                            "field":"SCORE_%s" % sensor
                        }
                    }, {
                        "id": "3",
                        "type": "date_histogram",
                        "schema": "segment",
                        "params":{
                            "field": timefield,
                            "interval": "custom",
                            "customInterval": "5s",
                            "min_doc_count": 1,
                            "extended_bounds": {}
                        }
                    }
                ],
                "listeners": {}
            }
            try:
                res = visualizations.add(viz)
                assert res['_id'] == viz_id
            except elasticsearch.exceptions.ConflictError:
                if update:
                    res = visualizations.update(viz)

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
        ret = dashboard.add_visualization(viz)
        i += 1

    # Create the index if it does not exist
    if not es_conn.indices.exists(index_name):
        index_properties = {"time" : {"type": "date"}}
        body = {"mappings": {index_name: {"properties": index_properties}}}
        es_conn.indices.create(index=index_name, body=body)

    try:
        ret = dashboards.add(dashboard)
    except elasticsearch.exceptions.ConflictError:
        # Dashboard already exists, let's update it if we have to
        if update:
            ret = dashboards.update(dashboard)

    # Create the index pattern
    es_conn.index(index='.kibana', doc_type="index-pattern", id=index_name,
                  body={"title": index_name, "timeFieldName": "time"})

    # Search for kibana config
    kibana_config = es_conn.search(index='.kibana',
                                   sort={'_uid': {'order': 'desc'}},
                                   doc_type='config')
    try:
        kibana_id = kibana_config['hits']['hits'][0]['_id']
    except:
        raise KibanaConfigNotFoundError()

    es_conn.update(index='.kibana', doc_type='config', id=kibana_id,
                   body={"doc": {"defaultIndex" : index_name}})

    return ret
