import elasticsearch

from kibana_dashboard_api import Visualization, Dashboard
from kibana_dashboard_api import VisualizationsManager, DashboardsManager


def generate_dashboard(es_conn, sensor_names, df_scored, index_name):
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
                        "id": "2",
                        "type": "max",
                        "schema":"radius",
                        "params": {
                            "field":"SCORE_%s" % sensor
                        }
                    }
                ],
                "listeners": {}
            }
            try:
                res = visualizations.add(viz)
                assert res['_id'] == viz_id
            except elasticsearch.exceptions.ConflictError:
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

    try:
        ret = dashboards.add(dashboard)
    except elasticsearch.exceptions.ConflictError:
        ret = dashboards.update(dashboard)
        #pass # Dashboard already exists, let's not overwrite it

    return ret
