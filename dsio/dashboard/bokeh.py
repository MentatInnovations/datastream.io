""" bokeh dashboard integration """
import threading
import webbrowser

import tornado.ioloop

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure, ColumnDataSource
from bokeh.layouts import gridplot
from bokeh.models import HoverTool
from bokeh.io import output_notebook, show


def generate_dashboard(sensors, title, cols=3, port=5001, update_queue=None):
    """ Returns a bokeh server configured with the dsio dashboard app"""

    def make_document(doc):
        """ Generates the dashboard document """
        # Initialize the data source
        data = {'time': []}
        for sensor in sensors:
            sensor_score = 'SCORE_%s' % sensor
            sensor_flag = 'FLAG_%s' % sensor
            data[sensor] = []
            data[sensor_score] = []
            data[sensor_flag] = []
        source = ColumnDataSource(data=data)

        # Add figure for each sensor
        tools = 'pan,wheel_zoom,xbox_select,reset'
        figures = []
        for sensor in sensors:
            fig = figure(title=sensor, tools=tools, x_axis_type='datetime',
                         plot_width=600, plot_height=300)
            fig.line('time', sensor, source=source)
            sensor_score = 'SCORE_%s' % sensor
            sensor_flag = 'FLAG_%s' % sensor
            fig.circle('time', sensor, size=5, source=source, color='red', fill_alpha=sensor_flag, line_alpha=0)
            hover = HoverTool(
                tooltips=[
                    ("time", "@time{%F %T}"),
                    ("value", "@%s" % sensor),
                    ("score", "@%s" % sensor_score),
                ],
                formatters={"time": "datetime"},
                mode='vline'
            )
            fig.add_tools(hover)

            if figures: # share the x-axis across all figures
                fig.x_range = figures[0].x_range

            figures.append(fig)

        grid = gridplot(figures, ncols=cols, sizing_mode='scale_width')
        doc.title = title
        doc.add_root(grid)

        def update():
            """ Check the queue for updates sent by the restreamer thread
                and pass them over to the bokeh data soure """
            data = update_queue.get().to_dict('list')
            source.stream(data)

        if update_queue: # Update every second
            doc.add_periodic_callback(update, 1000)

    app = Application(FunctionHandler(make_document))

    io_loop = tornado.ioloop.IOLoop.current()

    if io_loop._running: # Assume we're in a Jupyter notebook
        output_notebook()
        show(app)
    else: # Otherwise start the bokeh server in a thread and open browser
        server = Server({'/': app}, port=port)
        dashboard_thread = threading.Thread(target=server.io_loop.start, daemon=True)
        dashboard_thread.start()
        webbrowser.open('http://localhost:%s' % port)
