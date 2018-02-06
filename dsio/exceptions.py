"""Custom exceptions used by dsio

Error handling in dsio is done via exceptions. In this file we define a
variety of exceptions.

There is DsioError which every other exception should subclass so that it is
distinguished from other kind of exceptions not raised explicitly by dsio code.

The rule of thumb is that there should be as many exceptions are there are
errors. Feel free to create a new exception when an existing one doesn't quite
fit the purpose.

All exceptions should be named CamelCase and always use the Error suffix.
"""

import traceback


class DsioError(Exception):
    """All custom dsio exceptions should subclass this one.

    When printed, this class will always print its default message plus
    the message provided during exception initialization, if provided.

    """
    msg = "Dsio Error"
    code = -1

    def __init__(self, msg=None, exc=None):
        if exc is None and isinstance(msg, Exception):
            msg, exc = repr(msg), msg
        self.orig_exc = exc if isinstance(exc, Exception) else None
        self.orig_traceback = traceback.format_exc()
        msg = "%s: %s" % (self.msg, msg) if msg is not None else self.msg
        super(DsioError, self).__init__(msg)


class ModuleLoadError(DsioError):
    msg = "Error loading module"
    code = 1


class DetectorNotFoundError(DsioError):
    msg = "Anomaly detector not found"
    code = 2


class TimefieldNotFoundError(DsioError):
    msg = "Timefield not found in data"
    code = 3


class SensorsNotFoundError(DsioError):
    msg = "Selected sensors not found in data"
    code = 4


class ElasticsearchConnectionError(DsioError):
    msg = "Cannot connect to Elasticsearch"
    code = 5


class KibanaConfigNotFoundError(DsioError):
    msg = "Kibana config index not found in Elasticsearch"
    code = 6
