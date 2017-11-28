""" Helper functions """

import dateparser


def detect_time(dataframe, timefield=None, timeunit=None):
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
