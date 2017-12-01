""" Helper functions """

import dateparser


def detect_time(dataframe):
    """ Attempt to detect the time dimension in a dataframe """
    columns = set(dataframe.columns)
    timefield = unix = None
    for tfname in ['time', 'datetime', 'date', 'timestamp']:
        if tfname in columns:
            prev = current = None
            unix = True # Assume unix timestamp format unless proven otherwise
            for i in dataframe[tfname][:10]: # FIXME this seems arbitrary
                try:
                    current = dateparser.parse(str(i))
                    # timefield needs to be parsable and always increasing
                    if not current or (prev and prev > current):
                        tfname = ''
                        break
                    if unix and not (isinstance(i, float) or
                                     isinstance(i, int)):
                        unix = False
                except TypeError:
                    tfname = ''
                    break
            prev = current
            if tfname:
                timefield = tfname
                if isinstance(i, float) or isinstance(i, int):
                    unix = True
                break

    return timefield, unix
