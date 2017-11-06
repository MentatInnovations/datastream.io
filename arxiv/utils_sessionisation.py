import numpy as np
import pandas as pd
import maths as mt

d = 15*1000.0

def compute_no_sessions(x, d = d):
    result = np.sum(np.diff(x) >= d) + 1
    return result

def max1diff(x):
    return( np.max( np.insert(np.diff(x), 0, 0)) )

def compute_sessions(
        x,
        d=d,
        return_labels=True,
        slow_testing=False
):

    assert(type(x) == pd.core.series.Series)  # pd Series
    diffs = np.diff(x)
    assert((diffs >= 0).all())  # all positive
    session_changes = diffs >= d

    no_sessions = compute_no_sessions(x, d = d)
    labels = None
    if return_labels:
        labels = np.cumsum(np.insert(session_changes, 0, 0))
        if slow_testing:
            tmp = pd.DataFrame({'labels':labels, 'times':x}).groupby('labels')['times'].agg(
                lambda x: max1diff(np.array(x))
            )
            assert((tmp <= d).all())

        assert(max(labels) + 1 == no_sessions)

    output = {'no_sessions' : no_sessions, 'labels' : labels}
    return output


x = 1000.0*pd.Series((0, 10, 10, 20, 24, 50, 80, 82)).sort_values()  # generate 3 sessions (delta = 15s)
assert(compute_no_sessions(x) == 3)
assert(compute_sessions(x)['no_sessions'] == 3)



def mad(x):
    return np.median(np.absolute(x - np.median(x)))


# note to have agreement with R we must use mad(x, constant = 1) in R to disable asymptotic normality
assert(mad([1, 1, 2, 3, 5, 8]) == 1.5)


def is_monotonic(y):
    return ((np.diff(y) >= 0).all()) | ((np.diff(y) <= 0).all())

assert(is_monotonic([1, 2, 3, 4, 5]))
assert(is_monotonic([5, 4, 3, 2, 1]))
assert(~is_monotonic([1, 3, 2, 1]))


def compute_elbow(
        y,
        ymin = None,
        ymax = None
):
    diffs = np.diff(y)
    assert(is_monotonic(y))


    if ymin == None:
        ymin = np.min(y)

    if ymax == None:
        ymax = np.max(y)

    if diffs[0] >= 0: # if increasing
        result = np.argmax(diffs > np.median(diffs) + mad(diffs)) + 1
    else:
        result = np.argmin(diffs < np.median(diffs) - mad(diffs)) + 1

    return result

assert(compute_elbow(np.array([1, 2, 3, 4, 5, 6, 20, 22, 24])) == 6)
assert(compute_elbow(np.array([24, 22, 20, 6, 5, 4, 3, 2, 1])) == 4)

