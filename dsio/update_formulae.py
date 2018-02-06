"""
This is the "update_formulae" module.

It performs a number of weighted updates that are needed for streaming learning, e.g.,

>>> convex_combination(10,20,0.3)
13.0

It also hosts certain decision formulae.
"""

import numpy as np


def convex_combination(a, b, weight):
    """

    :param a: one summand (e.g., partial sum)
    :param b: another summand (e.g., the new datapoint)
    :param weight: the weight of the update (e.g., the inverse of the sample size)
    :return: the updated value (e.g., the new partial sum)

    >>> assert(np.mean([3,7]) == convex_combination(3,7,0.5))
    >>> assert(np.mean([1,1,1,2]) == convex_combination(1,2,0.25))

    """
    return (1-weight) * a + weight * b


def update_effective_sample_size(
    effective_sample_size,
    batch_size,
    forgetting_factor
):
    """

    :param effective_sample_size:
    :param batch_size:
    :param forgetting_factor:
    :return:

    >>> update_effective_sample_size(1.0,1.0,1.0)
    (2.0, 1.0)

    """
    updated_sample_size = (
        effective_sample_size * forgetting_factor + batch_size
    )
    weight = 1 - (
        (effective_sample_size*1.0 - batch_size) /
        (effective_sample_size*1.0)
    )
    return updated_sample_size, weight


def rolling_window_update(old, new, w=100):
    """

    :param old: Old data
    :param new: New data
    :param w: Controls the size of the rolling window
    :return: The w most recent datapoints from the concatenation of old and new

    >>> rolling_window_update(old=[1,2,3], new=[4,5,6,7],w=5)
    array([3,4,5,6,7])

    """
    out = np.concatenate((old, new))
    if len(out) > w:
        out = out[(len(out)-w):]
    return out


def decision_rule(score, threshold=0.99, two_sided=True):
    """

    :param score: a score, assumed normalised (between 0 and 1) representing anomalousness
    :param threshold: a user-specified threshold above which an alert should be raised
    :param two_sided: if True, we flag anomalies that are either smaller than 1-threshold or larger than threhsold
    :return: a boolean flag

    >>> decision_rule(score=0.9)
    False
    >>> decision_rule(score=0.95, threshold=0.9)
    True
    >>> decision_rule(score=0.0001, threshold=0.99)
    True
    >>> decision_rule(score=0.001, two_sided=False)
    False
    """
    if two_sided:
        ans = np.logical_or(score < 1-threshold, score > threshold)
    else:
        ans = score > threshold
    return ans
