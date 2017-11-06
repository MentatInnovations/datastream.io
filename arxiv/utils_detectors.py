import numpy as np
import pandas as pd


def anomaly_detector_train(x, detector_parameters):

    assert(isinstance(x, pd.DataFrame))
    assert (set(x.columns) == set(detector_parameters.keys()))

    ## initialise output dict
    #colid = x.columns[0]

    for colid in x.columns:
        colnow = x[colid]
        if detector_parameters[colid]['type'] == 'poisson':
            detector_parameters[colid]['mu'] = \
                np.mean(colnow / detector_parameters[colid]['scale'])
        if detector_parameters[colid]['type'] == 'binomial':
            detector_parameters[colid]['mu'] = np.mean(colnow)
    return detector_parameters


def anomaly_detector_test_single(xnew, detector_parameters):

    assert(isinstance(xnew, pd.DataFrame))
    assert(set(xnew.columns) == set(detector_parameters.keys()))
    assert(xnew.shape[0] == 1) # this interface only works with single rows

    scores = dict.fromkeys(xnew.columns)

    colid = xnew.columns[0]
    for colid in xnew.columns:
        colnow = float(xnew[colid])
        if detector_parameters[colid]['type'] == 'poisson':
            scores[colid] = colnow / detector_parameters[colid]['scale'] - detector_parameters[colid]['mu']
        if detector_parameters[colid]['type'] == 'binomial':
            scores[colid] = colnow - detector_parameters[colid]['mu']

    return np.max(scores.values())

