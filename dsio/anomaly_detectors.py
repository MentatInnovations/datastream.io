""" Base anomaly detector class and collection of built-in detectors """

import abc
import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
from scipy.stats import norm
from collections import namedtuple
from dsio.update_formulae import update_effective_sample_size
from dsio.update_formulae import convex_combination, rolling_window_update, decision_rule


def compute_confusion_matrix(detector_output, index_anomalies):

    index_detected = set(np.where(detector_output)[0])
    index_true = set(index_anomalies)
    true_anomalies = index_detected.intersection(index_true)
    false_anomalies = index_detected.difference(index_true)
    return {
        'TPR': len(true_anomalies)/(1.0*len(index_anomalies)),
        'FPR': len(false_anomalies)/(1.0*len(detector_output))}


class AnomalyDetector(object):
    """
    common base class for all anomaly detectors'
    """

    @abc.abstractmethod
    def update(self, x):
        raise NotImplementedError

    @abc.abstractmethod
    def detect(self, x):
        raise NotImplementedError

    @abc.abstractmethod
    def train(self, x):
        raise NotImplementedError

    @abc.abstractmethod
    def score(self, x):
        raise NotImplementedError

    #@abc.abstractmethod
    #def copy(self):
    #    raise NotImplementedError

    #@abc.abstractmethod
    #def serialise(self):
    #    raise NotImplementedError


class Gaussian1D(AnomalyDetector):
    def __init__(
            self,
            variable_types={np.dtype('float64'), np.dtype('int64'), np.dtype('int8')},
            model_params=namedtuple('model_params', 'mu std ess')(0, 1.0, 0),
            tuning_params=namedtuple('tuning_params', ['ff'])(0.9),
            threshold=0.99
    ):

        # TODO: we must find a way to validate incoming dtypes
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params
        self.threshold = threshold

    def update(self, x): # allows mini-batch
        x = pd.Series(x)
        ess, weight = update_effective_sample_size(
            effective_sample_size=self.model_params.ess,
            batch_size=len(x),
            forgetting_factor=self.tuning_params.ff
        )
        mu = convex_combination(
            self.model_params.mu,
            np.mean(x),
            weight=weight
        )
        std = np.std(x)
        # update model state at the end
        self.model_params = namedtuple('model_params', 'mu std ess')(mu, std, ess)

    def train(self, x):
        x = pd.Series(x)
        assert x.dtypes in self.variable_types
        mu = np.mean(x)
        std = np.std(x, ddof=1)
        ess = len(x)
        self.model_params = namedtuple('model_params', 'mu std ess')(mu, std, ess)

    def score(self, x):
        x = pd.Series(x)
        scaled_x = np.abs(x - self.model_params.mu)/(1.0*self.model_params.std)
        return norm.cdf(scaled_x)

    def detect(self, x):
        return decision_rule(self.score(x), self.threshold)


class Percentile1D(AnomalyDetector):

    def __init__(
            self,
            variable_types={np.dtype('float64'), np.dtype('int64'), np.dtype('int8')},
            model_params=namedtuple('model_params', 'sample')([0]),
            tuning_params=namedtuple('tuning_params', ['ff', 'w'])(1.0, 300.0),
            threshold=0.99
    ):

        # TODO: we must find a better way to validate incoming dtypes
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params
        self.threshold = threshold

    def update(self, x): # allows mini-batch
        x = pd.Series(x)
        window = rolling_window_update(
            old=self.model_params.sample, new=x,
            w=int(np.floor(self.tuning_params.w))
        )
        self.model_params = namedtuple('model_params', 'sample')(window)

    def train(self, x):
        x = pd.Series(x)
        assert x.dtypes in self.variable_types
        self.model_params = namedtuple('model_params', ['sample'])(x[:int(np.floor(self.tuning_params.w))])

    def score(self, x):
        x = pd.Series(x)
        scores = pd.Series([0.01*percentileofscore(self.model_params.sample, z) for z in x])
        return scores

    def detect(self, x):
        return decision_rule(self.score(x), self.threshold)
