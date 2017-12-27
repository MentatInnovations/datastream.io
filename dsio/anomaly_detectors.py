""" Base anomaly detector class and collection of built-in detectors """

import abc

import pandas as pd
import numpy as np

from scipy.stats import percentileofscore

from collections import namedtuple

from dsio.update_formulae import update_effective_sample_size
from dsio.update_formulae import convex_combination, rolling_window_update, decision_rule


class AnomalyDetector(object):
    """
    common base class for all anomaly detectors'
    """

    @abc.abstractmethod
    def __init__(self, variable_types=None, model_params=None, tuning_params=None, threshold=0.99):
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params
        self.threshold = threshold

    @abc.abstractmethod
    def update(self, x):
        raise NotImplementedError

    @abc.abstractmethod
    def detect(self, x):
        return decision_rule(self.score, self.threshold)

    @abc.abstractmethod
    def train(self, x):
        raise NotImplementedError

    @abc.abstractmethod
    def score(self, x):
        raise NotImplementedError

    @abc.abstractmethod
    def copy(self):
        raise NotImplementedError

    @abc.abstractmethod
    def serialise(self):
        raise NotImplementedError


class Gaussian1D(AnomalyDetector):
    def __init__(
            self,
            variable_types={np.dtype('float64'), np.dtype('int64'), np.dtype('int8')},
            model_params=namedtuple('model_params', 'mu ess')(0, 0),
            tuning_params=namedtuple('tuning_params', ['ff'])(0.9)):

        # TODO: we must find a way to validate incoming dtypes
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

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
        # update model state at the end
        self.model_params = namedtuple('model_params', 'mu ess')(mu, ess)

    def train(self, x):
        x = pd.Series(x)
        assert x.dtypes in self.variable_types
        mu = np.mean(x)
        ess = len(x)
        self.model_params = namedtuple('model_params', 'mu ess')(mu, ess)

    def score(self, x):
        x = pd.Series(x)
        return np.abs(x - self.model_params.mu)


class Percentile1D(AnomalyDetector):

    def __init__(
            self,
            variable_types={np.dtype('float64'), np.dtype('int64'), np.dtype('int8')},
            model_params=namedtuple('model_params', ['sample'])([0]),
            tuning_params=namedtuple('tuning_params', ['ff', 'w'])(1.0, 100.0)):

        # TODO: we must find a better way to validate incoming dtypes
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

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
        scores = [0.01*percentileofscore(self.model_params.sample, z) for z in x]
        return scores

