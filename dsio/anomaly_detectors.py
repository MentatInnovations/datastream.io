""" Base anomaly detector class and collection of built-in detectors """

import abc
import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
from scipy.stats import norm
from collections import namedtuple
from dsio.update_formulae import update_effective_sample_size
from dsio.update_formulae import (
    convex_combination,
    rolling_window_update,
    decision_rule
)

from sklearn.base import BaseEstimator


THRESHOLD = 0.99


class AnomalyMixin(object):
    """
    Mixin class for all anomaly detectors,
    compatible with BaseEstimator from scikit-learn.
    """
    _estimator_type = "anomaly"

    def fit_score(self, X):
        """Fits the model on X and scores each datapoint in X.

        Parameters
        ----------
        X : ndarray, shape (n_samples, n_features)
            Input data

        Returns
        -------
        y : ndarray, shape (n_samples, )
            anomaly scores
        """

        self.fit(X)
        return self.score_anomaly(X)

    def update(self, x):
        raise NotImplementedError

    def flag_anomaly(self, x):
        raise NotImplementedError

    def fit(self, x):
        raise NotImplementedError

    def score_anomaly(self, x):
        raise NotImplementedError


def compute_confusion_matrix(detector_output, index_anomalies):

    index_detected = set(np.where(detector_output)[0])
    index_true = set(index_anomalies)
    true_anomalies = index_detected.intersection(index_true)
    false_anomalies = index_detected.difference(index_true)
    return {
        'TPR': len(true_anomalies)/(1.0*len(index_anomalies)),
        'FPR': len(false_anomalies)/(1.0*len(detector_output))
    }


class Gaussian1D(BaseEstimator, AnomalyMixin):
    def __init__(
        self,
        ff=1.0,
        threshold=THRESHOLD
    ):
        self.ff = ff
        self.threshold = threshold
        self.ess_ = 1
        self.mu_ = 0
        self.std_ = 1

    def fit(self, x):
        x = pd.Series(x)
        self.__setattr__('mu_', np.mean(x))
        self.__setattr__('std_', np.std(x, ddof=1))
        self.__setattr__('ess_', len(x))

    def update(self, x):  # allows mini-batch
        try:
            getattr(self, "mu_")
        except AttributeError:
            raise RuntimeError("You must fit the detector before updating it")
        x = pd.Series(x)
        ess, weight = update_effective_sample_size(
            effective_sample_size=self.ess_,
            batch_size=len(x),
            forgetting_factor=self.ff
        )
        self.__setattr__('ess_', ess)
        self.__setattr__(
            'mu_',
            convex_combination(self.mu_, np.mean(x), weight=weight)
        )
        self.__setattr__('std_', np.std(x))

    def score_anomaly(self, x):
        x = pd.Series(x)
        scaled_x = np.abs(x - self.mu_)/(1.0*self.std_)
        return norm.cdf(scaled_x)

    def flag_anomaly(self, x):
        return decision_rule(self.score_anomaly(x), self.threshold)


class Percentile1D(BaseEstimator, AnomalyMixin):

    def __init__(
        self,
        ff=1.0,
        window_size=300,
        threshold=THRESHOLD
    ):
        self.ff = ff
        self.window_size = window_size
        self.threshold = threshold
        self.sample_ = []

    def fit(self, x):
        x = pd.Series(x)
        self.__setattr__('sample_', x[:int(np.floor(self.window_size))])

    def update(self, x):  # allows mini-batch
        x = pd.Series(x)
        window = rolling_window_update(
            old=self.sample_, new=x,
            w=int(np.floor(self.window_size))
        )
        self.__setattr__('sample_', window)

    def score_anomaly(self, x):
        x = pd.Series(x)
        scores = pd.Series([0.01*percentileofscore(self.sample_, z) for z in x])
        return scores

    def flag_anomaly(self, x):
        return decision_rule(self.score_anomaly(x), self.threshold)
