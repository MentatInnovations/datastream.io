import pandas as pd
import numpy as np
import abc
from dsio.maths import convex_combination


class AnomalyDetector(object):
    'common base class for all anomaly detectors'

    @abc.abstractmethod
    def __init__(self, variable_types=None, model_params=None, tuning_params=None):
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

    @abc.abstractmethod
    def update(self, x):
        raise NotImplementedError
        return

    @abc.abstractmethod
    def train(self, x):
        raise NotImplementedError
        return

    @abc.abstractmethod
    def score(self, x):
        raise NotImplementedError
        return

    @abc.abstractmethod
    def copy(self):
        raise NotImplementedError
        return


class Gaussian1D(AnomalyDetector):

    def __init__(self,
                 variable_types=[np.dtype('float64')], # TODO: we must find a way to validate incoming dtypes
                 model_params={'mu': 0, 'ss': 0},
                 tuning_params={'ff': 0.9}):
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

    def update(self, x): # allows mini-batch
        assert(isinstance(x, pd.Series))
        self.model_params['ss'] = self.tuning_params['ff'] * self.model_params['ss'] + len(x)
        self.model_params['mu'] = convex_combination(
            self.model_params['mu'],
            np.mean(x),
            weight=1.0/self.model_params['ss']
        )

    def train(self, x):
        assert (isinstance(x, pd.Series))
        self.model_params = {'mu': np.mean(x), 'ss': len(x)}

    def score(self, x):
        assert (isinstance(x, pd.Series))
        return np.abs(np.mean(x) - self.model_params['mu'])

#print 'Subclass:', issubclass(UnidimGaussianAD, AnomalyDetector)
#print 'Instance:', isinstance(UnidimGaussianAD(), AnomalyDetector)






