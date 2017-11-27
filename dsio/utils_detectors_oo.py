import pandas as pd
import numpy as np
import abc
from dsio.maths import convex_combination


class AnomalyDetector(object):
    'common base class for all anomaly detectors'

    @abc.abstractmethod
    def __init__(self, variable_names=None, variable_types=None, model_params=None, tuning_params=None):
        self.variable_names = variable_names
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

    @abc.abstractmethod
    def update(self, x):
        assert(isinstance(x, pd.Series))
        return

    @abc.abstractmethod
    def train(self, x):
        assert (isinstance(x, pd.Series))
        return

    @abc.abstractmethod
    def score(self, x):
        assert (isinstance(x, pd.Series))
        return

    @abc.abstractmethod
    def copy(self):
        return


class GaussianAnomalyDetector(AnomalyDetector):

    def __init__(self,
                 variable_names=None,
                 variable_types=None,
                 model_params={'mu': 0, 'ss': 0},
                 tuning_params={'ff': 0.9}):
        self.variable_names = variable_names
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

    def update(self, x):
        assert(isinstance(x, pd.Series))
        self.model_params['ss'] = self.tuning_params['ff'] * self.model_params['ss'] + 1
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

print 'Subclass:', issubclass(GaussianAnomalyDetector, AnomalyDetector)
print 'Instance:', isinstance(GaussianAnomalyDetector(), AnomalyDetector)





x = pd.Series([1,2,1,2])
xmore = pd.Series(3)
xtest = pd.Series(5)
ad1 = GaussianAnomalyDetector()
ad1.train(x)
ad1.update(xmore)
print(ad1.score(xtest))