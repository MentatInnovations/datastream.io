import pandas as pd
import abc
from scipy.stats import percentileofscore
from dsio.update_formulae import *

class AnomalyDetector(object):
    'common base class for all anomaly detectors'

    @abc.abstractmethod
    def __init__(self, variable_types=None, model_params=None, tuning_params=None):
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

    @abc.abstractmethod
    def update(self, x, weight):
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

    @abc.abstractmethod
    def serialise(self):
        raise NotImplementedError
        return






class Gaussian1D(AnomalyDetector):

    def __init__(self,
                 variable_types=[np.dtype('float64')], # TODO: we must find a way to validate incoming dtypes
                 model_params={'mu': 0, 'ess': 0},
                 tuning_params={'ff': 0.9}):
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

    def update(self, x): # allows mini-batch
        assert(isinstance(x, pd.Series))
        self.model_params['ess'], weight = update_effective_sample_size(
            effective_sample_size=self.model_params['ess'],
            batch_size=len(x),
            forgetting_factor=self.tuning_params['ff']
        )
        self.tuning_params['ff'] * self.model_params['ess'] + len(x)
        self.model_params['mu'] = convex_combination(
            self.model_params['mu'],
            np.mean(x),
            weight=1-(self.model_params['ess']-len(x))/self.model_params['ess']
        )

    def train(self, x):
        assert (isinstance(x, pd.Series))
        assert ([x.dtypes] == self.variable_types)
        self.model_params = {'mu': np.mean(x), 'ess': len(x)}

    def score(self, x):
        assert (isinstance(x, pd.Series))
        return np.abs(x - self.model_params['mu'])

#print 'Subclass:', issubclass(UnidimGaussianAD, AnomalyDetector)
#print 'Instance:', isinstance(UnidimGaussianAD(), AnomalyDetector)


class Quantile1D(AnomalyDetector):

    def __init__(self,
                 variable_types=[np.dtype('float64')], # TODO: we must find a way to validate incoming dtypes
                 model_params={'sample': [0]},
                 tuning_params={'ff': 1.0, 'w':100} # TODO: not supported yet on quantiles
                 ):
        self.variable_types = variable_types
        self.model_params = model_params
        self.tuning_params = tuning_params

    def update(self, x): # allows mini-batch
        assert(isinstance(x, pd.Series))
        self.model_params['sample'] = rolling_window_update(
            old=self.model_params['sample'], new=x,
            w=self.tuning_params['w']
        )

    def train(self, x):
        assert (isinstance(x, pd.Series))
        assert ([x.dtypes] == self.variable_types)
        self.model_params['sample'] = x[:self.tuning_params['w']]

    def score(self, x):
        assert (isinstance(x, pd.Series))
        scores = [0.01*percentileofscore(self.model_params['sample'], z) for z in x]
        return scores

