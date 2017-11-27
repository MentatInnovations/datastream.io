import pandas as pd
import numpy as np
import abc



class AnomalyDetector(object):
    'common base class for all anomaly detectors'

    @abc.abstractmethod
    def __init__(self, variable_names=None, variable_types=None, theta=None):
        self.variable_names = variable_names
        self.variable_types = variable_types
        self.theta = None

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


class GaussianAnomalyDetector(AnomalyDetector):

    def __init__(self, variable_names=None, variable_types=None, theta=None):
        self.variable_names = variable_names
        self.variable_types = variable_types
        self.theta = None

    def update(self, x):
        assert(isinstance(x, pd.Series))
        AnomalyDetector.theta['mu'] = convex_combination(
            AnomalyDetector.theta['mu'],
            np.mean(x),
            weight=1.0/AnomalyDetector.theta['ss']
        )

    def train(self, x):
        assert (isinstance(x, pd.Series))
        AnomalyDetector.theta = {'mu':np.mean(x), 'ss':len(x)}

    def score(self, x):
        assert (isinstance(x, pd.Series))
        return np.abs(np.mean(x) - AnomalyDetector.theta['mu'])

print 'Subclass:', issubclass(GaussianAnomalyDetector, AnomalyDetector)
print 'Instance:', isinstance(GaussianAnomalyDetector(), AnomalyDetector)





x = pd.Series([1,2,1,2])
xmore = pd.Series(3)
xtest = pd.Series(5)
ad1 = GaussianAnomalyDetector()
ad1.train(x)
ad1.update(xmore)
ad1.score(xtest)