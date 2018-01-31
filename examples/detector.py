import numpy as np
from sklearn.base import BaseEstimator

from dsio.anomaly_detectors import AnomalyMixin


class Greater_Than_Max_Rolling(BaseEstimator, AnomalyMixin):
    def __init__(self, ):
        pass

    def detect(self, x):
        score = self.score(x)
        return np.logical_or(score < 1-self.threshold, score > self.threshold)

    def score(self, x):
        from scipy.stats import percentileofscore
        return [0.01*percentileofscore(x, z) for z in x]

    def train(self, x):
        return

    def update(self, x):
        return

