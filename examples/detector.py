import numpy as np

from dsio.anomaly_detectors import AnomalyDetector


class Greater_Than_Max_Rolling(AnomalyDetector):
    def __init__(self, ):

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

