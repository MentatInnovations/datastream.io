"""
Example of how to embed sklearn objects into our framework by overriding certain methods
Over-riding parts of the sklearn implementation:
https://github.com/scikit-learn/scikit-learn/blob/a24c8b464d094d2c468a16ea9f8bf8d42d949f84/sklearn/neighbors/lof.py#L272
"""

import numpy as np
import pandas as pd

from sklearn.neighbors import LocalOutlierFactor
from dsio.anomaly_detectors import AnomalyMixin
from dsio.update_formulae import rolling_window_update


class LOFAnomalyDetector(LocalOutlierFactor, AnomalyMixin):
    def __init__(
            self,
            n_neighbors=20,
            window_size=1000
    ):
        self.window_size = window_size
        self.sample_ = []
        super(LOFAnomalyDetector, self).__init__(n_neighbors=n_neighbors)

    def fit(self, x, y=None):  # we add None to agree with sklearn conventions
        x = pd.Series(x)
        self.__setattr__('sample_ ', x[:int(np.floor(self.window_size))])
        super(LOFAnomalyDetector, self).fit(x.values.reshape(-1, 1))  # dsio currently only handles 1D data

    def update(self, x): # this simply refits the LOF for now
        x = pd.Series(x)
        window = rolling_window_update(
            old=self.sample_, new=x,
            w=int(np.floor(self.window_size))
        )
        self.__setattr__('sample_', window)
        self.fit(x)

    def score_anomaly(self, x):
        x = pd.Series(x)
        scores = (-self._predict(x.values.reshape(-1, 1))+1)/2.0
        return scores

    def flag_anomaly(self, x):
        return self.score_anomaly(x) == 1.0
