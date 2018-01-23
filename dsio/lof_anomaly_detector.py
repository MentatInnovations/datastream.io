"""
Base anomaly detector class and collection of built-in detectors
Based on the sklearn implementation:
https://github.com/scikit-learn/scikit-learn/blob/a24c8b464d094d2c468a16ea9f8bf8d42d949f84/sklearn/neighbors/lof.py#L272
"""

import numpy as np
from scipy.stats import scoreatpercentile

from dsio.update_formulae import decision_rule
from dsio.anomaly_detectors import AnomalyMixin
from sklearn.base import BaseEstimator
from sklearn.neighbors.base import KNeighborsMixin, NeighborsBase


class LOFEstimator(BaseEstimator, AnomalyMixin, KNeighborsMixin):
    def __init__(
            self, n_neighbors=20, algorithm='auto', leaf_size=30,
            metric='minkowski', p=2, metric_params=None,
            contamination=0.1, n_jobs=1
    ):
        self.n_neighbors = n_neighbors
        self.algorithm = algorithm
        self.leaf_size = leaf_size
        self.metric = metric
        self.p = p
        self.metric_params = metric_params
        self.n_jobs = n_jobs
        self.contamination = contamination

        self._fit_method = 'brute'
        self.effective_metric_ = 'euclidean'

    # from https://github.com/scikit-learn/scikit-learn/blob/a24c8b464d094d2c468a16ea9f8bf8d42d949f84/sklearn/neighbors/lof.py#L156
    def fit(self, X):
        """Fit the model using X as training data.

        Parameters
        ----------
        X : {array-like, sparse matrix, BallTree, KDTree}
            Training data. If array or matrix, shape [n_samples, n_features],
            or [n_samples, n_samples] if metric='precomputed'.

        Returns
        -------
        self : object
            Returns self.
        """
        if not (0. < self.contamination <= .5):
            raise ValueError("contamination must be in (0, 0.5]")

        # @TODO check the following
        # the following line is from sklearn.neighbors.base import NeighborsBase
        self._fit_X = X

        n_samples = self._fit_X.shape[0]
        if self.n_neighbors > n_samples:
            # @TODO change to warning
            raise ("n_neighbors (%s) is greater than the "
                   "total number of samples (%s). n_neighbors "
                   "will be set to (n_samples - 1) for estimation."
                   % (self.n_neighbors, n_samples))
        self.n_neighbors_ = max(1, min(self.n_neighbors, n_samples - 1))

        print(self._fit_X.shape)
        self._distances_fit_X_, self._neighbors_indices_fit_X_ = (
            self.kneighbors(None, n_neighbors=self.n_neighbors_))

        self._lrd = self._local_reachability_density(
            self._distances_fit_X_, self._neighbors_indices_fit_X_)

    def update(self, X):  # allows mini-batch
        # @TODO
        pass

    def score_anomaly(self, X):
        # Compute lof score over training samples to define threshold_:
        lrd_ratios_array = (self._lrd[self._neighbors_indices_fit_X_] /
                            self._lrd[:, np.newaxis])

        self.negative_outlier_factor_ = -np.mean(lrd_ratios_array, axis=1)

        return self.negative_outlier_factor_

    def flag_anomaly(self, X):
        self.score_anomaly(X)
        self.threshold_ = -scoreatpercentile(a = -self.negative_outlier_factor_,
                                             per = (100. * (1. - self.contamination)))

        return decision_rule(self.negative_outlier_factor_, self.threshold_)

    # from https://github.com/scikit-learn/scikit-learn/blob/a24c8b464d094d2c468a16ea9f8bf8d42d949f84/sklearn/neighbors/lof.py#L272
    def _local_reachability_density(self, distances_X, neighbors_indices):
        """The local reachability density (LRD)

        The LRD of a sample is the inverse of the average reachability
        distance of its k-nearest neighbors.

        Parameters
        ----------
        distances_X : array, shape (n_query, self.n_neighbors)
            Distances to the neighbors (in the training samples `self._fit_X`)
            of each query point to compute the LRD.

        neighbors_indices : array, shape (n_query, self.n_neighbors)
            Neighbors indices (of each query point) among training samples
            self._fit_X.

        Returns
        -------
        local_reachability_density : array, shape (n_samples,)
            The local reachability density of each sample.
        """
        dist_k = self._distances_fit_X_[neighbors_indices,
                                        self.n_neighbors_ - 1]
        reach_dist_array = np.maximum(distances_X, dist_k)

        #  1e-10 to avoid `nan' when nb of duplicates > n_neighbors_:
        return 1. / (np.mean(reach_dist_array, axis=1) + 1e-10)
