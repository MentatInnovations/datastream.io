"""
Base anomaly detector class and collection of built-in detectors
Based on the sklearn implementation:
https://github.com/scikit-learn/scikit-learn/blob/a24c8b464d094d2c468a16ea9f8bf8d42d949f84/sklearn/neighbors/lof.py#L272
"""

import numpy as np
import pandas as pd

from scipy.stats import scoreatpercentile
from warnings import warn

from dsio.update_formulae import decision_rule
from dsio.anomaly_detectors import AnomalyMixin
from sklearn.neighbors.base import KNeighborsMixin, NeighborsBase
from sklearn.utils.validation import check_is_fitted
from sklearn.utils import check_array

__all__ = ["LOFEstimator"]


class LOFEstimator(NeighborsBase, AnomalyMixin, KNeighborsMixin):
    def __init__(self, n_neighbors=20, algorithm='auto', leaf_size=30,
                 metric='minkowski', p=2, metric_params=None,
                 contamination=0.1, n_jobs=1):
        self._init_params(n_neighbors=n_neighbors,
                          algorithm=algorithm,
                          leaf_size=leaf_size, metric=metric, p=p,
                          metric_params=metric_params, n_jobs=n_jobs)

        self.contamination = contamination

    def fit(self, X):
        """
        In the fitting stage we're computing the neighbors matrix

        Fit the model using X as training data.

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

        # reshape the array if it only contains one feature but multiple samples
        # ex. n_samples=100:  (100,)  --> (100,1)
        if len(X.shape) == 1:
            X = X.reshape(-1, 1)

        # @TODO
        # removed the following line from sklearn implementation
        # super(LOFEstimator, self).fit(X)
        super(LOFEstimator, self)._fit(X)

        n_samples = self._fit_X.shape[0]
        if self.n_neighbors > n_samples:
            warn("n_neighbors (%s) is greater than the "
                 "total number of samples (%s). n_neighbors "
                 "will be set to (n_samples - 1) for estimation."
                 % (self.n_neighbors, n_samples))
        self.n_neighbors_ = max(1, min(self.n_neighbors, n_samples - 1))

        self._distances_fit_X_, _neighbors_indices_fit_X_ = (
            self.kneighbors(None, n_neighbors=self.n_neighbors_))

        self._lrd = self._local_reachability_density(
            self._distances_fit_X_, _neighbors_indices_fit_X_)

        # Compute lof score over training samples to define threshold_:
        lrd_ratios_array = (self._lrd[_neighbors_indices_fit_X_] /
                            self._lrd[:, np.newaxis])

        self.negative_outlier_factor_ = -np.mean(lrd_ratios_array, axis=1)

        self.threshold_ = -scoreatpercentile(
            -self.negative_outlier_factor_, 100. * (1. - self.contamination))

        return self

    # def update(self, x):  # allows mini-batch
    #     x = pd.Series(x)
    #     window = rolling_window_update(
    #         old=self.sample_, new=x,
    #         w=int(np.floor(self.window_size))
    #     )
    #     self.sample_ = window
    #

    def score_anomaly(self, X):
        # map results into [0,1]
        # The higher, the more abnormal
        # @TODO not sure if this is needed, depends on the definition of the function
        # @TODO there should be a numpy function, but none seemed to work as expected
        scores = -self._decision_function(X)
        return pd.Series(1 + (scores - np.max(scores)) / np.ptp(scores))

    def flag_anomaly(self, X):
        return pd.Series(decision_rule(-self._predict(X), 0, False))

    def _predict(self, X=None):
        """Predict the labels (1 inlier, -1 outlier) of X according to LOF.

        If X is None, returns the same as fit_predict(X_train).
        This method allows to generalize prediction to new observations (not
        in the training set). As LOF originally does not deal with new data,
        this method is kept private.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features), default=None
            The query sample or samples to compute the Local Outlier Factor
            w.r.t. to the training samples. If None, makes prediction on the
            training data without considering them as their own neighbors.

        Returns
        -------
        is_inlier : array, shape (n_samples,)
            Returns -1 for anomalies/outliers and +1 for inliers.
        """
        check_is_fitted(self, ["threshold_", "negative_outlier_factor_",
                               "n_neighbors_", "_distances_fit_X_"])

        if X is not None:
            # reshape the array if it only contains one feature but multiple samples
            # ex. n_samples=100:  (100,)  --> (100,1)
            if len(X.shape) == 1:
                X = X.reshape(-1, 1)
            X = check_array(X, accept_sparse='csr')
            is_inlier = np.ones(X.shape[0], dtype=int)
            is_inlier[self._decision_function(X) <= self.threshold_] = -1
        else:
            is_inlier = np.ones(self._fit_X.shape[0], dtype=int)
            is_inlier[self.negative_outlier_factor_ <= self.threshold_] = -1

        return is_inlier

    def _decision_function(self, X):
        """Opposite of the Local Outlier Factor of X (as bigger is better,
        i.e. large values correspond to inliers).

        The argument X is supposed to contain *new data*: if X contains a
        point from training, it consider the later in its own neighborhood.
        Also, the samples in X are not considered in the neighborhood of any
        point.
        The decision function on training data is available by considering the
        opposite of the negative_outlier_factor_ attribute.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            The query sample or samples to compute the Local Outlier Factor
            w.r.t. the training samples.

        Returns
        -------
        opposite_lof_scores : array, shape (n_samples,)
            The opposite of the Local Outlier Factor of each input samples.
            The lower, the more abnormal.
        """
        check_is_fitted(self, ["threshold_", "negative_outlier_factor_",
                               "_distances_fit_X_"])

        # reshape the array if it only contains one feature but multiple samples
        # ex. n_samples=100:  (100,)  --> (100,1)
        if len(X.shape) == 1:
            X = X.reshape(-1, 1)
        X = check_array(X, accept_sparse='csr')

        distances_X, neighbors_indices_X = (
            self.kneighbors(X, n_neighbors=self.n_neighbors_))
        X_lrd = self._local_reachability_density(distances_X,
                                                 neighbors_indices_X)

        lrd_ratios_array = (self._lrd[neighbors_indices_X] /
                            X_lrd[:, np.newaxis])

        # as bigger is better:
        return -np.mean(lrd_ratios_array, axis=1)

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
