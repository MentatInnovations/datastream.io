"""
A utility to generate data from a simple normal distribution with some alerts
"""

import numpy as np
import pandas as pd


def gen_data_with_obvious_anomalies(
    n=1000,
    anomalies=10,
    sigmas=5.0,
    filename=None
):
    """
    :param n: number of total samples, including anomalies, defaults to 1000
    :param anomalies: number of anomalies in the sample (should be a small proportion), defaults to 10
    :param sigmas: this describes how many sigmas away from the mean the anomalies should lie, defaults to 5
    :param filename: if None (default) then the function returns a pandas object, otherwise it writes to file
    :return: either None or a pandas data frame
    """

    x = np.random.normal(0, 1, n)
    index_of_anomalies = np.random.choice(n, size=anomalies, replace=False)

    # we shift by 5 sigmas (or whatever the user specified) in the direction of the datapoint
    # multiplying could end up with a normal value if the original value is small enough
    x[index_of_anomalies] = (
        x[index_of_anomalies] + np.sign(x[index_of_anomalies]) * sigmas
    )
    if filename:
        pd.DataFrame(
            data=x, columns=['simulated_data']
        ).to_csv(filename, index=False)
        return None
    else:
        return x, index_of_anomalies
