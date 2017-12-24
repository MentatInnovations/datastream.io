"""
A utility to generate data from a simple normal distribution with some alerts
"""

import numpy as np
import pandas as pd


def gen_data(n=1000, anomalies=10, sigmas=5.0, filename=None):
    """
    :param n: number of total samples, including anomalies
    :param anomalies: number of anomalies in the sample (should be a small proportion)
    :param sigmas: this describes how many sigmas away from the mean the anomalies should lie
    :param filename: if None then the function returns a pandas series, otherwise it writes to the specified file
    :return:
    """

    x = np.random.normal(0, 1, n)
    index_of_anomalies = np.random.choice(n, size=anomalies, replace=False)

    # we shift by 5 sigmas (or whatever the user specified) in the direction of the datapoint
    # multiplying could end up with a normal value if the original value is small enough
    x[index_of_anomalies] = x[index_of_anomalies] + np.sign(x[index_of_anomalies]) * sigmas
    if filename:
        pd.DataFrame(data=x, columns=['simulated_data']).to_csv(filename, index=False)
        return None
    else:
        return x


gen_data(n=10,anomalies=2,sigmas=10, filename='tst.csv',)