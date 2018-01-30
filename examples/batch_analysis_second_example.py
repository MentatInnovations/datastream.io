import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.curdir)))

from dsio.anomaly_detectors import Gaussian1D, Percentile1D, compute_confusion_matrix
from examples.lof_anomaly_detector import LOFEstimator
from dsio.generate_data import gen_data_with_obvious_anomalies

x, index_anomalies = gen_data_with_obvious_anomalies(n=1000, anomalies=20)
detectors = [Gaussian1D, Percentile1D, LOFEstimator]

for detector in detectors:
    d = detector()
    print('\nRunning Detector:\n\t{}'.format(d))
    d.fit(x[:50])

    print('Scoring anomalies:')
    print(list(d.score_anomaly(x)[:10]))

    print('Flagging anomalies:')
    detector_output = d.flag_anomaly(x)
    print(list(detector_output[:10]))

    print('Confusion Matrix:')
    print(compute_confusion_matrix(detector_output, index_anomalies))

    print('>> Updating with some more datapoints')
    d.update(x[101:])

    print('Flagging anomalies:')
    detector_output = d.flag_anomaly(x)
    print(list(detector_output[:10]))

    print('Confusion Matrix:')
    print(compute_confusion_matrix(detector_output, index_anomalies))

    print('-' * 20)
