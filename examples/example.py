import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.curdir)))

from dsio.anomaly_detectors import Gaussian1D, Percentile1D, compute_confusion_matrix
from dsio.lof_anomaly_detector import LOFEstimator
from dsio.generate_data import gen_data_with_obvious_anomalies

x, index_anomalies = gen_data_with_obvious_anomalies(n=1000, anomalies=50)
#
# detector1 = Gaussian1D()
# print(detector1.__class__)
# detector1.fit(x[:50])
# detector_output1 = detector1.flag_anomaly(x)
# print(compute_confusion_matrix(detector_output1, index_anomalies))
# detector1.update(x[101:])
# detector_output1 = detector1.flag_anomaly(x)
# print(compute_confusion_matrix(detector_output1, index_anomalies))
#
# detector2 = Percentile1D()
# print(detector2.__class__)
# detector2.fit(x[:50])
# detector_output2 = detector2.flag_anomaly(x)
# print(compute_confusion_matrix(detector_output2, index_anomalies))
# detector2.update(x[101:])
# detector_output2 = detector2.flag_anomaly(x)
# print(compute_confusion_matrix(detector_output2, index_anomalies))
#

detector3 = LOFEstimator()
print(detector3.__class__)
detector3.fit(x[:50])
detector_output3 = detector3.flag_anomaly(x)
print(compute_confusion_matrix(detector_output3, index_anomalies))
detector3.update(x[101:])
detector_output3 = detector3.flag_anomaly(x)
print(compute_confusion_matrix(detector_output3, index_anomalies))