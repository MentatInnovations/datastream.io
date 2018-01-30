from dsio.anomaly_detectors import Gaussian1D, Percentile1D, compute_confusion_matrix
from dsio.generate_data import gen_data_with_obvious_anomalies
from examples.lof_anomaly_detector import LOFAnomalyDetector

x, index_anomalies = gen_data_with_obvious_anomalies(n=1000, anomalies=50)



detector1 = Gaussian1D()
detector1.fit(x[:50])
detector_output1 = detector1.flag_anomaly(x)
print(compute_confusion_matrix(detector_output1, index_anomalies))
detector1.update(x[101:])
detector_output1 = detector1.flag_anomaly(x)
print(compute_confusion_matrix(detector_output1, index_anomalies))

detector2 = Percentile1D()
detector2.fit(x[:50])
detector_output2 = detector2.flag_anomaly(x)
print(compute_confusion_matrix(detector_output2, index_anomalies))
detector2.update(x[101:])
detector_output2 = detector2.flag_anomaly(x)
print(compute_confusion_matrix(detector_output2, index_anomalies))


detector3 = LOFAnomalyDetector()
detector3.fit(x[:50])
detector_output3 = detector3.flag_anomaly(x)
print(compute_confusion_matrix(detector_output3, index_anomalies))
detector3.fit(x[101:])
detector_output3 = detector3.flag_anomaly(x)
print(compute_confusion_matrix(detector_output3, index_anomalies))
