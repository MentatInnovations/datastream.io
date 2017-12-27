from dsio.anomaly_detectors import Gaussian1D, Percentile1D
from dsio.generate_data import gen_data_with_obvious_anomalies

x = gen_data_with_obvious_anomalies(n=1000, anomalies=10)

detector1 = Gaussian1D()
detector1.train(x[:100])
detector1.update(x[101:200])
detector1.score(x)

detector2 = Percentile1D()
detector2.train(x[:100])
detector2.update(x[101:200])
detector2.score(x)