import dsio
reload(dsio)
import pandas as pd

df = pd.read_csv('static/data/kddcup_sample.csv')

x = df['dst_host_srv_rerror_rate']


dd = dsio.anomaly_detectors.Quantile1D()
dd.train(x)
dd.score(x)

ad = dsio.anomaly_detectors.Gaussian1D()
ad.train(x)
ad.score(x)