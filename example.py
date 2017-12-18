import dsio
import pandas as pd


df = pd.read_csv('static/data/kddcup_sample.csv')['dst_host_srv_rerror_rate']

z = pd.Series([0.01, 0.3, 0.5])
dd = dsio.anomaly_detectors.Quantile1D()
dd.train(df)
dd.update(z)
dd.score(z)

ad = dsio.anomaly_detectors.Gaussian1D()
ad.train(df)
ad.update(z)
ad.score(z)