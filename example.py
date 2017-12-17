import dsio
reload(dsio)
import pandas as pd

df = pd.read_csv('static/data/kddcup_sample.csv')['dst_host_srv_rerror_rate']

z = pd.Series([0.05, 0.1, 0.35])

dd = dsio.anomaly_detectors.Quantile1D()
dd.train(df)
dd.score(z)

ad = dsio.anomaly_detectors.Gaussian1D()
ad.train(df)
ad.score(z)