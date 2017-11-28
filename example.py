import dsio
reload(dsio)
import pandas as pd

df = pd.read_csv('static/data/kddcup_sample.csv')

x = df[['dst_host_srv_rerror_rate', 'dst_host_same_src_port_rate']]


ad = dsio.anomaly_detectors.Gaussian1D()