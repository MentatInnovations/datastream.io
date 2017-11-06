
class AnomalyDetector:
    'common base class for all anomaly detectors'
    def __init__(self, variable_name, variable_type, mu_init = 0):
        self.variable_name = variable_name
        self.variable_type = variable_type
        self.mu = mu_init

    def train(self, x):
        assert(isinstance(x, pd.Series))
        AnomalyDetector.mu = np.mean(x)

    def score(self, x):
        return np.abs(np.mean(x) - AnomalyDetector.mu)

ad1 = AnomalyDetector('gif_present','boolean')
ad1.train(xtrain)
ad1.score(xnew)

