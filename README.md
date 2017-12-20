# datastream.io
An open-source framework for real-time anomaly detection using Python, ElasticSearch and Kibana.

## Installation
The recommended installation method is to use pip within a Python 3.x virtalenv.

  virtualenv --python=python3 dsio-env
  source dsio-env/bin/activate
  pip install -e git+https://github.com/MentatInnovations/datastream.io#egg=dsio

## Usage

### Elasticsearch & Kibana

You need to have access to running Elasticsearch and Kibana 5.x instances in order to use dsio. If you don't have them already, you can easily start them up in your machine using the docker-compose.yaml file within the examples directory. Docker and docker-compose need to be installed for this to work.

  cd dsio-env/src/dsio/examples
  docker-compose up -d

Check that Elasticsearch and Kibana are up.

  docker-compose ps

Once you're done working with dsio you can bring them down.

  docker-compose down

Keep in mind that docker-compose commands need to be run in the directory where the docker-compose.yaml file resides (e.g. dsio-env/src/dsio/examples)

### Examples

You can use the example csv datasets or to provide your own. If the dataset includes a time dimension dsio will attempt to detect it automatically. Alternatively, you can --timefield argument to manually configure the field that designates the time dimension. If no such field exists, dsio will assume the data is a time series starting from now with 1sec intervals between samples.

  dsio data/cardata_sample.csv

The above command will load the cardata sample csv and will use the default Quantile1D anomaly detector to apply scores for each numeric column. Then it will generate an appropriate Kibana dashboard and will restream the data to Elasticsearch. A browser window should open that will point to the generated Kibana dashboard. Elasticsearch and Kibana are assumed to be running in the default location, http://localhost:9200/ and http://localhost:5601/app/kibana respectively. You can customize these locations using the --es-uri and --kibana-uri arguments.

You can experiment with different datasets and anomaly detectors. E.g.

  dsio --detector gaussian1d data/kddup_sample.csv

### Defining your own anomaly detectors

You can use dsio with your own hand coded anomaly detectors. These should inherit from the AnomalyDetector abstract base class and implement at least the train, update & score methods. You can find an example 99th percentile anomaly detector in the examples dir. Load the python modules that contain your detectors using the --modules argument and select the target detector by providing its class name to the --detector argument (case insensitive).

  dsio  --modules detector.py --detector percentile data/cardata_sample.csv

