# Mirror setup in https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/juliaset/setup.py
# https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/

import setuptools

REQUIRED_PACKAGES = [
  'absl-py==0.9.0',
  'apache-beam[gcp]==2.23.0',
  'google-cloud-bigquery==1.26.0',
  'google-cloud-pubsub==1.7.0',
  'google-cloud-storage==1.29.0',
  'pandas==1.0.5',
]

setuptools.setup(
    name='Raxxla',
    version='1.0',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_namespace_packages(),
)
