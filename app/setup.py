# Mirror setup in https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/juliaset/setup.py
# https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/

import setuptools

REQUIRED_PACKAGES = ['absl-py==0.9.0']

setuptools.setup(
    name='Raxxla',
    version='1.0',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_namespace_packages(),
)
