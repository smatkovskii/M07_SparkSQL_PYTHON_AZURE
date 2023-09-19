#!/usr/bin/env python
from setuptools import setup

setup(
    name='sparksql',
    version='0.1.0',
    description='BDCC Pyspark SQL project',
    packages=['sparksql'],
    package_dir={'sparksql': 'src/sparksql'},
    zip_safe=False
)
