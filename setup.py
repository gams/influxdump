# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from codecs import open
from os import path


here = path.abspath(path.dirname(__file__))

# create long decscription based on README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'LICENSE'), encoding='utf-8') as f:
    license = f.read()

# list requirements for setuptools
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requirements = []
    for name in f.readlines():
        if not name.startswith('--'):
            requirements.append(name.rstrip())

# list requirements for setuptools
with open(path.join(here, 'requirements-dev.txt'), encoding='utf-8') as f:
    requirements_dev = [name.rstrip() for name in f.readlines()]

setup(
    name='influxdump',
    version="1.0.1",
    description='InfluxDB data backup tool',
    long_description=long_description,
    url='https://github.com/gams/influxdump',
    author="gams",
    author_email="code@measureofquality.com",
    license=license,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
    ],
    keywords='influxdb',
    packages=find_packages(exclude=['contrib', 'docs', 'test*']),
    install_requires=requirements,
    extra_require={
        'dev': requirements_dev,
    },
)
