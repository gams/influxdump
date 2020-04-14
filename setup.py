# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from codecs import open
from os import path


here = path.abspath(path.dirname(__file__))

# create long decscription based on README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, "influxdump", "__about__.py")) as f:
    about = {}
    exec(f.read(), about)

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
    name=about["__name__"],
    version=about["__version__"],
    description=about["__summary__"],
    long_description=long_description,
    url=about["__uri__"],
    author=about["__author__"],
    author_email=about["__contact__"],
    license=about["__license__"],
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
    entry_points={
        'console_scripts': [
            "influxdump=influxdump:main",
        ]
    },
)
