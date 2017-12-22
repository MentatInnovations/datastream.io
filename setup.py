#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

with open('requirements.txt') as req_file:
    requirements = req_file.read()

test_requirements = [
    # TODO: put package test requirements here
]


setup(
    name="dsio",
    version="0.1.0",
    description="Toolkit for anomaly detection on streaming data",
    long_description=readme + '\n\n' + history,
    packages=["dsio"],
    package_dir={'dsio':
                 'dsio'},
    include_package_data=True,
    install_requires=requirements,
    license="Apache 2.0",
    zip_safe=False,
    keywords='dsio',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5'
    ],
    entry_points={
        'console_scripts': ['dsio=dsio.main:main'],
    },
    test_suite='tests',
    tests_require=test_requirements)
