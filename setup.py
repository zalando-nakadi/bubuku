#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import inspect
import os
import sys

import setuptools
from setuptools import setup
from setuptools.command.test import test
from distutils.core import Command

if sys.version_info < (3, 4, 0):
    sys.stderr.write('FATAL: Bubuku needs to be run with Python 3.4+\n')
    sys.exit(1)

__location__ = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))


def read_version(package):
    with open(os.path.join(package, '__init__.py'), 'r') as fd:
        for line in fd:
            if line.startswith('__version__ = '):
                return line.split()[-1].strip().strip("'")


NAME = 'bubuku'
MAIN_PACKAGE = 'bubuku'
VERSION = read_version(MAIN_PACKAGE)
DESCRIPTION = 'AWS support for kafka broker'
LICENSE = 'Apache License 2.0'
URL = 'https://github.com/zalando-incubator/bubuku'
AUTHOR = 'Dmitry Sorokin'
EMAIL = 'dmitriy.sorokin@zalando.de'
KEYWORDS = 'aws kafka supervisor'

# Add here all kinds of additional classifiers as defined under
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: Implementation :: CPython',
]

CONSOLE_SCRIPTS = [
    'bubuku-daemon = bubuku.daemon:main',
    'bubuku-cli = bubuku.cli:cli'
]


class DockerUpCommand(Command):
    description = "Start up docker compose with 3 bubuku and 1 zookeeper instances"
    user_options = [
        ('bubuku-scale=', None, 'Specify number of bubuku instances')
    ]

    def initialize_options(self):
        self.bubuku_scale = 3

    def finalize_options(self):
        pass

    def run(self):
        os.system('docker-compose up -d --build && docker-compose scale bubuku=' + str(self.bubuku_scale))


class DockerDownCommand(Command):
    description = "Stop docker compose"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        os.system('docker-compose down')


class PyTest(test):
    def run_tests(self):
        try:
            import pytest
        except:
            raise RuntimeError('py.test is not installed, run: pip install pytest')
        params = {'args': self.test_args}
        errno = pytest.main(**params)
        sys.exit(errno)


def read(fname):
    with open(os.path.join(__location__, fname)) as f:
        return f.read()


def setup_package():
    command_options = {'test': {'test_suite': ('setup.py', 'tests')}}

    setup(
        name=NAME,
        version=VERSION,
        url=URL,
        description=DESCRIPTION,
        author=AUTHOR,
        author_email=EMAIL,
        license=LICENSE,
        keywords=KEYWORDS,
        classifiers=CLASSIFIERS,
        test_suite='tests',
        packages=setuptools.find_packages(exclude=['tests', 'tests.*']),
        install_requires=[req for req in read('requirements.txt').split('\\n') if req != ''],
        cmdclass={'test': PyTest, 'docker_up': DockerUpCommand, 'docker_down': DockerDownCommand},
        tests_require=['pytest-cov', 'pytest'],
        command_options=command_options,
        entry_points={
            'console_scripts': CONSOLE_SCRIPTS,
        },
    )

if __name__ == '__main__':
    setup_package()
