from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import sys


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name="zeroflo",
    version="0.1",
    packages=find_packages(),
    scripts=[],
    install_requires=[
        'docutils>=0.3',
        'aiozmq>=0.5',
        'pyadds>=0.1',
        ],
    package_data={
        '': ['*.txt', '*.rst'],
        },
    test_module="tests",
    cmdclass={'test': PyTest},
    tests_require=[
        'pytest>=1.8',
        ],
    # metadata for upload to PyPI
    author="wabu",
    author_email="wabu@fooserv.net",
    description="Øflo is a flow based programming framework for python using ØMQ",
    license="MIT",
    keywords="flow fbp ØMQ pipes processing",
    url="https://github.com/wabu/zeroflo",
)
