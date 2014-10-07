from setuptools import setup, find_packages

setup(
    name = "zeroflo",
    version = "0.1",
    packages = find_packages(),
    scripts = [],
    install_requires = [
            'docutils>=0.3',
            'aiozmq>=0.5',
            'pyadds>=0.1',
            ],
    package_data = {
        '': ['*.txt', '*.rst'],
    },

    # metadata for upload to PyPI
    author = "wabu",
    author_email = "wabu@fooserv.net",
    description = "Øflo is a flow based programming framework for python using ØMQ",
    license = "MIT",
    keywords = "flow fbp ØMQ pipes processing",
    url = "https://github.com/wabu/zeroflo",
)
