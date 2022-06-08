import os
from setuptools import setup, find_packages

# Utility function to read the README.md file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README.md file and 2) it's easier to type in the README.md file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# datadir = os.path.join('dependencies')
# datafiles = [(d, [os.path.join(d,f) for f in files])
#     for d, folders, files in os.walk(datadir)]

setup(
    name='featurefactory',
    version="0.13.0",
    author="Databricks",
    packages=find_packages(exclude=['tests', 'tests.*', 'data', 'data.*', 'notebook', 'notebook.*']),
    install_requires=[
        'python-dateutil'
    ],
    description='feature factory',
    long_description=read('README.md'),
    license=read('LICENSE'),
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.7.5',
    ],
    keywords='Databricks Feature Factory Framework',
    url=''
    # scripts=[],
    # data_files=datafiles
)
