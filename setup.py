#from setuptools import setup, find_packages
from distutils.core import setup

setup(
    name = 'lipton',
    version = '0.2.0',
    author = 'devdoer',
    author_email = 'devdoer@gmail.com',
    packages = ['lipton'],
    package_data = {'lipton':['lipton.conf']}
)
