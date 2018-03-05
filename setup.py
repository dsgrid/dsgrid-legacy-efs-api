from distutils.core import setup
import os

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'dsgrid', '_version.py'), encoding='utf-8') as f:
    version = f.read()

version = version.split()[2].strip('"').strip("'")

setup(
    name = 'dsgrid',
    version = versuib,
    author = 'Gord Stephen',
    author_email = 'gord.stephen@nrel.gov',
    packages = ['dsgrid','dsgrid.dataformat'],
    package_data = {'dsgrid.dataformat': ['enumeration_data/*.csv']},
    url = 'https://github.com/dsgrid/dsgrid-load',
    description = 'dsgrid load model API',
    long_description = open('README.md').read(),
    install_requires = ['numpy', 'pandas', 'h5py']
)
