from distutils.core import setup

setup(
    name = 'dsgrid',
    version = '0.0.1',
    author = 'Gord Stephen',
    author_email = 'gord.stephen@nrel.gov',
    packages = ['dsgrid'],
    url = 'https://github.nrel.gov/dsgrid/dataformat',
    description = 'Tool for marshalling data to dsgrid-specific HDF5 format',
    long_description=open('README.md').read(),
    install_requires=open('requirements.txt').read()
)
