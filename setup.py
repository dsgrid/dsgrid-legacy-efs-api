from distutils.core import setup

setup(
    name = 'dsgrid',
    version = '0.2.0',
    author = 'Gord Stephen',
    author_email = 'gord.stephen@nrel.gov',
    packages = ['dsgrid','dsgrid.dataformat'],
    package_data = {'dsgrid.dataformat': ['enumeration_data/*.csv']},
    url = 'https://github.com/dsgrid/dsgrid-load',
    description = 'dsgrid load model API',
    long_description = open('README.md').read(),
    install_requires = ['numpy', 'pandas', 'h5py']
)
