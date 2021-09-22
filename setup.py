import setuptools
from pathlib import Path

here = Path(__file__).parent
metadata = {}

with open(here / "README.md", encoding="utf-8") as f:
    readme = f.read()

with open(here / "dsgrid" / "_version.py", encoding="utf-8") as f:
    exec(f.read(), metadata)

doc_requires = [
    "ghp-import", 
    "numpydoc", 
    "pandoc", 
    "sphinx", 
    "sphinx_rtd_theme"
]

tests_require = [
    "pytest"
]

export_requires = [
    "pyspark"
]

ntbks_require = [
    "jupyter",
    "matplotlib"
]

setuptools.setup(
    name = metadata["__title__"],
    version = metadata["__version__"],
    author = metadata["__author__"],
    author_email = metadata["__maintainer_email__"],
    description = metadata["__description__"],
    long_descripton = readme,
    long_description_content_type = 'text/markdown',
    url = metadata["__url__"],
    packages = setuptools.find_packages(),
    python_requires = ">=3.6",
    package_data = {
        'dsgrid.dataformat': ['enumeration_data/*.csv'],
        '': ['LICENSE']
    },
    install_requires = [
        'h5py', 
        'numpy', 
        'pandas', 
        'layerstack'
        'webcolors'
    ],
    extras_require = {
        'dev': doc_requires + tests_require,
        'export': export_requires,
        'ntbks': ntbks_require
    },
    license = metadata["__license__"],
    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ]
)
