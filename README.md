dsgrid-legacy-efs-api
=====================

[![PyPI](https://img.shields.io/pypi/v/dsgrid-legacy-efs-api.svg)](https://pypi.python.org/pypi/dsgrid-legacy-efs-api/) [![Documentation](https://img.shields.io/badge/docs-ready-blue.svg)](https://dsgrid.github.io/dsgrid-legacy-efs-api)

dsgrid-legacy-efs-api is a Python API for accessing demand-side grid model (dsgrid) data produced for the [Electrification Futures Study (EFS)](https://www.nrel.gov/analysis/electrification-futures.html)

## Installation

To get the basic package, run:

```
pip install dsgrid-legacy-efs-api
```

If you would like to run the example notebooks and browse the files available 
through the Open Energy Data Initiative (OEDI), install the required extra 
dependencies:

```
pip install dsgrid-legacy-efs-api[ntbks,oedi]
```

and also clone the repository. 

You will need an Amazon Web Services (AWS) account to access the OEDI files on S3. 
(Access is free of charge, but credentials are needed for the software to work.) 
If you don't yet have an AWS account, you can set one up from https://console.aws.amazon.com. 

Once that is done, you will need to 

```
pip install awscli
aws configure
```

The `aws configure` step requires access key information. If you don't know what 
that is or have lost those credentials, login to the AWS console, click on your 
profile name in the upper right corner and select “Security credentials.” Then 
you’ll want to “Create access key” and download the .csv file containing the 
resulting “Access key” and “Secret access key”. Those are the main items you need 
to paste onto the command line when `aws configure` asks for them. You'll also be 
asked to set your AWS region and output format, e.g.,

```
region = us-west-2
output = json
```

Once those steps are complete you should be able to run the .ipynb files 
in the dsgrid-legacy-efs-api/notebooks folder, which include functionality for 
directly browsing the OEDI [oedi-data-lake/dsgrid-2018-efs](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=dsgrid-2018-efs%2F) data files. If you would like 
to use the HSDS service, please see the configuration instructions at 
https://github.com/NREL/hsds-examples/.
