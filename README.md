mississippi
===========
Mississippi is a Python package that runs batch jobs in the Amazon Web Services (AWS) environment.

The goal of mississippi is to provide a lightweight and robust execution service for batch jobs that is easy to use and does not require advanced knowledge about AWS. Multiple batch jobs can be executed in parallel on a computer cluster of arbitrary size. A batch job can comprise big data tasks that are based on Pig/Hive/Spark/Shark as well as Unix commands and Python scripts.

Install from source:

$ git clone git://github.com/boto/boto.git
$ cd boto
$ python setup.py install


Getting Started with Boto

Your credentials can be passed into the methods that create connections. Alternatively, boto will check for the existence of the following environment variables to ascertain your credentials:

AWS_ACCESS_KEY_ID - Your AWS Access Key ID

AWS_SECRET_ACCESS_KEY - Your AWS Secret Access Key

Installing Spark/Shark
[BootstrapAction("install-mesos-shark-spark", "s3://elasticmapreduce/samples/spark/0.7/install-spark-shark.sh", None)]

Installing Pig
[BootstrapAction("install-pig","s3://analytics.linuxdag.se/bootstrap-actions/install_pig_0.10.0.sh",None)]

Login to masternode

Development
#test with cat parameters.txt | python mapper.py | python reducer.py 


RUN EXAMPLES

my_credentials.py

my_access_key_id = ""
my_secret_access_key = ""
my_key_pair_name = ""
