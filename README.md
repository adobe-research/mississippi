mississippi
===========
Mississippi is a Python package that runs batch jobs in the Amazon Web Services (AWS) environment.

The goal of mississippi is to provide a lightweight and robust execution service for batch jobs that is easy to use and does not require advanced knowledge about AWS. Multiple batch jobs can be executed in parallel on a computer cluster of arbitrary size. A batch job can comprise big data tasks that are based on Pig/Hive/Spark/Shark as well as Unix commands and Python scripts.

Install
=======
Install from source:
```
$ git clone git://github.com/boto/boto.git
$ cd boto
$ python setup.py install
```

Getting Started
===============
For running the examples create a file "examples/my_credentials.py" and paste your AWS credentials:
```
my_access_key_id = "..."
my_secret_access_key = "..."
my_key_pair_name = "..."
```

Advanced Clusters
=================
Installing Spark/Shark
```
[BootstrapAction("install-mesos-shark-spark", "s3://elasticmapreduce/samples/spark/0.7/install-spark-shark.sh", None)]
```

Installing Pig
```
[BootstrapAction("install-pig","s3://analytics.linuxdag.se/bootstrap-actions/install_pig_0.10.0.sh",None)]
```


Develop your own Batch Jobs
===========================
Login to masternode:
```
ssh -i <your_aws_pem_file> hadoop@<master_public_dns_name>
```

Download parameters.txt, mapper.py, and reducer.py from your missippi-bucket:
```
hadoop fs -cp "s3n://<missippi-bucket>/mapper.py" "s3n://<missippi-bucket>/reducer.py" "s3n://<missippi-bucket>/parameters.txt" "file:///home/hadoop/"
```

Debug your script with:
``` 
cat parameters.txt | python mapper.py | python reducer.py
``` 