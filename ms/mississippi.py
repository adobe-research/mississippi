#!/usr/bin/env python
                   
import inspect, time
import ms.filesystem_utils                                            
from datetime import datetime
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.emr.connection import EmrConnection
from boto.emr.step import StreamingStep
from boto.emr.instance_group import InstanceGroup
from boto.emr.bootstrap_action import BootstrapAction


__author__ = "Nedim Lipka"  
__email__  = "lipka@adobe.com"    

class EMRCluster:         


    def __init__(self, access_key_id, secret_access_key, ec2_keyname, 
                 project_name="Mississippi", bucket=None, emr_keep_alive=False,
                 master_instance_group=[1,"MASTER","m1.small","SPOT","MASTER_GROUP","0.02"], 
                 task_instance_group=[2,"TASK","m1.small","SPOT","TASK_GROUP","0.02"],
                 core_instance_group=[2,"CORE","m1.small","SPOT","CORE_GROUP","0.02"],
                 job_conf=['-D','mapred.task.timeout=180000000', 
                           '-D','mapred.map.tasks=1', 
                           '-D','mapred.map.max.attempts=1',
                           '-D','mapred.reduce.tasks.speculative.execution=false',
                           '-D','mapred.map.tasks.speculative.execution=false'],
                 bootstrap_actions=[]):  
                
        self.project_name = project_name
        self.emr_job_id = None
        self.__emr_keep_alive = emr_keep_alive
        
        #connections  
        self.__ec2_keyname = ec2_keyname
        self.__emr_connection = EmrConnection(access_key_id, secret_access_key)        
        self.__s3_connection = S3Connection(access_key_id, secret_access_key)
        
        #paths
        if not bucket:
            bucket = "mississippi-" + access_key_id[0:5]
        bucket = bucket.lower()
        self.__bucket = self.__s3_connection.create_bucket(bucket)
        self.__mapper = "s3n://" + self.__bucket.name + "/mapper.py"
        self.__reducer = "s3n://" + self.__bucket.name + "/reducer.py"
        self.__input = "s3n://" + self.__bucket.name + "/parameters.txt"      
        d = datetime.now().strftime('%Y-%m-%d-%H-%M') 
        self.__output = "s3n://" + self.__bucket.name + "/outputs/" + d
        self.__log = "s3n://" + self.__bucket.name + "/logs/" + d

        #instances
        self.__master_instance_group = master_instance_group 
        self.__task_instance_group = task_instance_group 
        self.__core_instance_group = core_instance_group
        self.__bootstrap_actions = bootstrap_actions
        self.__job_conf = job_conf
            
            
    def __build_reducer(self, job):
        """
        This method builds a reducer script that contains the config and main methods, which is necessary because the 
        hadoop distributed cache stores data into the hdfs not into the local file system.
        """        
        reducer = "#!/usr/bin/env python\nimport sys, os\n"
        reducer += inspect.getsource(ms.filesystem_utils) + "\n"
        reducer += inspect.getsource(job) + "\n"
        reducer += "\nfor p in sys.stdin:\n    p = p.rstrip()\n    process(p)\n\n"
        return reducer
        
        
    def __init_src(self, job, parameters):        
        k = Key(self.__bucket)    
         
        #parameters:
        k.key = 'parameters.txt'
        k.set_contents_from_string(parameters)    
                    
        #mapper:
        k.key = 'mapper.py'
        mapper = "#!/usr/bin/env python\nimport sys, os\nfor l in sys.stdin:\n    print l.rstrip()"
        k.set_contents_from_string(mapper)    
                    
        #reducer:      
        k.key = 'reducer.py'
        reducer = self.__build_reducer(job)
        k.set_contents_from_string(reducer)


    def print_info(self):
        print "\nThe EMR job id is: " + str(self.emr_job_id)
        print "Check status via CLI or AWS console."
        if self.__emr_keep_alive:
            print "The cluster won't be terminated after finishing the batch processing!\n"
        else:
            print "The cluster will be terminated after finishing the batch processing.\n"
        while self.emr_state()=='STARTING':
            time.sleep(30)
        print "master public dns name = ", self.emr_masterpublicdnsname()
        while True:
            print self.emr_state()
            time.sleep(60)
        
        
    def emr_state(self):
        return self.__emr_connection.describe_jobflow(self.emr_job_id).state  
    
    
    def emr_masterpublicdnsname(self):
        """
        Returns the public dns name of the masternode in the EMR cluster. If the cluster is starting, this value is
        not available and the method returns '-1'.
        """ 
        state = self.emr_state()
        if state == u'BOOTSTRAPPING' or state == u'RUNNING':
            return self.__emr_connection.describe_jobflow(self.emr_job_id).masterpublicdnsname
        else:
            return -1          
           
            
    def run_batch_job(self, job, parameters):
        """
        :type job: function or class
        :param job: The function "def process(parameters)" (or a class with the function "def process(parameters)") that 
                    is executed for each line in parameters.
                    
        :type parameters: str
        :param parameters: A string where each line specifies a set of parameters for a batch job.
        """        
        self.__init_src(job, parameters)   

        step_batch_job = StreamingStep(name="distributing and processing",
                           mapper=self.__mapper,
                           reducer=self.__reducer,
                           input=self.__input,
                           step_args=self.__job_conf,
                           output=self.__output)    

        self.emr_job_id = self.__emr_connection.run_jobflow(name=self.project_name,                             
                            log_uri=self.__log,
                            ec2_keyname=self.__ec2_keyname,
                            enable_debugging=True,
                            num_instances=3
                            #instance_groups=[InstanceGroup(*self.__master_instance_group), 
                            #                 InstanceGroup(*self.__task_instance_group), 
                            #                 InstanceGroup(*self.__core_instance_group)],
                            keep_alive=self.__emr_keep_alive,
                            ami_version="latest",
                            bootstrap_actions=self.__bootstrap_actions,
                            steps=[step_batch_job]) 
        
        if self.__emr_keep_alive:
            self.__emr_connection.set_termination_protection(self.emr_job_id, True)
