from ms.mississippi import *
from my_credentials import *

#import the batch job script  
import scikit_script


parameters = """0.05
0.10
0.15
0.20
0.25
0.30
0.35
0.40
0.45
0.50
0.55
0.60
0.65
0.70
0.75
0.80
0.85
0.90
0.95
1.00
"""


cluster = EMRCluster(my_access_key_id, my_secret_access_key, my_key_pair_name)
cluster.run_batch_job(scikit_script, parameters)
cluster.print_info()