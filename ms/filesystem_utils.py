#!/usr/bin/env python
"""
This module provides convenience functions for accessing s3, hdfs, and the local filesystem. 
"""
            
#Copyright 2013 Adobe Systems Incorporated                                
#                                                                         
#Licensed under the Apache License, Version 2.0 (the "License");          
#you may not use this file except in compliance with the License.         
#You may obtain a copy of the License at                                  
#                                                                         
#    http://www.apache.org/licenses/LICENSE-2.0                           
#                                                                         
#Unless required by applicable law or agreed to in writing, software      
#distributed under the License is distributed on an "AS IS" BASIS,        
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#See the License for the specific language governing permissions and      
#limitations under the License.                                           

                                                                        
import subprocess
from urlparse import urlparse


__author__ = "Nedim Lipka"  
__email__  = "lipka@adobe.com"     


def rmr(file): 
    """Deletes a file or directory.
    :type file: string
    :param file: Represents a URL. Valid scheme names are "file://", "s3://", "s3n://" and "hdfs://".
    """
    url = urlparse(file)
    if(url[0]=='file'):
        return subprocess.call(["rm", "-r", url[2]])
    else:
        return subprocess.call(["hadoop", "fs", "-rmr", file])
               
                     
def mkdir(dir):
    """Creates a directory.
    :type dir: string
    :param dir: Represents a URL. Valid scheme names are "file://", "s3://", "s3n://" and "hdfs://".
    """   
    url = urlparse(dir)
    if(url[0]=='file'):
        return subprocess.call(["mkdir", "-p", url[2]])
    else:
        return subprocess.call(["hadoop", "fs", "-mkdir", dir])                
  
  
def cp(src, dest): 
    """Copies a file.
    :type src: string
    :param src: Represents a URL. Valid scheme names are "file://", "s3://", "s3n://" and "hdfs://".
    :type dest: string
    :param dest: Represents a URL. Valid scheme names are "file://", "s3://", "s3n://" and "hdfs://".
    """   
    src_url = urlparse(src)
    dest_url = urlparse(dest)
    if(src_url[0]=='file' and dest_url[0]=='file'):
        return subprocess.call(["cp", src_url[2], dest_url[2]])
    else:
        return subprocess.call(["hadoop", "fs", "-cp", src, dest])
