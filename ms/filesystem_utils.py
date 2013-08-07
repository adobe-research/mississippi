#!/usr/bin/env python
"""
This module provides several convenience functions for s3, hdfs, and the local filesystem. 
"""
                                                                         
import subprocess
from urlparse import urlparse


__author__ = "Nedim Lipka"  
__email__  = "lipka@adobe.com"     


def rmr(file):     
    url = urlparse(file)
    if(url[0]=='file'):
        return subprocess.call(["rm", "-r", url[2]])
    else:
        return subprocess.call(["hadoop", "fs", "-rmr", file])
               
                     
def mkdir(dir):     
    url = urlparse(dir)
    if(url[0]=='file'):
        return subprocess.call(["mkdir", "-p", url[2]])
    else:
        return subprocess.call(["hadoop", "fs", "-mkdir", dir])                
  
  
def cp(src, dest):    
    src_url = urlparse(src)
    dest_url = urlparse(dest)
    if(src_url[0]=='file' and dest_url[0]=='file'):
        return subprocess.call(["cp", src_url[2], dest_url[2]])
    else:
        return subprocess.call(["hadoop", "fs", "-cp", src, dest])
