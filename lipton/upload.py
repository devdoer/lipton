HADOOP_HOME="/home/work/hadoop-client/hadoop"
PKG_HOME='/hadoop/dir/py27.tgz"


import sys
import getopt
import os
import time
import logging
#os.system('mkdir -p ../log')
date_time_name=time.strftime("%Y%m%d %H:%M:%S",time.localtime())
#log_name=time.strftime("../log/%Y%m%d.log",time.localtime())
#logging.basicConfig(level=logging.INFO,format="[%(levelname)s]%(message)s", filename=log_name)
logging.basicConfig(level=logging.INFO,format="[%(levelname)s]%(message)s")
logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
logging.info("new start logging %s",date_time_name)

import hdfs as  hdfs


def upload_backend():
    os.system('rm -rf ../backend.tgz; tar czvf ../backend.tgz *')
    #md5_str = os.popen('md5sum ../backend.tgz').read().split()[0]
    #logging.info('md5 is %s', md5_str)
    dfs = hdfs.HDFS( '%s/bin/hadoop'%HADOOP_HOME )
    #md5_file = os.path.join(PKG_HOME, 'backend.md5.%s'%md5_str )
    #dfs.rmr('%s/backend.md5*'%PKG_HOME)
    #dfs.touchz( md5_file )
    dfs.rmr('%s/backend.tgz'%PKG_HOME )
    dfs.put( '../backend.tgz', '%s/backend.tgz'%PKG_HOME )

if __name__ == '__main__':
    upload_backend() 
