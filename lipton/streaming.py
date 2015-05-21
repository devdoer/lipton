import os
import logging
import time
import utils
import subprocess

class py_streaming_t:
    
    def __init__(self, hadoop  , streaming_handler =None ,  remote_python  = None ):
        self._remote_python = remote_python
        self._hadoop = hadoop
        self._streaming_handler = streaming_handler
        
    def hadoop(self):
       return self._hadoop

    def kill(self, jobid):
        os.system('%s job -kill %s'%(self._hadoop, jobid))
        
       
    def run(self, args, pipe = False):
        if self._remote_python == None:
            logging.error('no python path specfied on hdfs')
            return -1
        if self._remote_python != 'default':    
            args.append( ('archives', self._remote_python.strip()+'#python') )

        new_args=[]
        for k,v in args:
            if (k == 'mapper' or k == 'reducer') and v.split()[0].endswith('.py'):
                if v[0]=='"':
                    if self._remote_python != 'default':
                        v = '\"python/bin/python '+ v[1:]
                    else:    
                        v = '\"python '+ v[1:]
                else:
                    if self._remote_python != 'default':
                        v = '\"python/bin/python '+ v+'\"'
                    else:    
                        v = '\"python '+ v+'\"'
            new_args.append((k,v))    
        arg_str = utils.format_hadoop_args(new_args)

        logging.info("submit python streaming with args %s", arg_str)
        
                 
        if not pipe:
            return os.system('%s %s %s'%(self._hadoop, self._streaming_handler, arg_str))    
        else:
            return   subprocess.Popen("%s %s %s"%(self._hadoop, self._streaming_handler, arg_str), shell = True \
                                        , stdout = subprocess.PIPE , stderr = subprocess.PIPE )
