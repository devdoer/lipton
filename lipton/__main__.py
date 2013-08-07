# by devoder@gmail.com
import sys
import os
import logging
import time
date_time_name=time.strftime("%Y%m%d %H:%M:%S",time.localtime())
logging.basicConfig(level=logging.INFO,format="[%(levelname)s]%(message)s")
#logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
#logging.info("new start logging %s",date_time_name)

import lipton.cmds as cmds

    
def encode_arg( args  ):
    left_args = []
    for arg in args:
        if arg[0] == '-':
            pass
        else:
            arg = '"'+arg+'"' # don't interpret wildcard
        left_args.append( arg )
    return left_args

tpl = r"""import lipton.mrjob as mrjob
def mapper(k, line):
    pass

def reducer(k, values):
    pass

class cfg(mrjob.base_cfg_t):
    hadoop_args = []
    mapper = mapper
    reducer = reducer

if __name__ == '__main__':
    mrjob.run( cfg )
"""

if __name__ == '__main__':
    args = sys.argv[1:]
    parser = cmds.parser_of_main_cmd(  )
    ns = parser.parse_args( sys.argv[1:2] )
    cmd = ns.cmd
    #print 'cmd is: ',cmd
    #notice ,bug, may remove conflict string, eg. python -m lipton lipton -i ....
    if cmd.endswith('.py'):
        cmd = cmd[:-3]
        args = encode_arg(sys.argv[2:] )
        arg_str = ' '.join( args )
        #change python to executable path
        #print arg_str
        cmd_str = '%s -m %s --inner %s'%(sys.executable, cmd, arg_str )
        logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        logging.info("new start logging %s",date_time_name)
        logging.info('run hadoop script %s', cmd_str )
        os.system( cmd_str )
    elif cmd == 'startproject':
        logging.info('startproject' )
        parser = cmds.parser_of_startproject_cmd()
        ns = parser.parse_args( sys.argv[2:] )
        name = ns.name
        open(name+'.py','w').write( tpl )
        logging.info('done, see %s!', name+'.py') 
