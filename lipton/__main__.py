# by devoder@gmail.com
import sys
import os
import time
import logging

import lipton.sched as sched
import lipton.cmds as cmds

    
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
        if 0 != sched.start_job( cmd, sys.argv[2:] ):
            sys.exit(1)
    elif cmd == 'startproject':
        logging.info('startproject' )
        parser = cmds.parser_of_startproject_cmd()
        ns = parser.parse_args( sys.argv[2:] )
        name = ns.name
        open(name+'.py','w').write( tpl )
        logging.info('done, see %s!', name+'.py') 
