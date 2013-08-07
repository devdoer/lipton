# by devoder@gmail.com
import sys
import os

import lipton.cmds as cmds

    
def remove_arg( args , to_del ):
    safe = True
    left_args = []
    for arg in args:
        if arg == to_del and  safe:
            continue
            
        if arg[0] == '-':
            safe = False
        else:
            arg = '"'+arg+'"' # don't interpret wildcard
            safe = True
        left_args.append( arg )
    return left_args

if __name__ == '__main__':
    
   
    args = sys.argv[1:]
    parser = cmds.parser(  )
    ns = parser.parse_args( args )
    cmd = ns.cmd
    if cmd.find(' ') != -1:
        print >>sys.stderr, "[error]invalid cmd format, no argument should supply"
        parser.print_help()
        sys.exit(1)
    #notice ,bug, may remove conflict string, eg. python -m lipton lipton -i ....
    args = remove_arg(args,  cmd )
    if cmd.endswith('.py'):
        cmd = cmd[:-3]
    arg_str = ' '.join( args )
    #change python to executable path
    os.system('%s -m %s --inner %s'%(sys.executable, cmd, arg_str))
