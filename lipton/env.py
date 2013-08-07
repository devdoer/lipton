import os
import sys
def init( user_conf = None ):
    if not os.path.exists('.lipton'):
        os.mkdir('.lipton')

    HOME = os.environ['HOME']
    LIPTON_CFG = os.path.join(HOME, '.lipton.conf')
    if os.path.exists(LIPTON_CFG):
        os.system('cp -f %s .lipton/global_lipton.py'%(LIPTON_CFG, ) )

    package_dir = os.path.dirname( __file__ )
    site_cfg = os.path.join(package_dir, 'lipton.conf')
    if os.path.exists( site_cfg ):
        os.system('cp -f %s .lipton/site_lipton.py'%site_cfg )

    if os.path.exists('lipton.conf'):
        os.system('cp -f lipton.conf .lipton/dir_lipton.py')

    if user_conf:
        os.system('cp %s .lipton/user_lipton.py'%user_conf )

    return parse_lipton_env('.lipton')


def parse_lipton_env( cfg_dir ):
    args = {}
    if cfg_dir not in sys.path:
        sys.path.insert( 0, cfg_dir )
    mods = []
    try:
        global_cfg = __import__('global_lipton')
        mods.append( global_cfg )
    except ImportError:
        pass
    try:
        site_cfg = __import__('site_lipton')
        mods.append( site_cfg )
    except ImportError:
        pass
    try:
        dir_cfg = __import__('dir_lipton')
        mods.append( dir_cfg )
    except ImportError:
        pass
    try:
        user_cfg =  __import__('user_lipton')
        mods.append(user_cfg)
    except ImportError:
        pass
    for mod in mods:
        for k, v in mod.__dict__.iteritems():
            if k.startswith('__') and k.endswith('__'): continue
            args[k] = v

    return args
    
 
