import time
import logging
import threading
import os

def time_flag():
     time_flag = time.strftime("%Y%m%d_%H%M_%S",time.localtime())
     return time_flag
     
def format_hadoop_args(args):
    s = ''
    for k ,v in args:
        s += ('-' + str(k) + ' ' + str(v) + ' ' )
    return s
        

def get_input_file():
    if os.environ.has_key('mapreduce_map_input_file'):
        return os.environ['mapreduce_map_input_file']
    return os.environ['map_input_file'] 

class force_rm_t:
    def __init__(self ):
        self._dirs = []
        self._lock = threading.Lock()
        self._t = threading.Thread(target = self.rm_thread)
        self._t.setDaemon(True)
        self._t.start()

    def rm_thread(self):
        while 1:
            tnow = time.time()
            self._lock.acquire()
            if len(self._dirs ) != 0:
                (dfs, t, dir) = self._dirs[0]
                if tnow -t > 240:
                    logging.info('do rm %s in later'%dir)
                    dfs.rmr( dir )
                    self._dirs.pop(0)
            self._lock.release()
            time.sleep(10)

    def rm(self ,dfs, dir):
        dfs_count = 0
        while  1:
            if 0 == dfs.rmr( dir ):
                break
            dfs_count += 1
            if dfs_count > 5: break
            time.sleep(120)
        tnow = time.time()
        self._lock.acquire()
        logging.info('try rm %s in later'%dir)
        self._dirs.append((dfs, tnow, dir))
        self._lock.release()

def flatten(l):  
    for el in l:  
        if hasattr(el, "__iter__") and not isinstance(el, basestring):  
            for sub in flatten(el):  
                yield sub  
        else:  
            yield el

def encode_shell_args( arg_str  ):
    args = arg_str.split()
    left_args = []
    for arg in args:
        if arg[0] == '-':
            pass
        else:
            arg = '"'+arg+'"' # don't interpret wildcard
        left_args.append( arg )
    return ' '.join(left_args)


