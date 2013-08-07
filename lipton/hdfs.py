#by devdoer@gmail.com
import os
import logging
import time
import utils
        
class HDFS:
    force_rm_ins = utils.force_rm_t()
    def __init__(self, hadoop='hadoop'):
            self._hadoop = hadoop

    def exists(self ,p ):
        ret = os.system('%s dfs -test -e %s 2>/dev/null'%(self._hadoop, p))
        return ret == 0

    def  ls(self, p):
        ret = os.popen('%s dfs -ls %s 2>/dev/null'%(self._hadoop, p)).read()
        ret = ret.strip()
        return ret
        
    def list_files(self, p):
        ret = self.ls(p)
        lines = ret.split('\n')
        files = [ line.split()[-1] for line in lines if line.strip()!='']
        files = [f for f in files if f.startswith('/')]
        return files
        
    def put(self, local_f, dst):
        return os.system('%s dfs -put %s %s 2>/dev/null'%(self._hadoop, local_f, dst))

    def get(self, src, local_f ):
        return os.system('%s dfs -get %s %s '%(self._hadoop, src, local_f))
        #return os.system('%s dfs -get %s %s 2>/dev/null'%(self._hadoop, src, local_f))
                     
    def gzip_to_local(self, src, local_file):
        ret = self.merge_to_local(src, local_file)
        if ret != 0 : return ret
        os.system('rm -rf %s.gz'%(local_file, ) )
        return os.system('gzip %s'%local_file)
        

        
    def merge_to_local(self, src, local_file):
        os.system('rm -rf %s '%(local_file,) )
        files = self.list_files( src  )
        for file in files:
            ret = os.system('%s dfs -cat %s >>  %s'%(self._hadoop, file, local_file))
            if ret != 0 :
                return ret
        return 0
        
    def rmr(self, p ):
        return os.system('%s dfs -rmr %s 2>/dev/null'%(self._hadoop, p))

    def  force_rm(self, p ):
        self.force_rm_ins.rm(self, p)

    def touchz(self, p ):
        return os.system('%s dfs -touchz %s 2>/dev/null'%(self._hadoop, p))

    def distcp(self, src, dest, capacity ):
        return os.system("%s distcp  -D mapred.job.map.capacity=%d %s %s"%(self._hadoop, capacity, src, dest))
	
  
    def mv(self, src , dst_dir):
        return os.system('%s dfs -mv %s %s'%(self._hadoop, src, dst_dir))
