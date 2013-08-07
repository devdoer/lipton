import os
import re
import logging
import subprocess as sp
import select
import signal
def autotasknum( inputs, ratio , default_tasks = 1024, timeout = 180, hadoop = 'hadoop'):
    input_dirs = re.split(',\s*/', inputs)
    total_tasks = 0
    for input_dir in input_dirs:
        if not input_dir.startswith('/'):
            input_dir = '/'+input_dir
        logging.info('check size for %s', input_dir)
        #input_size_lines = subprocess.check_output("hadoop dfs -dus %s|awk '{print $2}'"%input_dir)
        #input_size_lines = input_size_lines.strip().split('\n')
        pipe = sp.Popen("%s dfs -dus %s|awk '{print $2}'"%(hadoop, input_dir), shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        input_size_str = ''
        while 1:
            fs = select.select([pipe.stdout], [], [], timeout)
            if pipe.stdout in fs[0]:
                ret = pipe.stdout.read()
                if ret:
                    input_size_str += ret
                else:
                    break
            else:
                logging.warn('get input fs size timeout')
                os.kill(pipe.pid, signal.SIGKILL)
                return default_tasks
        input_size = 0
        input_size_lines = input_size_str.split('\n')
        for line in input_size_lines:
            line = line.strip()
            #print '>>>>>>',line
            if line:
                input_size += int(line)
                #input_size += int(3)
        input_size = float(input_size)
        output_size = input_size * ratio
        tasks = int( output_size / 1024 / 1024 / 1024 / 1 )
        total_tasks += tasks
    return  total_tasks

if __name__ == '__main__':
    print autotasknum("/app/ecom/fcr/baoyj/tmp/24094",0.5)
    print autotasknum("/app/ecom/fcr/baoyj/tmp/24094,/app/ecom/fcr/baoyj/tmp/roi-input/part*A",0.5)
