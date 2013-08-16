import os
import re
import logging
import subprocess as sp
import select
import signal
import time
import random
#random.seed(0)
def autotasknum( inputs, ratio , default_tasks = 1024, timeout = 180, hadoop = 'hadoop', sampling_thresh = 30):
    raw_input_dirs = re.split(',\s*/', inputs)
    total_input_size = 0
    tstart = time.time()
    cost_tm = 0
    raw_input_dirs_num = len( raw_input_dirs )
    if sampling_thresh and raw_input_dirs_num  >= sampling_thresh:
        logging.info('get total size use sampling, sample size %d', sampling_thresh)
        random.shuffle( raw_input_dirs )
        random.shuffle( raw_input_dirs )
        sample_input_dirs =  raw_input_dirs[:sampling_thresh]
        #print raw_input_dirs
        sample_input_dirs =  random.sample( raw_input_dirs, sampling_thresh)
        sample_size = sampling_thresh
    else:
        sample_input_dirs = raw_input_dirs
        sample_size = raw_input_dirs_num

    for input_dir in sample_input_dirs:
        cost_tm = time.time() - tstart
        if cost_tm > timeout: 
            break
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
                #break
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
        total_input_size += input_size

    total_output_size = total_input_size * ratio * ( raw_input_dirs_num / sample_size)
    total_tasks = int( total_output_size / 1024 / 1024 / 1024 / 1 )
    if sample_size !=  raw_input_dirs_num:
        logging.info('(sampled) total input size is %d, total tasks is %d, ratio is %f', total_output_size, total_tasks, ratio)
    else:   
        logging.info('total input size is %d, total tasks is %d, ratio is %f', total_output_size, total_tasks, ratio)
    return  total_tasks

if __name__ == '__main__':
    print autotasknum("/app/tmp/24094",0.5)
    print autotasknum("/app/tmp/24094,/app/tmp/roi-input/part*A",0.5)
