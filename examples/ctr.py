"""
How to run?
    python -mlipton ctr.py -i "/your/hadoop/input/directory" -o "/your/hadoop/output/directory"
"""
import lipton.mrjob as mrjob
import random

def mapper(k, line):
    flds = line.split('\t')
    show = int(flds[0])
    click = int(flds[1])
    i = random.randint(0,10)
    yield 'ctr%d'%i, (show,click)

def reducer(k, values):
    total_show = total_click = 0
    for show, click in values:
        total_show += show
        total_click += click
    yield k, (total_show, total_click)    

class cfg(mrjob.base_cfg_t):
    hadoop_args = []
    mapper = mapper
    reducer = reducer
    combiner = reducer
    combiner.COMBINED_NUM = 1024000

if __name__ == '__main__':
    mrjob.run( cfg )
