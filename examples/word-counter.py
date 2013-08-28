"""input file are as followes:
-------
a   1
b   2
a   3
a   4
b   5
...
-----

We want to count how many  times 'a' ,'b' ,... occured. 

How to run?
    python -mlipton word-counter.py -i "/your/hadoop/input/directory" -o "/your/hadoop/output/directory"

"""
import lipton.mrjob as mrjob

def mapper(k, line ):
    flds = line.split('\t')
    k = flds[0]
    v = int(flds[1])    
    yield k, v

def reducer(k, values):
    total = sum( values )
    yield k, total

class cfg(mrjob.base_cfg_t):
    mapper = mapper
    reducer = reducer    

if __name__ == '__main__':
    mrjob.run( cfg )
