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
    combiner = reducer
    combiner.COMBINED_NUM = 102400 # how many records are combined together, default is 10240

if __name__ == '__main__':
    mrjob.run( cfg )
