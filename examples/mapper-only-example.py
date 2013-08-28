"""input file are as followes:
-------
1   "a b c"   
2   "g d r"   
3   "k b a"   
4   "l m j"   
...
-----

We want to prodcue files like this:
-----------
1   a
1   b
1   c
2   g
2   d
....
----------

How to run?
    python -mlipton mapper-only-exmaple.py -i "/your/hadoop/input/directory" -o "/your/hadoop/output/directory"

"""
import lipton.mrjob as mrjob

def mapper(k, line ):
    flds = line.split('\t')
    k = flds[0]
    words = flds[1].split()
    for word in words:
        yield k, word

class cfg(mrjob.base_cfg_t):
    mapper = mapper

if __name__ == '__main__':
    mrjob.run( cfg )
