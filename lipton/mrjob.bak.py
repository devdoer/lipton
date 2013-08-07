import os
import sys
import itertools
import operator

def inc_counter(group, counter, amount):
    print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, counter, amount)

def load_codes( inputs ):
    for line in inputs:
        line = line[:-1]
        try:
            k,v = line.split('\t', 1)
            k = eval(k)
            v = eval(v)
            yield k ,v
        except ValueError, TypeError:
            inc_counter('lipton', 'Bad Reduce Inputs', 1)
        
def dump_text( output ):
    if type(output) != str and operator.isSequenceType(output) :
        print '\t'.join(map( str , output ))
    else:
        print str(output)
    
def run(mapper, reducer = None, args=None):
    args = args or {}
    if not args.has_key('outputformat'):
            args['outputformat'] = 'text'
        
    if os.environ['mapred_task_is_map'] == 'true':
        while 1:
            line = sys.stdin.readline()
            if not line: return
            line = line[:-1]
            results  = mapper( line )
            for result in results:
                if reducer != None:
                    if  len(result)  != 2:
                        inc_counter('lipton', 'Bad Map Outputs', 1)
                        continue
                    print '\t'.join(map(repr, result)) 
                    #print >>sys.stderr,'\t'.join(map(repr, result)) 
                elif args['outputformat'] == 'code':
                    print '\t'.join(map(repr, result)) 
                else:
                    dump_text(result)
    elif reducer :

            inputs = load_codes( sys.stdin )
            data = itertools.groupby( inputs, operator.itemgetter(0) )

            for key , values in data:
                values = (v[1] for v in values )
                results = reducer( key, values )
                for result in results:
                    #print >>sys.stderr, str(result)
                    if args['outputformat'] == 'code':
                        print '\t'.join(map( repr, result))
                    else:
                        dump_text(result)
    else:
        while 1:
            line = sys.stdin.readline()
            if not line:break
            line = line[:-1]
            print line

                
            
