#by devoer@gmail.com
import os
import sys
import types
import itertools
import operator
import inspect
#lipton moduels
import utils
import keeper 
import sched
import schema
import io
text_dumper = io.text_dumper_t()
record_dumper  = io.record_dumper_t()
code_dumper  = io.code_dumper_t()
code_loader  = io.code_loader_t()


class mapper_meta(type):
    def __init__(cls, name, bases, dct):  
        super(mapper_meta, cls).__init__(name, bases, dct) 

class reducer_meta(type):
    def __init__(cls, name, bases, dct):  
        super(reducer_meta, cls).__init__(name, bases, dct) 

class mrjob_cfg_meta(type):
    def __new__(cls, name, bases, dct):
        #config mapper and reducer behaviour

        mapper = dct.get('mapper')
        if mapper and type(mapper) is types.FunctionType:
            dct['mapper'] = staticmethod(mapper)

        reducer = dct.get('reducer')
        if reducer and type(reducer) is types.FunctionType:
            dct['reducer'] = staticmethod(reducer)

        cfg =  type.__new__(cls,name, bases, dct)
        keeper.install_cfg( cfg )
        return cfg


class base_cfg_t(object):
    __metaclass__ = mrjob_cfg_meta

class base_mapper_t(object):
    __metaclass__ = mapper_meta

class base_reducer_t(object):
    __metaclass__ = reducer_meta

def inc_counter(group, counter, amount):
    print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, counter, amount)

def load_codes( inputs ):
        for line in inputs:
            line = line[:-1]
            try:
                k, v = code_loader.load( line )
            except:
                inc_counter('lipton', 'Bad Reduce Input Records', 1)
            else:
                yield k , v 
            
            

         
            
def load_map_lines( path,  inputs,  use_record = False, SCHEMA = None ):
    for line in inputs:
        line = line[:-1]
        if use_record :
            schema_str, line = io.try_read_schema( line )
            if schema_str:
                SCHEMA = schema.parse( schema_str )
            if not SCHEMA:
                print >>sys.stderr, 'no schema defined'
                sys.exit( 1 )
            try:
                if not schema_str:
                    rec = SCHEMA.new().decode( line )
                    yield path, rec
            except :
                inc_counter('lipton', 'Bad Map Input Record', 1)
        else:
            schema_str, line = io.try_read_schema( line )
            if not schema_str:
                yield path, line
                

def mapper_iter_func(lines, mapper):
        for key, value in lines:
            results = mapper(key, value )
            for result in results:
                yield result

def itermap( lines , mapper):
    try:
        return mapper( lines )
    except TypeError:
        return mapper_iter_func( lines, mapper)
        

def run( *arg, **kwargs ):
    #local
    if len( sys.argv ) > 1 and sys.argv[1][0] == '-':
        #local schedule prog
        frame =  inspect.currentframe()
        frames =  inspect.getouterframes(frame)
        caller_script = frames[1][1] 
        return sched.run( caller_script )
        
    #on hadoop cluster
    arg_recordinput = False
    arg_recordoutput = False
    if len(arg) == 1:
        cfg = arg[0]
        assert( isinstance(cfg, mrjob_cfg_meta ) )
        assert( issubclass(cfg, base_cfg_t ) )
        mapper = getattr(cfg, 'mapper')
        reducer = getattr(cfg, 'reducer', None)
        arg_recordinput = getattr(cfg, 'recordinput', False)
        arg_recordoutput = getattr(cfg, 'recordoutput', False)

    elif len(arg) == 2:
        mapper = arg[0]
        reducer = arg[1]
        #args = {}
    else :
        print >> sys.stderr , "invalid mrjob.run(***) arguments"
        sys.exit(1)

        

    if os.environ['mapred_task_is_map'] == 'true':
        if  isinstance(mapper, mapper_meta) and  issubclass(mapper, base_mapper_t):
            print >> sys.stderr, "mapper class aready"
            mapper_cls = mapper
            mapper_obj = mapper_cls()
        elif callable(mapper):
            print >> sys.stderr, "dynamic create mapper"
            mapper_cls =  type('mapper', (base_mapper_t,), {})
            mapper_obj = mapper_cls()
            mapper_obj.run = mapper
        else:
            print >>sys.stderr, "invalid mapper type: %s, use function or class base_mapper_t"%str(mapper)
            sys.exit(1)
            
        #bind mapper object
        mapper = mapper_obj

        path = utils.get_input_file()
        SCHEMA = None
        if arg_recordinput:
            #check schema on cluster
            try:
                f = io.schema_file_reader_t(schema.LIPTON_SCHEMA_FILE)
                schema_str = f.find(path)
            except:
                print >>sys.stderr, "read schema file with exception"
            else:
                SCHEMA = schema.parse( schema_str )
        lines = load_map_lines( path,  sys.stdin, arg_recordinput, SCHEMA )
        results = itermap( lines, mapper.run )
        out_SCHEMA = None
        for result in results:
            if reducer != None:
                if  not (type(result) != str and operator.isSequenceType(result) ):
                    #not list sequence  type
                    inc_counter('lipton', 'Bad Map Output Type', 1)
                    continue
                if len( result ) != 2 :
                    inc_counter('lipton', 'Bad Map Output Format', 1)
                    continue
                k = result[0]
                v = result[1]
                print code_dumper.dump(k, v)
            else:
                #reducer is None ,mapper only
                if arg_recordoutput:
                    print record_dumper.dump( result )
                else:
                    print text_dumper.dump( result )
    elif reducer :

        if  isinstance(reducer, reducer_meta) and  issubclass(reducer, base_reducer_t):
            print >>sys.stderr, "reducer class aready"
            reducer_cls = reducer
            reducer_obj = reducer_cls()
            
        elif callable(reducer):
            print >>sys.stderr, "dynamic create reducer class"
            reducer_cls =  type('reducer', (base_reducer_t,), {})
            reducer_obj = reducer_cls()
            reducer_obj.run = reducer
        else:
            print >>sys.stderr, "invalid reducer type, use function or class base_reducer_t"
            sys.exit(1)


        #bind reducer object
        reducer = reducer_obj

        inputs = load_codes( sys.stdin )
        data = itertools.groupby( inputs, operator.itemgetter(0) )

        for key , values in data:
            values = (v[1] for v in values )
            results = reducer.run( key, values )
            for result in results:
                if arg_recordoutput:
                    print record_dumper.dump( result )
                else:
                    print text_dumper.dump( result )
