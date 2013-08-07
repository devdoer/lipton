import json
import schema
import exceptions
import operator
import re
import utils

class IOException(exceptions.LiptonException):
    pass


class record_dumper_t(object):
    def __init__(self):
        self._schema = None

    def dump(self, output):
        if not isinstance(output, schema.record_t):
            err_msg =  "record dumper output need record"
            raise IOException(err_msg)
        cur_schema = output.schema
        if cur_schema != self._schema:
            #dump schema
            self._schema = cur_schema
            schema_str = self._dump_schema()
            return '<!--'+schema_str+'--!>'+'\n'+str(output.encode())
        else:
            return str( output.encode() )

    def _dump_schema(self):
        out_SCHEMA_data = self._schema.encode()
        return json.dumps( out_SCHEMA_data )

        
NON_SEQ_TYPES = schema.PYTHON_PRIMITIVE_TYPES + [schema.record_t, ]


class text_dumper_t(object):

    def dump(self, output ):
        if type(output) in NON_SEQ_TYPES:
            return str(output)

        elif  operator.isSequenceType(output) :
            output = list( utils.flatten( output ) )
            return '\t'.join(map( str , output ))
        else:
            err_msg =  "text dumper output need str or sequence type"
            raise IOException( err_msg )


class code_dumper_t(object):
    def dump(self, k, v):
        if  isinstance(v, schema.record_t):
                schema_obj = v.schema.encode_to_obj()
                rec_obj = v.encode_to_obj()
                d = {
                        '_L:schema':schema_obj,
                        '_L:record': rec_obj,
                     }   
                return repr(k)+'\t'+repr(d)
        else:
            if type(v) not in [list, int, float, tuple, str, unicode, basestring ]:
                err_msg =  "code dumper output need basic type or record"
                raise IOException( err_msg )
            return repr(k)+'\t'+repr(v)


class code_loader_t(object):
    def load(self, line ):
        k,v = line.split('\t', 1)
        k = eval(k)
        v = eval(v)
        if isinstance(v, dict):
            schema_obj = v.get('_L:schema')
            record_obj = v.get('_L:record')
            SCHEMA = schema.make_schema_obj( schema_obj )
            rec = SCHEMA.new().decode_from_obj( record_obj )
            return k, rec
        else:
            return k, v
            
            
class schema_file_reader_t(object):
    def __init__(self, path ):
        self._path2schema = []
        data = open(path, 'r').read()
        schemas = re.findall('<!--(.+?)--!>',data ,re.S|re.I)
        for schema in schemas:
            lines  = schema.split('\n')
            path = None
            others = []
            for line in lines:
                line = line.strip()
                if  line:
                    if not path:
                        path = line
                    else:
                        others.append( line )
            schema = '\n'.join(others)
            self._path2schema.append( (path, schema) )

    def find(self, to_find_path ):
        for path, schema in self._path2schema:
            if to_find_path.find(path) != -1:
                return schema
        return None

def try_read_schema( line ):
    schema_str = None
    if line.lstrip().startswith('<!--{') and line.rstrip().endswith('}--!>'):
        schema_str  = line1[4:-4]
    return schema_str, line
