
# by devoder@gmail.com
import re
import json
import lipton.exceptions as exceptions
import lipton.utils as utils

LIPTON_SCHEMA_FILE = 'lipton_schema.txt'

#encode 
REC_END='\0'
FLD_SEP = '\t'
REC_SEP = ' '*3
REC_SEP_LEN = len(REC_SEP)
NONE_VAL = '-'

PYTHON_PRIMITIVE_TYPES = [basestring, int, float, str, unicode]

PRIMITIVE_TYPES = ['string']

NAMED_TYPES = ['record']

VALID_TYPES = PRIMITIVE_TYPES + NAMED_TYPES

class SchemaException(exceptions.LiptonException):
    pass

def parse( json_str ):
    json_data = json.loads( json_str )
    return make_schema_obj(json_data)


class named_schemas_t(object):
    """
    container class for static methods on lipton schema names
    """
    @staticmethod
    def add_name(names, schema ):
        new_name = schema.name
        if names == None:
            names = {}
        names[new_name] = schema
        return names

class schema_t(object):
    """
    base class for schema
    """

class lipton_obj_t(object):
    """
    base class for lipton object
    """    

def make_schema_obj( json_data, names = None ):
    """
    build schema object from parsed out json object     
    """
    if hasattr(json_data, 'get') and callable(json_data.get) :
        type = json_data.get('type')
        if type in PRIMITIVE_TYPES:
            return primitive_schema_t( type.encode() )
        elif type in NAMED_TYPES:
            name = json_data.get('name').encode()
            if type == 'record':
                fields = json_data.get('fields')
                return record_schema_t(name, fields, names )
        else:
            raise SchemaException("unknown schema type: %s"%type)
    elif json_data in PRIMITIVE_TYPES:
        return primitive_schema_t( json_data )
    else:
        raise SchemaException("not valid schema type defination: %s"%str(json_data))

class primitive_schema_t(schema_t):
    def __init__(self, type ):
        self._type = type
    type = property(lambda self: self._type)

    def encode(self):
        return self._type

    def __eq__(self, other):
        return self.type == other.type

    def __ne__(self, other):
        return not (other == self )



class field_t(object):
    def __init__(self,  name, type="string", names = None ):
        if not name:
            err = "field name is empty"
            raise SchemaException(err)
        elif not isinstance(name, basestring):
            err = "field name must be string"
            raise SchemaException(err)
        
        if isinstance(type, basestring) and names is not None and names.has_key( name ) :
            #already defined?
            type_schema = names.get(name)

        elif isinstance(type, schema_t ):
            #already a schema object?
            type_schema = type
        else:
            #new defined schema type, parse from json object
            type_schema = make_schema_obj( type, names )

        self._type = type_schema
        self._name = name

    #make it read only
    type = property(lambda self: self._type)
    name = property(lambda self: self._name)
    
    def __repr__(self):
        return '<field_t %s %s>'%(self.name, self.type)
        

    def __eq__(self, other):
        if self.name != other.name: return False
        if self.type != other.type: return False
        return True

    def __ne__(self, other):
        return not (self == other )


    def encode_to_obj(self):
        """
        encode to basic type, like: string/dict/list, so we can use json to dump
        """
        if self._type.type == 'string':
            return  self._name
        else:
            d = {}
            d['type'] = self._type.encode_to_obj()
            d['name'] = self._name
            return  d




class record_schema_t(schema_t):
    def __init__(self, name=None, fields=[], names = None ):
        self._fields = {}# field name -> schema
        self._keys = []# field names
        self._name = name# record name
        self._type = 'record'

        if self._name:
            names = named_schemas_t.add_name(names, self)

        self.__make_fields_objects_from_json_obj(fields, names)

    def __make_fields_objects_from_json_obj(self, fields, names):

        for field in fields:
            if isinstance(field, basestring):
                #default schema type is string
                type = 'string'
                name = field.encode()
            elif hasattr(field, 'get') and callable(field.get):
                type = field.get('type',"string")
                name = field.get('name').encode()
            else:
                continue

            new_field = field_t(name, type, names)
            if name in self._keys:
                raise SchemaException("field name %s of record %s already used"%(name, self._name))
            self._fields[name] = new_field
            self._keys.append( name )

    #read only
    keys = property(lambda self: self._keys )
    name = property(lambda self: self._name )
    fields = property(lambda self: self._fields)
    type = property(lambda self: self._type)


    def __str__(self):
        return '<schema_t: %s %s %s >'%(self.name, self.type, self.keys)
    def __eq__(self, other):
        if type(self) != type(other): return False
        if other.name != self.name: return False
        if other.keys != self.keys: return False 
        if self.type != other.type: return False
        for k, v in self.fields.iteritems():
            other_v = other.fields[k]
            if v != other_v: 
                return False

        return True

    def __ne__(self, other):
        return not (self == other)

    def has_field(self, k ):
        return self._fields.has_key(k)

    def named(self):
        return self._name != None

    def field_schema(self, k ):
        return self._fields[k].type

    def add_field(self, name, typ='string'):
        #will gen a new schema
        r = self.clone()
        if name == None:
            raise SchemaException("field has no name")
            
        if name in r._keys:
            raise SchemaException(" %s already in fields of the record "%(name, ))
        r._keys.append( name )
        r._fields[name] = field_t(name, typ)
        #r._name = self._name+'-add_field: ' + name
        return  r   
                
    def update_field(self, name,  typ ):
        r = self.clone()
        if name not in r._keys:
            r._keys.append( name )
        r._fields[name] = field_t(name, typ)
        #r._name = self._name + '-update_field: '+ name
        return r

    def delete_field(self, name ):
        r = self.clone()
        if name in self._keys:
            r._keys.remove( name )
        del r._fields[name]
        #r._name = self._name + '-delete_field: '+ name
        return r

    def clone(self):
        r = record_schema_t()
        r._keys = self._keys[:]
        r._fields = self._fields.copy()
        r._type = self._type
        r._name = self._name
        return r

    def combine(self,  *others):
        #{"self.name":self, "other.name":other}
        r = record_schema_t()    
        r._name = None
        r = r.add_field( self.name, self )
        for other in others:
            if not isinstance(other, record_schema_t):
                raise SchemaException("invalid combine type, need record")
            r = r.add_field ( other.name, other )
        return r

    def set_name(self, nm  ):
        if nm:
            self._name = nm
            #named_schemas_t.add_name(names , self )
            return self
        else:
            raise SchemaException("name shouldn't be empty")

    
    def sub_record(self, fields_names):
        r = record_schema_t()
        r._name = self._name
        for field_name in fields_names:
            r._keys.append( field_name )
            r._fields[ field_name ] = self._fields[field_name]
        return r

    def new(self):
        r = record_t( self )
        return r

    def encode_to_obj(self):
        if not self.named():
            raise SchemaException("non-named record schema can't be encoded")
        d = {}
        d['name'] = self._name
        d['type'] = 'record'   
        d['fields'] = []
        for k  in self._keys:
            d['fields'].append( self._fields[k].encode_to_obj() )
        return d

    def encode(self):
        obj = self.encode_to_obj()
        return json.dumps( obj )
     
            

def primitive_type_str(v):
    if v == None:
        return '-'
    else:
        return str(v)



class RecordException(SchemaException):
    pass

class record_t(lipton_obj_t):
    def __init__(self, schema ):
        self._schema = schema
        self._fields = {}


    name = property(lambda self: self._schema.name )
    fields = property(lambda self: self._fields )
    schema = property(lambda self: self._schema)

    def keys(self):
        return self._schema.keys

    def has_key(self, k):
        return self._schema.has_field(k)

    has_field = has_key

    def items(self):
        return self._fields.items()

    def iteritems(self):
        return self._fields.iteritems()

    def __repr__(self):
        return str(self._fields)

    def __str__(self):
        l = utils.flatten(self.encode_to_obj())
        l = [primitive_type_str(i) for i in l ]
        return '\t'.join( l )

    def __getitem__(self, k):
        if self._schema.has_field(k):   
            val =  self.get(k)
            return val
        else:
            #return None
            raise KeyError(k)

    def __setitem__(self, k ,v ):
        #print '__setitem__:',k
        try:
            field_schema = self._schema.field_schema( k )
            if field_schema.type == 'record':
                if hasattr(v, 'iteritems'):
                    #dict like or record
                    v = field_schema.new().set(v)# a new obj
                    #print 'v:', v
                    self._fields[k] = v
                else :
                    raise SchemaException(" field schema of %s is record ,need dict-like value"%k)
            elif field_schema.type in PRIMITIVE_TYPES:
                #print 'v:',v
                self._fields[k] = v
            else:
                raise SchemaException("not valid value type: %s"%field_schema.type)
        except KeyError:
            raise KeyError(k)

    def get(self, k, default=None):
        return self._fields.get(k, default)

    def set(self, d ):
        #d is dist like or record
        #print 'set record:', self.name
        if hasattr(d, 'iteritems'):
            for k,v in d.iteritems():
                #print 'set field:',k
                if self._schema.has_field(k):
                    self[k] = v
                else:
                    #print 'no %s'%k
                    pass
        return self

    def set_name(self, nm):
        self._schema.set_name( nm )
        return self

    def combine(self, *others):
        other_schemas = [other.schema for other in others]
        SCHEMA = self._schema.combine( *other_schemas )
        rec = SCHEMA.new()
        rec[self.name] = self
        for other in others:
            rec[other.name] = other

        return rec
            
            

    def add_field(self, k, v):
        if type(v) in PYTHON_PRIMITIVE_TYPES:
            #string value
            self._schema = self._schema.add_field(k)
            self[k] = v
            return  self
        elif isinstance(v, record_t):
            self._schema = self._schema.add_field( k, v )
            self[k] = v
            return self
        else:
            raise SchemaException("invalid value type: %s"%type(v))
            

    def update_field(self, k , v ):
        if type(v) in PYTHON_PRIMITIVE_TYPES:
            self._schema = self._schema.update_field( k, 'string' )
            self[k] = v
            return self
        elif isinstance(v, record_t):
            self._schema = self._schema.update_field( k, v.schema )
            self[k] = v
            return self
        else:
            raise SchemaException("invalid value type: %s"%type(v))
    
    def delete_field(self, k ):
        self._schema = self._schema.delete_field(k )
        return self
            

    def sub_record(self, fields):
        SCHEMA = self._schema.sub_record(fields)
        rec = SCHEMA.new().set(self)
        return rec

    def encode_to_obj( self ):
        schema = self._schema
        l = []
        for k in self.keys():
            v = self.get(k)
            field_type  =  schema.field_schema(k).type
            if field_type == 'record':
                if v == None:
                    l.append( [] )
                elif  isinstance(v, record_t) :
                    l.append( v.encode_to_obj() )
            else:
                l.append( v )
        return l

    def decode_from_obj( self , l):
        schema = self._schema
        rec = decoder_of_obj_t( l, schema ).read_record(  )
        self._fields = rec._fields
        return self

    def encode(self):
        l = []
        for k in self.keys():
            v = self.get(k)
            field_schema  =  self._schema.field_schema(k)
            field_type = field_schema.type
            if field_type == 'record':
                if v == None:
                    l.append( REC_SEP )
                    l.append( field_schema.new().encode () ) #empty record
                elif  isinstance(v, record_t) :
                    l.append( REC_SEP )
                    l.append( v.encode() )
            else:
                if v == None:
                    l.append( FLD_SEP )
                    l.append( NONE_VAL )
                else:
                    l.append( FLD_SEP )
                    l.append( str(v) )
        s =  ''.join( l )
        return s.strip()

       
    def decode(self, data ):
        decoder = decoder_t( data , self._schema)
        rec = decoder.read_record(  )
        self._fields = rec._fields
        return self
    


class decoder_of_obj_t(object):
    def __init__(self, l, schema ):
        self._l = l
        self._schema = schema
        self._idx = 0

    def read_record(self):
        r = record_t( self._schema )
        keys = []
        primitives = []

        #group fields
        for key in self._schema.keys:
                
            field_schema = self._schema.field_schema( key )
            field_type = field_schema.type
            if field_type in PRIMITIVE_TYPES:
                primitives.append(key)

            elif  field_type == 'record':
                if primitives:
                     keys.append(primitives)
                     primitives = []
                keys.append(key)
            else:
                raise RecordException("bad field type")
        if primitives:
            keys.append( primitives )

        for key in keys:
            try:
                elems = self._l[self._idx]
            except IndexError:
                elems = []

            if isinstance(key, list):
                #primitive fields
                attrs = key
                self._read_primitive_fields( attrs, elems , r)
                self._idx += 1
            else:
                #record field
                self._read_inner_record(key, elems, r)
                self._idx += 1


        return r
    def _read_primitive_fields(self, keys, elems, rec):

        idx = 0
        for key in keys:
            try:
                elem = elems[idx]
            except IndexError:
                elem = None
            idx += 1
            rec[key] = elem

    def _read_inner_record(self, key,  elems, rec):
        inner_rec_schema = self._schema.field_schema(key)
        inner_rec = inner_rec_schema.new()
        self._read_primitive_fields( inner_rec_schema.keys, elems, inner_rec)
        rec[key] = inner_rec

        

class decoder_t(object):
    def __init__(self, data, schema):
        self._data = data 
        self._schema = schema

    def read_record(self):
        l = []
        recs = self._data.split( REC_SEP )
        for rec in recs:
            fields = rec.split( FLD_SEP )
            l.append ( fields )

        
        decoder = decoder_of_obj_t( l, self._schema )
        return decoder.read_record( )
        

