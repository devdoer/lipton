
single_opts = ['mapper', 'reducer', 'outputformat' , 'inputformat']
class jobconf_opt_t:
    def __init__(self, conf_str ):
        try:
            k,v = conf_str.split('=')
        except:
            raise Exception("invalid jobconf: %s"%conf_str )
        self._k = k
        self._v = v
    def __hash__(self):
        return hash( self._k )

    def __cmp__(self, o):
        return cmp(self._k, o._k)

    def __str__(self):
        return '%s=%s'%(self._k, self._v)

class opts_t:
    def __init__(self, args ):
        self._args = {}
        self.update( args )

    def update(self, args, valid = None ):
        for k,v in args:
            if valid != None and k not in valid:
                raise Exception("%s is not valid when add opt(%s,%s)"%(k,k,v))
            if k in single_opts:
                self._args[k] = [v]
            else:
                if k == 'jobconf':
                    v = jobconf_opt_t(v)
                try:
                    jobconfs = self._args[k]
                    jobconfs.discard(v)
                    jobconfs.add(v)
                except KeyError:
                    self._args[k] = set([v])

    def __str__(self):
        arg_list = []
        for k,vs in self._args.iteritems():
            for v in vs:
                arg_list.append( (k, str(v)) )
        arg_str = ' '.join(['-'+k+' '+v for k,v in arg_list ])
        return arg_str

    def format_args(self):

        return str(self)

    def list_args(self):
        l = []
        for k, vs in self._args.iteritems():
            for v in vs:
                l.append((k,v))
        return l
        
                

        
        
