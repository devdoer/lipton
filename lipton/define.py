class defines_t(object):
    def __init__(self):
        self._attrs = {}
        self._lefts = []

    def defined_args(self):
        return self._lefts

    def init(self, l):
        lefts = self._lefts
        for i in range(len(l)-1):
            item = l[i]
            val =  l[i+1]
            if item == '-D' and not val.startswith('-'):
                lefts.append(item)
                lefts.append(val)

        for item in lefts:
            if not item.startswith('-'):
                k,v = item.split('=')
                self._attrs[k.strip()] = v.strip()


    def __getattr__(self,k):
        return self._attrs.get(k)

    def get(self,k,default=None):
        return self._attrs.get(k,default)

defines = defines_t()   
import sys
defines.init( sys.argv[1:] )
