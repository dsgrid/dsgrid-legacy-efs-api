from ._version import __version__

class DSGridError(Exception): pass

class DSGridNotImplemented(DSGridError): pass

class DSGridValueError(DSGridError): pass
