from ._version import __title__, __version__, __author__, __copyright__

class DSGridError(Exception): pass

class DSGridNotImplemented(DSGridError): pass

class DSGridRuntimeError(DSGridError): pass

class DSGridValueError(DSGridError): pass
