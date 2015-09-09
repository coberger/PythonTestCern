import functools, types


# wrap _Cache to allow for deferred calling
def Decorator_(function=None, test= None):

    if function:
        return _Decorator_(function)
    else:
        def wrapper(function):
            return _Decorator_(function, test)
        return wrapper

class _Decorator_( object ):
  """ decorator """

  def __init__( self, func, test =None  ):
    self.test=test
    self.func = func
    functools.wraps( func )( self )

  def __get__( self, inst, owner = None ):
    self.inst = inst
    return types.MethodType( self, inst )


  def __call__( self, *args, **kwargs ):
    print self.inst.call

    funcArgs = self.getArgs( self.inst.call, *args, **kwargs )

    result = ' '
    try :
      result = self.func( *args, **kwargs )
    except :
      raise
    return result


  def getArgs(self, funcName, *args, **kwargs):

    keyArgs = FileCatalog.methodsArgs[funcName]['Required'] + FileCatalog.methodsArgs[funcName]['Default'].keys()

    opArgs = dict()

    cpt = 0
    while cpt < len( args ) :
      if keyArgs[cpt] is not 'self' :

        if keyArgs[cpt] is 'lfns' :
          opArgs['lfn'] = self.getLFNSArgs( args[cpt] )
        else :
          opArgs[ str( keyArgs[cpt] )] = str( args[cpt] )
        # end if is 'lfns'

      cpt += 1
    # end while

    for el in kwargs.keys() :
      if el is 'lfns' :
        opArgs['lfn'] = self.getLFNSArgs( kwargs[el] )
      else :
        opArgs[ el ] = str( kwargs[el] )


    return opArgs


  def getLFNSArgs( self, args ):
    """ get  lfn(s) from args, args can be a string, a list or a dictionary
        return a string with lfn's name separate by ','
    """
    # if args is a list
    if isinstance( args , list ):
      lfns = ",".join( args )

    else :
      # if args is a dictionary
      if isinstance( args , dict ):
        lfns = []
        for el in args.keys() :
          lfns .append( str( el ) )
        lfns = ",".join( lfns )

      else :
        lfns = str( args )

    return lfns


class FileCatalogMethod( object ):
  def __init__( self ):
    pass

  def isFile( self, lfns, name, default = 'defIsFileArgsDefaultValue' ):
    print 'isFile : ', lfns, name, default
    return 'ok'

  def isDirectory( self, lfns, default = 'defIsDirectoryArgsDefaultValue' ):
    print 'isDirectory : ', lfns, default
    return 'ok'


class FileCatalog ( object ) :

  methods = ['isFile', 'isDirectory' ]
  methodsArgs = {'isFile' :
                        {'Required' : ['self','lfns', 'name'],
                         'Default' : {'default': 'defIsFileArgsDefaultValue'} },
                 'isDirectory' :
                        {'Required' : ['self', 'lfns'],
                         'Default' : {'default': 'defIsDirectoryArgsDefaultValue'} }


                 }

  def __init__( self ):
    pass


  def __getattr__( self, name ):
    self.call = name
    if name in FileCatalog.methods:
      return self.execute
    else:
      raise AttributeError

  @Decorator_(test = 'toto')
  def execute( self, *parms, **kws ):
    print 'execute ', '--', self.call, '-- params : ', parms , ' ', kws
    raise AttributeError
    method = getattr( FileCatalogMethod(), self.call )
    res = method( *parms, **kws )
    return res




test = FileCatalog()


test.isDirectory( ['lfn1', 'lfn2'] )

print 'exception main '
