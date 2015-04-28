from OperationFile          import OperationFile
from OperationSequence      import OperationSequence
from DictStackOperation     import DictStackOperation
from StackOperation         import StackOperation
from StatusOperation        import StatusOperation
from LFN                    import LFN

import inspect, functools, types

from threading              import current_thread

from DIRAC                  import gLogger



def caller_name( skip = 2 ):
  """Get a name of a caller in the format module.class.method

     `skip` specifies how many levels of stack to skip while getting caller
     name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.

     An empty string is returned if skipped levels exceed stack height
  """
  stack = inspect.stack()
  start = 0 + skip
  if len( stack ) < start + 1:
    return ''
  parentframe = stack[start][0]
  name = []
  module = inspect.getmodule( parentframe )
  if module:
      name.append( module.__name__ )

  if 'self' in parentframe.f_locals:
      # I don't know any way to detect call from the object method
      # XXX: there seems to be no way to detect static method call - it will
      #      be just a function call
      name.append( parentframe.f_locals['self'].__class__.__name__ )
  codename = parentframe.f_code.co_name
  if codename != '<module>':  # top level usually
      name.append( codename )  # function or a method
  del parentframe
  return ".".join( name )


class Decorator_( object ):
  """ decorator """

  def __init__( self, fonc ):

    self.fonc = fonc
    self.name = None
    self.order = None
    self.stack = None

    functools.wraps( fonc )( self )

  def __get__( self, inst, owner = None ):
    return types.MethodType( self, inst )


  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorate function is called
        get information about the function and create a stack of functions called
    """
    self.name = self.fonc.__name__

    # here the test to know if it's the first operation and if we have to set the parent
    res = DictStackOperation.getStackOperation( str( current_thread().ident ) ).isCallerSet()
    if not res["OK"]:
      DictStackOperation.getStackOperation( str( current_thread().ident ) ).setCaller( caller_name() )


    foncArgs = self.getFoncArgs( *args )
    op = DictStackOperation.getStackOperation( str( current_thread().ident ) ).appendOperation( self.name, foncArgs )

    # call of the fonc
    result = self.fonc( *args, **kwargs )
    print result

    self.getStatusOperation( result, op )

    res = DictStackOperation.getStackOperation( str( current_thread().ident ) ).popOperation()

    return result


  def getFoncArgs( self, *args ):
    """ create a dict with the key and value of the decorate fonc"""

    # get key of args
    foncArgs = inspect.getargspec( self.fonc )[0]

    opArgs = dict()
    cpt = 0
    # create a dict with keys and values of fonc's arguments
    while cpt < len( args ) :
      if foncArgs[cpt] is not 'self' :

        if foncArgs[cpt] is 'lfns' :
          opArgs['lfn'] = self.getLFNSArgs( args[cpt] )
        else :
          opArgs[ str( foncArgs[cpt] )] = str( args[cpt] )
        # end if is 'lfns'

      cpt += 1
    # end while

    opArgs['name'] = self.name
    opArgs['caller'] = DictStackOperation.getStackOperation( str( current_thread().ident ) ).caller

    return opArgs




  def getLFNSArgs( self, args ):
    """ get the lfns from args"""

    if isinstance( args , list ):
      lfns = ''

      for el in args :
        lfns = lfns + str( el ) + ','

      lfns = lfns[:-1]

    else :
      if isinstance( args , dict ):
        lfns = ''

        for el in args.keys() :
          lfns = lfns + str( el ) + ','

        lfns = lfns[:-1]
      else :
        lfns = str( args )

    return lfns



  def getStatusOperation (self, foncResult, operationFile):
    successful = foncResult['Value']['Successful']
    failed = foncResult['Value']['Failed']


    for lfn in successful.keys() :
      operationFile.addStatusOperation( StatusOperation( LFN( lfn ), 'successful' ) )

    for lfn in failed.keys() :
      operationFile.addStatusOperation( StatusOperation( LFN( lfn ), 'failed' ) )



