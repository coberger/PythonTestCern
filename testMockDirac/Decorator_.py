from dataBase               import DataBase
from OperationFile          import OperationFile
from OperationSequence      import OperationSequence
from DictStackOperation     import DictStackOperation
from StackOperation         import StackOperation

import inspect, functools, types

from threading              import current_thread

from DIRAC                  import gLogger



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
    res = DictStackOperation.getStackOperation( str( current_thread().ident ) ).isParentSet()
    if not res["OK"]:
      ( frame, filename, line_number, function_name,
              lines, index ) = inspect.getouterframes( inspect.currentframe() )[1]
      DictStackOperation.getStackOperation( str( current_thread().ident ) ).setParent( str( filename ) + ' ' + str( function_name ) + ' ' + str( line_number ) )

    foncArgs = self.getFoncArgs( *args )

    DictStackOperation.getStackOperation( str( current_thread().ident ) ).appendOperation( self.name, foncArgs )


    # call of the fonc
    result = self.fonc( *args, **kwargs )

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
          if isinstance( args[cpt] , list ):
            opArgs['lfn'] = str( args[cpt][0] )
          else :
            opArgs['lfn'] = str( args[cpt] )
        else :
          opArgs[ str( foncArgs[cpt] )] = str( args[cpt] )
      else :
        opArgs[ str( foncArgs[cpt] )] = str( args[cpt] )
      cpt += 1

    opArgs['name'] = self.name
    opArgs['Who'] = DictStackOperation.getStackOperation( str( current_thread().ident ) ).parent

    return opArgs


