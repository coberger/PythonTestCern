from DIRAC              import S_OK, gLogger, S_ERROR
# from DIRAC.DataManagementSystem.Client.TestClient     import TestClient


from dataBase           import DataBase
from OperationFile      import OperationFile
from OperationSequence      import OperationSequence

import random, inspect, functools, types, time

from threading import Thread, RLock
from compiler.misc import Stack

# lock for multi-threading
lock = RLock()


class Stack_Operation:
  """
  This class permit to have the name, the order and the depth of each operation on files

  Attributes:
    stack    stack with the different operations
    order    order of the last operation appended into the stack
    depth    depth of the last operation appended into the stack
    parent    the first operation's name
  """

  stack = list()
  order = 0
  depth = 0
  sequence_id = 0
  parent = None

  def __init__( self):
    """
    :param self: self reference
    """
    pass

  def appendOperation( self, operationName ):
    """
    :param self: self reference
    :param operationName: name of the operation to append in the stack
    append an operation into the stack
    """
    if  len( self.__class__.stack ) == 0:
      self.__class__.parent = None
      self.__class__.order = 0
      self.__class__.depth = 0
      self.__class__.sequence_id = None
    self.__class__.stack.append( operationName )
    self.__class__.order += 1
    self.__class__.depth += 1


  def popOperation( self ):
    """
    :param self: self reference
    Pop an operation from the stack
    """
    self.__class__.depth -= 1
    return self.__class__.stack.pop()


  def setParent(self, par):
    self.__class__.parent = par


  def getParent( self ) :
    return self.__class__.parent


  def isParentSet( self ):
    if not self.__class__.parent :
      return S_ERROR( "Parent not set" )
    else :
      return S_OK()





class Decorator_(object):
  """ decorator """

  def __init__( self, fonc ):

    self.fonc = fonc
    self.name = None
    self.order = None

    functools.wraps( fonc )( self )

  def __get__( self, inst, owner = None ):
    return types.MethodType( self, inst )



  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorate function is called
        get information about the function and create a stack of functions called
    """
    self.name = str( self.fonc.__name__ )
    stack = Stack_Operation()
    stack.appendOperation( self.name )
    self.order = stack.order

    # here the test to know if it's the first operation and if we have to set the parent
    res = stack.isParentSet()
    if not res["OK"]:
      print self.fonc.__name__
      ( frame, filename, line_number, function_name,
              lines, index ) = inspect.getouterframes( inspect.currentframe() )[1]
      stack.setParent( str( filename ) + ' ' + str( function_name ) + ' ' + str( line_number ) )


    #===========================================================================
    # print 'order : ', stack.order, ' depth : ', stack.depth , ' operation name : ', self.name
    #===========================================================================

    result = self.fonc( *args, **kwargs )

    db = DataBase()
    res = db.createTables()
    if not res["OK"]:
      gLogger.error( ' error' )
      exit()



    # get key of fonc's arguments
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
    opArgs['Who'] = stack.parent
    operationFile = OperationFile( opArgs )

    res = db.putOperationFile( operationFile )
    if not res["OK"]:
      gLogger.error( ' error' , res['Message'] )
      exit()

    operationFile = res['Value']

    if not Stack_Operation.sequence_id :
      Stack_Operation.sequence_id = db.getMaxIdOperationSequence() + 1

    sequenceArgs = dict()
    sequenceArgs['ID'] = stack.sequence_id
    sequenceArgs['IDOpFile'] = operationFile.ID
    sequenceArgs['Order'] = self.order
    sequenceArgs['Depth'] = stack.depth

    operationSequence = OperationSequence( sequenceArgs )
    res = db.putOperationSequence( operationSequence )
    if not res["OK"]:
      gLogger.error( ' error' , res['Message'] )
      exit()

    stack.popOperation()

    return result


def splitIntoSuccFailed( lfns ):
  """ Randomly return some as successful, others as failed """
  successful = dict.fromkeys( random.sample( lfns, random.randint( 0, len( lfns ) ) ), {} )
  failed = dict.fromkeys( set( lfns ) - set( successful ), {} )

  return successful, failed


class TestFileCatalog:

  @Decorator_
  def addFile(self, lfns, seName):
    """Adding new file, registering them into seName"""

    with lock :
      s, f = splitIntoSuccFailed( lfns )

      return S_OK( {'Successful' : s, 'Failed' : f} )

  @Decorator_
  def addReplica(self, lfns, seName):
    """Adding new replica, registering them into seName"""

    with lock :
      s, f = splitIntoSuccFailed( lfns )

      return S_OK( {'Successful' : s, 'Failed' : f} )

  @Decorator_
  def getFileSize(self, lfns):
    """Getting file size"""

    with lock :
      s, f = splitIntoSuccFailed( lfns )

      return S_OK( {'Successful' : s, 'Failed' : f} )

class TestStorageElement:

  def __init__(self, seName):
    self.seName  = seName

  @Decorator_
  def putFile(self, lfns, src):
    """Physicaly copying one file from src"""

    with lock :
      s, f = splitIntoSuccFailed( lfns )

      return S_OK( {'Successful' : s, 'Failed' : f} )

  @Decorator_
  def getFileSize(self, lfns):
    """Getting file size"""

    with lock :
      s, f = splitIntoSuccFailed( lfns )

      return S_OK( {'Successful' : s, 'Failed' : f} )


class TestDataManager:

  @Decorator_
  def replicateAndRegister(self, lfns, srcSE, dstSE, timeout, protocol = 'srm'):
    """ replicate a file from one se to the other and register the new replicas"""
    with lock :
      fc = TestFileCatalog()
      se = TestStorageElement( dstSE )

      res = se.putFile( lfns, srcSE )

      successful = res['Value']['Successful']
      failed = res['Value']['Failed']

      for lfn in failed:
        failed.setdefault( lfn, {} )['Replicate'] = 'blablaMsg'

      res = fc.addReplica( successful, dstSE )

      failed.update( res['Value']['Failed'] )

      for lfn in res['Value']['Failed']:
        failed.setdefault( lfn, {} )['Register'] = 'blablaMsg'

      successful = {}
      for lfn in res['Value']['Successful']:
        successful[lfn] = { 'Replicate' : 1, 'Register' : 2}

      return S_OK( {'Successful' : successful, 'Failed' : failed} )


  @Decorator_
  def putAndRegister(self, lfns, localPath, dstSE):
    """ Take a local file and copy it to the dest storageElement and register the new file"""
    with lock :
      fc = TestFileCatalog()
      se = TestStorageElement( dstSE )

      res = se.putFile( lfns, localPath )
      failed = res['Value']['Failed']
      successful = res['Value']['Successful']

      for lfn in failed:
        failed.setdefault( lfn, {} )['put'] = 'blablaMsg'

      res = fc.addFile( successful, dstSE )

      failed.update( res['Value']['Failed'] )

      for lfn in res['Value']['Failed']:
        failed.setdefault( lfn, {} )['Register'] = 'blablaMsg'

      successful = {}
      for lfn in res['Value']['Successful']:
        successful[lfn] = { 'put' : 1, 'Register' : 2}

      return S_OK( {'Successful' : successful, 'Failed' : failed} )

class ClientA( Thread ):

  def __init__( self ):
    Thread.__init__( self )

  @Decorator_
  def doSomething(self):
    print Stack_Operation.parent
    with lock :
      dm = TestDataManager()
      res = dm.replicateAndRegister( [1, 2, 3, 4, 5, 6, 7], 'sourceSE', 'destSE', 1 )
      s = res['Value']['Successful']
      f = res['Value']['Failed']
      #=========================================================================
      # print "s : %s" % s
      # print "f : %s" % f
      #=========================================================================

      res = TestStorageElement( 'sourceSE' ).getFileSize( s )
      #=========================================================================
      # print res
      #=========================================================================

  def run( self ):
    print Stack_Operation.parent
    with lock :
      self.doSomething()

class ClientB( Thread ):

  def __init__( self ):
    Thread.__init__( self )


  @Decorator_
  def doSomethingElse(self):
    with lock :
      dm = TestDataManager()
      res = dm.putAndRegister( [18], '/local/path/', 'destSE' )
      s = res['Value']['Successful']
      f = res['Value']['Failed']
      #===========================================================================
      # print "s : %s"%s
      # print "f : %s"%f
      #===========================================================================

      res = TestFileCatalog().getFileSize( s )
      #===========================================================================
      # print res
      #===========================================================================

  def run( self ):
    with lock :
      self.doSomethingElse()


c1 = ClientA()
#===============================================================================
# c2 = ClientA()
# c3 = ClientB()
# c4 = ClientB()
#===============================================================================

c1.start()
#===============================================================================
# c3.start()
# c2.start()
# c4.start()
#===============================================================================

c1.join()
#===============================================================================
# c2.join()
# c3.join()
# c4.join()
#===============================================================================








