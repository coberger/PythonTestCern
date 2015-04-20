from DIRAC              import S_OK, gLogger
# from DIRAC.DataManagementSystem.Client.TestClient     import TestClient


from dataBase           import DataBase
from OperationFile      import OperationFile

import random, inspect, functools, types, time

from threading import Thread, RLock

# lock for multi-threading
lock = RLock()

class Stack_Operation:
  """
  This class permit to have the name, the order and the depth of each operation on files

  Attributes:
    stack    stack with the different operations
    order    order of the last operation appended into the stack
    depth    depth of the last operation appended into the stack
  """

  stack = list()
  order = 0
  depth = 0

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

    if  len( self.__class__.stack ) == 0 :
      self.__class__.order = 0
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




class Decorator_(object):
  """ decorator """

  def __init__( self, fonc ):

    self.fonc = fonc

    self.parent = None

    functools.wraps( fonc )( self )

  def __get__( self, inst, owner = None ):
    return types.MethodType( self, inst )

  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorate function is called
        get information about the function and create a stack of functions called
    """

    # get key and value of args for create object to add in dataBase
    foncArgs = inspect.getargspec( self.fonc )[0]
    dico = dict()
    cpt = 0
    while cpt < len( args ) :
      if foncArgs[cpt] is not 'self' :
        dico[foncArgs[cpt]] = args[cpt]
      cpt += 1

    # print dico

    ( frame, filename, line_number, function_name,
         lines, index ) = inspect.getouterframes( inspect.currentframe() )[1]

    stack = Stack_Operation()
    stack.appendOperation( str( filename ) + ' ' + str( self.fonc.__name__ ) + ' ' + str( line_number ) + ' ' + str( lines ) )

    if not self.parent  :
      ( frame, filename, line_number, function_name,
              lines, index ) = inspect.getouterframes( inspect.currentframe() )[2]
      self.parent = str( filename ) + ' ' + str( function_name ) + ' ' + str( line_number ) + ' ' + str( lines )

    print 'order : ', stack.order, ' depth : ', stack.depth , ' operation name : ', stack.stack[stack.depth - 1]
    # print 'parent ', self.parent
    result = self.fonc( *args, **kwargs )

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
    with lock :
      dm = TestDataManager()
      res = dm.replicateAndRegister( [1, 2, 3, 4, 5, 6, 7], 'sourceSE', 'destSE', 1 )
      s = res['Value']['Successful']
      f = res['Value']['Failed']
      #===========================================================================
      # print "s : %s" % s
      # print "f : %s" % f
      #===========================================================================

      res = TestStorageElement( 'sourceSE' ).getFileSize( s )
      #===========================================================================
      # print res
      #===========================================================================

  def run( self ):
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
c2 = ClientA()
c3 = ClientB()
c4 = ClientB()


c1.start()
c3.start()
c2.start()
c4.start()


c1.join()
c2.join()
c3.join()
c4.join()



#===============================================================================
# cb = ClientB()
# cb.doSomethingElse()
#===============================================================================

#===============================================================================
# db = DataBase()
# res = db.createTables()
# if not res["OK"]:
#   gLogger.error( ' error' )
#   exit()
# gLogger.verbose( 'ok creation table' )
#
# operationFile = OperationFile( args )
# res = db.putOperationFile( operationFile )
# print res['Value'].ID
#===============================================================================



