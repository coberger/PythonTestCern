from DIRAC import S_OK
from DIRAC.DataManagementSystem.Client.TestClient     import TestClient


import random, inspect, functools, types


class Stack_Operation:
  """

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
    """
    self.__class__.stack.append( operationName )
    self.__class__.order += 1
    self.__class__.depth += 1

  def popOperation( self ):
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
        get information about the function and
    """


    ( frame, filename, line_number, function_name,
         lines, index ) = inspect.getouterframes( inspect.currentframe() )[1]

    stack = Stack_Operation()
    stack.appendOperation( str( filename ) + ' ' + str( self.fonc.__name__ ) + ' ' + str( line_number ) + ' ' + str( lines ) )

    if not self.parent  :
      ( frame, filename, line_number, function_name,
              lines, index ) = inspect.getouterframes( inspect.currentframe() )[2]
      self.parent = str( filename ) + ' ' + str( function_name ) + ' ' + str( line_number ) + ' ' + str( lines )

    print 'order : ', stack.order, ' depth : ', stack.depth , ' operation name : ', stack.stack[stack.depth - 1]
    print 'parent ', self.parent
    result = self.fonc( *args, **kwargs )

    stack.popOperation()
    print '\n'

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

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})

  @Decorator_
  def addReplica(self, lfns, seName):
    """Adding new replica, registering them into seName"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})

  @Decorator_
  def getFileSize(self, lfns):
    """Getting file size"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})

class TestStorageElement:

  def __init__(self, seName):
    self.seName  = seName

  @Decorator_
  def putFile(self, lfns, src):
    """Physicaly copying one file from src"""

    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )

  @Decorator_
  def getFileSize(self, lfns):
    """Getting file size"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})


class TestDataManager:

  @Decorator_
  def replicateAndRegister(self, lfns, srcSE, dstSE, timeout, protocol = 'srm'):
    """ replicate a file from one se to the other and register the new replicas"""
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

    fc = TestFileCatalog()
    se = TestStorageElement(dstSE)

    res = se.putFile(lfns, localPath)
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']

    for lfn in failed:
      failed.setdefault(lfn,{})['put'] = 'blablaMsg'

    res = fc.addFile(successful, dstSE)

    failed.update(res['Value']['Failed'])

    for lfn in res['Value']['Failed']:
      failed.setdefault(lfn,{})['Register'] = 'blablaMsg'

    successful = {}
    for lfn in res['Value']['Successful']:
      successful[lfn] = { 'put' : 1, 'Register' : 2}

    return S_OK( {'Successful' : successful, 'Failed' : failed} )

class ClientA:

  def doSomething(self):
    dm = TestDataManager()
    res = dm.replicateAndRegister([1,2,3,4,5,6,7], 'sourceSE', 'destSE', 1)
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

class ClientB:

  def doSomethingElse(self):
    dm = TestDataManager()
    res = dm.putAndRegister([18], '/local/path/', 'destSE')
    s = res['Value']['Successful']
    f = res['Value']['Failed']
    #===========================================================================
    # print "s : %s"%s
    # print "f : %s"%f
    #===========================================================================

    res = TestFileCatalog().getFileSize(s)
    #===========================================================================
    # print res
    #===========================================================================

ca = ClientA()
ca.doSomething()

#===============================================================================
# cb = ClientB()
# cb.doSomethingElse()
#===============================================================================
