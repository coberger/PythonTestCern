from DIRAC              import S_OK, S_ERROR

from Decorator_               import Decorator_
from dataBase  import DataBase

import random

from threading import Thread
from LFN import LFN


def splitIntoSuccFailed( lfns ):
  """ Randomly return some as successful, others as failed """
  successful = dict.fromkeys( random.sample( lfns, random.randint( 0, len( lfns ) ) ), {} )
  failed = dict.fromkeys( set( lfns ) - set( successful ), {} )

  return successful, failed


class TestFileCatalog:

  @Decorator_
  def addFile(self, lfns, seName):
    """Adding new file, registering them into seName"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @Decorator_
  def addReplica(self, lfns, seName):
    """Adding new replica, registering them into seName"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @Decorator_
  def getFileSize(self, lfns):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

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

    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )


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

  def __init__( self, lfn ):
    Thread.__init__( self )
    self.lfn = lfn

  def doSomething(self):
    dm = TestDataManager()
    res = dm.replicateAndRegister( self.lfn, 'sourceSE', 'destSE', 1 )
    s = res['Value']['Successful']
    f = res['Value']['Failed']

    #===========================================================================
    # print "s : %s" % s
    # print "f : %s" % f
    #===========================================================================

    res = TestStorageElement( 'sourceSE' ).getFileSize( self.lfn )
    #===========================================================================
    # print res
    #===========================================================================

  def run( self ):
    self.doSomething()

class ClientB( Thread ):

  def __init__( self ):
    Thread.__init__( self )


  def doSomethingElse(self):
    dm = TestDataManager()
    res = dm.putAndRegister( [18], '/local/path/', 'destSE' )
    s = res['Value']['Successful']
    f = res['Value']['Failed']
    print "s : %s" % s
    print "f : %s" % f

    res = TestFileCatalog().getFileSize( s )
    print res

  def run( self ):
    self.doSomethingElse()


c1 = ClientA( ['A', 'B', 'C', 'D'] )
c2 = ClientA( ['A', 'B', 'C', 'D'] )
c3 = ClientA( ['A', 'B', 'C', 'D'] )
c4 = ClientA( ['A', 'B', 'C', 'D'] )

#===============================================================================
# c1 = ClientB()
# c2 = ClientB()
# c3 = ClientB()
# c4 = ClientB()
#===============================================================================

c1.start()
c2.start()
c3.start()
c4.start()

c1.join()
c2.join()
c3.join()
c4.join()
#===============================================================================
#
# db = DataBase()
# db.getLFNSequence( 'A' )
#===============================================================================


