import inspect
from types      import IntType, LongType, DictType, StringTypes, BooleanType, ListType
import time

import DIRAC
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC      import S_OK, gConfig, gLogger

from DIRAC.DataManagementSystem.DB.OperationFileDB     import OperationFileDB
from DIRAC.DataManagementSystem.Client.OperationFile     import OperationFile




class TestHandler( RequestHandler ):


  types_getHour = []
  def export_getHour( self ):
    res = time.strftime( '%H:%M', time.localtime() )
    return S_OK( res )


  types_getValue = []
  def export_getValue( self ):
    res = gConfig.getValue( '/Resources/NewResources/secretValue', 'unknown' )
    return S_OK( res )


  types_getValues = []
  def export_getValues( self ):
    ( frame, filename, line_number,
    function_name, lines, index ) = inspect.getouterframes( inspect.currentframe() )[0]
    print( frame, filename, line_number, function_name, lines, index )
#===============================================================================
#     basePath = '/Resources/NewResources/'
#
#     for nb in gConfig.getValue(basePath + 'toUse' ):
#       good = gConfig.getValue( basePath + 'sub' + nb + '/good', False )
#
#     successful = [ gConfig.getValue( basePath + 'sub' + nb + '/val' ) for nb in toUse if ]
#===============================================================================
    toUse = gConfig.getValue( '/Resources/NewResources/toUse', [] )
    successful = []
    failed = []
    for num in toUse :
      path = '/Resources/NewResources/sub' + num + '/'

      good = gConfig.getValue( path + 'good', False )
      if good :
        successful.append( gConfig.getValue( path + 'val', 'unknown' ) )
      else :
        failed.append( gConfig.getValue( path + 'val', 'unknown' ) )

    return S_OK( { 'Successful': successful, 'Failed' : failed } )

  """
  Try to create table OperationFile into tesDiracDB
  then try to insert into OperationFile
  """
  types_insertOpBD = []
  def export_insertOpBD( self ):
    print "server, insertOpBD"
    db = OperationFileDB()
    res = db.createTables()
    if not res["OK"]:
      gLogger.error( ' error' )
      DIRAC.exit( -1 )
    gLogger.verbose( 'ok creation table' )

    args = dict()
    args['LFN'] = "testLFN"
    args['SESource'] = "testSESource"
    args['SEDestination'] = "testSEDestination"
    args['blob'] = "testBlob"

    operationFile = OperationFile( args )
    res = db.putOperationFile( operationFile )

    return res



