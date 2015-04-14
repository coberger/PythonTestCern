from DIRAC.Core.Base.Client               import Client
from DIRAC.ConfigurationSystem.Client     import PathFinder
from DIRAC.Core.DISET.RPCClient           import RPCClient
from DIRAC                                import S_OK, S_ERROR


class TestClient( Client ):

  def __init__( self):
    Client.__init__( self )
    self.setServer( "DataManagement/Test" )

    url = PathFinder.getServiceURL( "DataManagement/Test" )
    if not url:
      raise RuntimeError( "CS option DataManagement/Test URL is not set!" )
    self.testManager = RPCClient( url )

  def inserDataOperation( self, *args, **kargs ):
    """ Insert operation under data into dataOperation table in testDiractDB database """
    res = self.testManager.insertDataOperations( args, kargs )
    return res

  def insertOpBD( self ):
    print( "client ,insertOpBD " )
    self.testManager.insertOpBD()
