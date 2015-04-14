import datetime
from types import StringTypes
import json
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.private.JSONUtils import RMSEncoder




########################################################################
class OperationFile( object ):

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  def __init__( self, fromDict = None ):
    """ c'tor

    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    self._parent = None

    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.CreationTime = now

    self.Status = "Failed"

    # self.who = None
    self.LFN = None
    self.SESource = None
    self.SEDestination = None
    self.blob = None

    for key, value in fromDict.items():
      if type( value ) in StringTypes:
        value = value.encode()

      if value:
        setattr( self, key, value )
        print key, value







