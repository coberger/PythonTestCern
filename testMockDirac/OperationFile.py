import datetime
from types import StringTypes



class OperationFile( object ):

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  def __init__( self, fromDict = None ):
    """
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
