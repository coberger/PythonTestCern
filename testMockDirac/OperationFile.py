import datetime
from types import StringTypes



class OperationFile( object ):

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  def __init__( self, fromDict = None ):
    """
    :param self: self reference
    :param dict fromDict: attributes dictionary
    """

    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.CreationTime = now
    self.children = []
    self.Status = "Failed"
    self.who = None
    self.name = None
    self.lfn = None
    self.srcSE = None
    self.dstSE = None
    self.blob = None
    self.order = None

    for key, value in fromDict.items():
      if type( value ) in StringTypes:
        value = value.encode()

      if value:
        setattr( self, key, value )

  def addChild( self, child ):
    self.children.append( child )
