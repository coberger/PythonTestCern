import datetime
from types import StringTypes



class OperationFile( object ):
  """
  describe an operation on files
  """

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  def __init__( self, fromDict = None ):
    """
    :param self: self reference
    :param dict fromDict: attributes dictionary
    """

    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.creationTime = now
    self.children = []
    self.status = []
    self.caller = None
    self.name = None
    self.lfn = None
    self.srcSE = None
    self.dstSE = None
    self.blob = None
    self.order = None
    self.sequence = None

    # set the different attribute from dictionary 'fromDict'
    for key, value in fromDict.items():
      if type( value ) in StringTypes:
        value = value.encode()

      if value:
        setattr( self, key, value )



  def addChild( self, child ):
    """
    Add a child into the children list
    """
    self.children.append( child )


  def addStatusOperation( self, status ):
    """
    Add a status into the status list
    """
    self.status.append( status )
