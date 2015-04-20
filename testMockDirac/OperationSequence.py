from types import StringTypes



class OperationSequence( object ):


  def __init__( self, fromDict = None ):
    """
    :param self: self reference
    :param dict fromDict: attributes dictionary
    """


    # self.who = None
    self.IDOpFile = None
    self.Order = None
    self.Depth = None

    for key, value in fromDict.items():
      if type( value ) in StringTypes:
        value = value.encode()

      if value:
        setattr( self, key, value )
