from DIRAC              import S_OK, S_ERROR


class StackOperation :
  """
  This class permit to have the name, the order and the depth of each operation on files
  """

  def __init__( self ):
    """
    :param self: self reference
    """
    self.stack = list()
    self.order = 0
    self.depth = 0
    self.sequence_id = 0
    self.parent = None


  def appendOperation( self, operationName ):
    """
    :param self: self reference
    :param operationName: name of the operation to append in the stack
    append an operation into the stack
    """
    if  len( self.stack ) == 0:
      self.parent = None
      self.order = 0
      self.depth = 0
      self.sequence_id = None
    self.stack.append( operationName )
    self.order += 1
    self.depth += 1



  def popOperation( self ):
    """
    :param self: self reference
    Pop an operation from the stack
    """
    self.depth -= 1
    res = self.stack.pop()

    return res



  def setParent( self, par ):
    self.parent = par


  def getParent( self ) :
    return self.parent


  def isParentSet( self ):
    if not self.parent :
      return S_ERROR( "Parent not set" )
    else :
      return S_OK()
