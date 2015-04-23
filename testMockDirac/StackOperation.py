from DIRAC                  import S_OK, S_ERROR

from OperationFile          import OperationFile
from OperationSequence      import OperationSequence
from dataBase               import DataBase


class StackOperation :
  """
  This class permit to have the name, the order and the depth of each operation on files
  """

  def __init__( self ):
    """
    :param self: self reference
    """
    self.stack = list()
    self.sequence_id = 0
    self.parent = None


  def appendOperation( self, operationName, args ):
    """
    :param self: self reference
    :param operationName: name of the operation to append in the stack
    append an operation into the stack
    """
    if  len( self.stack ) == 0 :
      self.parent = None
      self.sequence_id = None
    self.stack.append( OperationFile( args ) )



  def popOperation( self ):
    """
    :param self: self reference
    Pop an operation from the stack
    """
    if len( self.stack ) != 1 :
      self.stack[len( self.stack ) - 2].addChild( self.stack[len( self.stack ) - 1] )

    res = self.stack.pop()

    cpt = 0
    for child in res.children :
      child.order = cpt
      cpt += 1

    self.insertOperations( res )

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


  def putOperationFile( self, operationFile ):
    """ put an OperationFile into database """

    res = db.putOperationFile( operationFile )
    if not res["OK"]:
      gLogger.error( ' error' , res['Message'] )
      exit()
    return res


  def insertOperations( self, fileOperations ):
    """ insert in bloc into database a list of operation"""
    db = DataBase()
    db.createTables()
    db.putOperationFile( fileOperations )















