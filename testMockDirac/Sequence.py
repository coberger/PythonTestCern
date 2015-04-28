

class Sequence( object ) :

  def __init__( self, operations ):
    self.stack = list()
    self.operations = list()

    self.stack.append( operations )
    while len( self.stack ) != 0 :

      element = self.stack.pop()
      element.sequence = self
      self.operations.append( element )
      for child in element.children :
        self.stack.append( child )
