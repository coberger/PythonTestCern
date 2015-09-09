class A( object ):

  def __init__( self ):
    self.x = 0
    self.extra = {}

  def __setattr__( self, name, value ):
    print "setattr %s %s" % ( name, value )
    if hasattr( self, name ):
      return super( A, self ).__setattr__( name, value )

    elif name in ['_sa_instance_state', 'userName', 'extra'] :
      super( A, self ).__setattr__( name, value )
    else :
      self.extra[name] = value


obj = A()
print dir( obj )
print obj.x
print obj.extra

obj.x = 2
print obj.x
print obj.extra

obj.y = 3
print obj.x
print obj.extra

