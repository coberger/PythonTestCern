import inspect, functools, types

from threading import Thread, current_thread, Lock






class DictCaller :
  """ contains all stackOperation needed by different thread"""

  dict = dict()
  lock = Lock()

  def __init__( self ):
    pass

  @classmethod
  def getCaller( cls, threadID ):
    """ return the StackOperation associated to the threadID
        :param threadID: if of the thread
    """
    cls.lock.acquire()
    if threadID not in cls.dict :
      res = None
    else :
      res = cls.dict[threadID]
    cls.lock.release()

    return res

  @classmethod
  def setCaller( cls, threadID, caller ):
    """ return the StackOperation associated to the threadID
        :param threadID: if of the thread
    """
    cls.lock.acquire()
    if threadID not in cls.dict:
      cls.dict[threadID] = caller
    cls.lock.release()


def caller_name( skip = 2 ):
  """Get a name of a caller in the format module.class.method

     `skip` specifies how many levels of stack to skip while getting caller
     name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.

     An empty string is returned if skipped levels exceed stack height
  """
  stack = inspect.stack()
  start = 0 + skip
  if len( stack ) < start + 1:
    return ''
  parentframe = stack[start][0]
  name = []
  module = inspect.getmodule( parentframe )
  if module:
      name.append( module.__name__ )

  if 'self' in parentframe.f_locals:
      # I don't know any way to detect call from the object method
      # XXX: there seems to be no way to detect static method call - it will
      #      be just a function call
      name.append( parentframe.f_locals['self'].__class__.__name__ )
  codename = parentframe.f_code.co_name
  if codename != '<module>':  # top level usually
      name.append( codename )  # function or a method
  del parentframe
  return ".".join( name )




class Decorator0( object ):
  module_name = None;
  def __init__( self, fonc, ):

    self.fonc = fonc
    functools.wraps( fonc )( self )
    self.caller = caller_name()

  def __get__( self, inst, owner = None ):
    return types.MethodType( self, inst )

  def __call__( self, *args, **kwargs ):
    # call of the fonc
    print ' call ', self.fonc.__name__
    caller = DictCaller.getCaller( current_thread().ident )
    if not caller :
       DictCaller.setCaller( current_thread().ident, caller_name() )
    print 'decorate function  ' , current_thread().ident
    print DictCaller.getCaller( current_thread().ident )
    result = self.fonc( *args, **kwargs )

    return result



#===============================================================================
# class Decorator1( object ):
#
#   def __init__( self, moduleName = None ):
#     self.module_name = moduleName
#
#   def __get__( self, inst, owner = None ):
#     return types.MethodType( self, inst )
#
#   def __call__( self, f ):
#     def wrapped_f( *args ):
#       if self.module_name :
#         DictCaller.setCaller( current_thread().ident, self.module_name )
#       f( *args )
#     return wrapped_f
#===============================================================================



def Decorator2( caller = None ):
  class ClassWrapper:
    def __init__( self, cls ):
        self.other_class = cls

    def __call__( self, *cls_args ):
      other = self.other_class( *cls_args )

      if caller :
        print 'decorator class : ', current_thread().ident
        DictCaller.setCaller( current_thread().ident, caller )

        other.caller = caller
      return other
  return ClassWrapper



class DataManager( object ) :
  def __init__( self ):
    pass

  @Decorator0
  def putFile( self, lfn ):
    print 'putFile ', lfn


class ClientB( Thread ) :
  def __init__( self ):
    Thread.__init__( self )


  # @Decorator2( moduleName = 'totototo ' )
  def do( self ):
    print 'thread : ', current_thread().ident
    dm = DataManager()
    dm.putFile( 'lfnnnnnn' )


  def run( self ):
    self.do()


@Decorator2( caller = 'totototototo' )
class ClientA( Thread ) :
  def __init__( self ):
    Thread.__init__( self )
    self.caller = None
    print self.ident

  def do( self ):
    dm = DataManager()
    dm.putFile( 'lfnnnnnn' )

  def run( self ):
    self.do()

c0 = ClientA()
# c1 = ClientB()

print 'class of client A : ', c0.__class__

c0.start()
# c1.start()

c0.join()
# c1.join()



