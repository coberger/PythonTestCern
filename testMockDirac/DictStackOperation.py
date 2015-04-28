from threading          import Lock
from StackOperation     import StackOperation

class DictStackOperation :
  """ contains all stackOperation needed by different thread"""

  dict = dict()
  # lock for multi-threading
  lock = Lock()


  def __init__( self ):
    pass


  @classmethod
  def getStackOperation( cls, threadID ):
    """ return the StackOperation associated to the threadID
        :param threadID: if of the thread
    """
    cls.lock.acquire()
    if threadID not in cls.dict:
      cls.dict[threadID] = StackOperation()
    res = cls.dict[threadID]
    cls.lock.release()

    return res


