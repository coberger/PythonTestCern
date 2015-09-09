import time
import sqlite3

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey, Table, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker, relationship, mapper

DBSession = None
engine = None

metadata = MetaData()

class DLSequence( object ):
  def __init__( self, un ):
    self.userName = un
    self.extra = dict()

  def __setattr__( self, name, value ):
    if hasattr( self, name ):
      super( DLSequence, self ).__setattr__( name, value )
    elif name in ['_sa_instance_state', 'userName', 'extra'] :
      super( DLSequence, self ).__setattr__( name, value )
    else :
      self.extra[name] = value


class DLSequenceAttribute( object ):
  def __init__( self, name ):
    self.name = name

class DLSequenceAttributeValue( object ):
  def __init__( self, value ):
    self.value = value
    self.sequence = None
    self.sequenceAttribute = None

dataLoggingSequenceTable = Table( 'DLSequence', metadata,
                   Column( 'sequenceID', Integer, primary_key = True ),
                   Column( 'userName', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )
mapper( DLSequence, dataLoggingSequenceTable )

dataLoggingSequenceAttribute = Table( 'DLSequenceAttribute', metadata,
                   Column( 'sequenceAttributeID', Integer, primary_key = True ),
                   Column( 'name', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )
mapper( DLSequenceAttribute, dataLoggingSequenceAttribute )

dataLoggingSequenceAttributeValue = Table( 'DLSequenceAttributeValue', metadata,
                   Column( 'sequenceID', Integer, ForeignKey( 'DLSequence.sequenceID' ), primary_key = True ),
                   Column( 'sequenceAttributeID', Integer, ForeignKey( 'DLSequenceAttribute.sequenceAttributeID' ), primary_key = True ),
                   Column( 'value', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )
mapper( DLSequenceAttributeValue, dataLoggingSequenceAttributeValue,
                      properties = { 'sequence' : relationship( DLSequence ),
                                     'sequenceAttribute' : relationship( DLSequenceAttribute ) } )


def init_sqlalchemy():
    global engine
    global DBSession
    engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB', echo = True )
    metadata.bind = engine
    DBSession = sessionmaker( bind = engine, autoflush = False, expire_on_commit = False )
    metadata.create_all( engine )


def getOrCreate( session, model, value ):
    if value is None :
      return S_OK( None )
    else:
      instance = session.query( model ).filter_by( name = value ).first()
      if not instance:
        instance = model( value )
        session.add( instance )
        session.commit()
      session.expunge( instance )
    return  instance

def test_insert():
    init_sqlalchemy()
    ses = DBSession()
    seq = DLSequence( 'toto' )
    seq.jobID = 17
    for key, value in seq.extra.items():
      sav = DLSequenceAttributeValue( value )
      sav.sequence = seq
      sav.sequenceAttribute = getOrCreate( ses, DLSequenceAttribute, key )

    ses.merge( seq )
    ses.commit()


if __name__ == '__main__':
    test_insert()

