import time
import sqlite3

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey, Table, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker, relationship, mapper
from time import sleep

DBSession = None
engine = None

metadata = MetaData()

class User( object ):
  def __init__( self ):
    self.firstName = None
    self.lastName = None

userTable = Table( 'User', metadata,
                   Column( 'userID', Integer, primary_key = True ),
                   Column( 'firstName', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )
mapper( User, userTable )


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
    user = User()
    user.firstName = 'toto'
    ses.add( user )
    ses.commit()
    names = ['toto','titi']
    users = ses.query( User.firstName ,User  ).filter( User.firstName.in_(names) ).all()
    print users
    print users[1][1]

if __name__ == '__main__':
    test_insert()

