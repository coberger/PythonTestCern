# from DIRAC
from DIRAC              import S_OK, gConfig, gLogger, S_ERROR
from OperationFile      import OperationFile
from OperationSequence  import OperationSequence

# from sqlalchemy
from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB, BigInteger, distinct
from sqlalchemy.orm import mapper, sessionmaker, relationship




# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()
operationFileTable = Table( 'OperationFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'CreationTime', DateTime ),
                   Column( 'Who', String( 255 ) ),
                   Column( 'Status', Enum( 'Done', 'Failed' ), server_default = 'Failed' ),
                   Column( 'LFN', String( 255 ) ),
                   Column( 'SESource', String( 255 ) ),
                   Column( 'SEDestination', String( 255 ) ),
                   Column( 'blob', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )

mapper( OperationFile, operationFileTable )


operationSequenceTable = Table( 'OperationSequence', metadata,
                    Column( 'ID', Integer, primary_key = True ),
                    Column( 'IDOpFile', Integer,
                            ForeignKey( 'OperationFile.ID', ondelete = 'CASCADE' ),
                            nullable = False ),
                    Column( 'Order', Integer ),
                    Column( 'Depth', Integer, primary_key = True ),
                   mysql_engine = 'InnoDB' )

mapper( OperationSequence, operationSequenceTable )

class DataBase( object ):

  def __init__( self, systemInstance = 'Default' ):


    self.engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB' )

    metadata.bind = self.engine
    self.DBSession = sessionmaker( bind = self.engine )


  def createTables( self ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      return S_ERROR( e )
    return S_OK()


  def putOperationFile( self, operationFile ):
    session = self.DBSession( expire_on_commit = True )
    try:
      # operationFile = session.merge( operationFile )
      session.add( operationFile )
      session.commit()
      # session.expunge_all()

      print operationFile.ID

      return S_OK( operationFile )

    except Exception, e:
      session.rollback()
      gLogger.error( 'impossible d inserer dans la table OperationFile' )
      return S_ERROR( "putRequest: unexpected exception %s" % e )
    finally:
      session.close()


  def putOperationSequence( self, operationSequence ):
    session = self.DBSession( expire_on_commit = True )
    try:

      # operationFile = session.merge( operationFile )
      session.add( operationSequence )
      session.commit()
      # session.expunge_all()

      return S_OK( operationSequence )

    except Exception, e:
      session.rollback()
      gLogger.error( 'impossible d inserer dans la table OperationSequence' )
      return S_ERROR( "putRequest: unexpected exception %s" % e )
    finally:
      session.close()

