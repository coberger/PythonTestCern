# from DIRAC
from DIRAC      import S_OK, gConfig, gLogger, S_ERROR
from DIRAC.DataManagementSystem.Client.OperationFile import OperationFile

# from sqlalchemy
from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB, BigInteger, distinct
from sqlalchemy.orm import mapper, sessionmaker




# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()
operationFileTable = Table( 'OperationFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'CreationTime', DateTime ),
                   #============================================================
                   # Column( 'who', Integer,
                   #         ForeignKey( 'Who.ID', ondelete = 'CASCADE' ),
                   #         nullable = False ),
                   #============================================================
                   Column( 'Status', Enum( 'Done', 'Failed' ), server_default = 'Failed' ),
                   Column( 'LFN', String( 255 ), index = True ),
                   Column( 'SESource', String( 255 ) ),
                   Column( 'SEDestination', String( 255 ) ),
                   Column( 'blob', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )

mapper( OperationFile, operationFileTable )

class OperationFileDB( object ):

  def __init__( self, systemInstance = 'Default' ):


    self.engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB' )

    metadata.bind = self.engine
    self.DBSession = sessionmaker( bind = self.engine )


  def createTables( self, toCreate = None, force = False ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      return S_ERROR( e )
    return S_OK()


  def putOperationFile( self, operationFile ):
    session = self.DBSession( expire_on_commit = True )
    try:

      request = session.merge( operationFile )
      session.add( request )
      session.commit()
      session.expunge_all()

      return S_OK( operationFile )

    except Exception, e:
      session.rollback()
      gLogger.error( 'impossible d inserer dans la table' )
      return S_ERROR( "putRequest: unexpected exception %s" % e )
    finally:
      session.close()

