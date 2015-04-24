# from DIRAC
from DIRAC              import S_OK, gConfig, gLogger, S_ERROR
from OperationFile      import OperationFile
from Sequence           import Sequence

# from sqlalchemy
from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB
from sqlalchemy.orm import mapper, sessionmaker, relationship, backref




# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

sequenceTable = Table( 'Sequence', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   mysql_engine = 'InnoDB' )

mapper( Sequence, sequenceTable, properties = { 'operations' : relationship( OperationFile ) } )

operationFileTable = Table( 'OperationFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'CreationTime', DateTime ),
                   Column( 'name', String( 255 ) ),
                   Column( 'Who', String( 255 ) ),
                   Column( 'Status', Enum( 'Done', 'Failed' ), server_default = 'Failed' ),
                   Column( 'lfn', String( 255 ) ),
                   Column( 'srcSE', String( 255 ) ),
                   Column( 'dstSE', String( 255 ) ),
                   Column( 'blob', String( 2048 ) ),
                   Column( 'parent_id', Integer, ForeignKey( 'OperationFile.ID' ) ),
                   Column( 'sequence_id', Integer, ForeignKey( 'Sequence.ID' ) ),
                   Column( 'order', Integer ),
                   # Column( 'parent_id', Integer ),
                   mysql_engine = 'InnoDB' )

mapper( OperationFile, operationFileTable  , properties = { 'children' : relationship( OperationFile ),
                                                            'sequence' : relationship( Sequence ) } )


class DataBase( object ):

  def __init__( self, systemInstance = 'Default' ):


    self.engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB', echo = False )

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

      return S_OK( operationFile )

    except Exception, e:
      session.rollback()
      gLogger.error( 'impossible to insert into table OperationFile' )
      return S_ERROR( "putOperationFile: unexpected exception %s" % e )
    finally:
      session.close()


  def putSequence( self, sequence ):
    session = self.DBSession( expire_on_commit = True )
    try:

      # operationFile = session.merge( operationFile )
      session.add( sequence )
      session.commit()
      session.expunge_all()

      return S_OK( sequence )

    except Exception, e:
      session.rollback()
      gLogger.error( "putSequence: unexpected exception %s" % e )
      return S_ERROR( "putSequence: unexpected exception %s" % e )
    finally:
      session.close()



