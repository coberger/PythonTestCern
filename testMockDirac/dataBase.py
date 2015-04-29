# from DIRAC
from DIRAC              import S_OK, gConfig, gLogger, S_ERROR
from OperationFile      import OperationFile
from Sequence           import Sequence
from LFN                import LFN
from StatusOperation    import StatusOperation

# from sqlalchemy
from sqlalchemy         import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB
from sqlalchemy.orm     import mapper, sessionmaker, relationship, backref




# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

lfnTable = Table( 'LFN', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True ),
                   mysql_engine = 'InnoDB' )

mapper( LFN, lfnTable )


statusOperationTable = Table( 'StatusOperation', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'IDOp', Integer, ForeignKey( 'OperationFile.ID' ) ),
                   Column( 'IDLFN', Integer, ForeignKey( 'LFN.ID' ) ),
                   Column( 'status', Enum( 'Successful', 'Failed' ), server_default = 'Failed' ),
                   mysql_engine = 'InnoDB' )

mapper( StatusOperation, statusOperationTable, properties = { 'lfn' : relationship( LFN ) } )


sequenceTable = Table( 'Sequence', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   mysql_engine = 'InnoDB' )

mapper( Sequence, sequenceTable, properties = { 'operations' : relationship( OperationFile ) } )


operationFileTable = Table( 'OperationFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'creationTime', DateTime ),
                   Column( 'name', String( 255 ) ),
                   Column( 'caller', String( 255 ) ),
                   Column( 'lfn', String( 255 ) ),
                   Column( 'srcSE', String( 255 ) ),
                   Column( 'dstSE', String( 255 ) ),
                   Column( 'blob', String( 2048 ) ),
                   Column( 'parent_id', Integer, ForeignKey( 'OperationFile.ID' ) ),
                   Column( 'sequence_id', Integer, ForeignKey( 'Sequence.ID' ) ),
                   Column( 'order', Integer ),
                   mysql_engine = 'InnoDB' )

mapper( OperationFile, operationFileTable  , properties = { 'children' : relationship( OperationFile ),
                                                            'sequence' : relationship( Sequence ),
                                                            'status': relationship( StatusOperation ) } )


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
      print 'createTables ', e
      return S_ERROR( e )
    return S_OK()


  def putOperationFile( self, operationFile ):
    """ put an operation file into database"""
    session = self.DBSession( expire_on_commit = True )
    try:
      session.add( operationFile )
      session.commit()

      return S_OK( operationFile )

    except Exception, e:
      session.rollback()
      gLogger.error( 'impossible to insert into table OperationFile' )
      return S_ERROR( "putOperationFile: unexpected exception %s" % e )
    finally:
      session.close()


  def putSequence( self, sequence ):
    """ put a sequence into database"""
    session = self.DBSession()
    sequence.stack.append( sequence.operations[0] )

    while len( sequence.stack ) != 0 :
      element = sequence.stack.pop()
      for el in element.status :
        res = self.putLFN( el.lfn , session )
        el.lfn = res['Value']

      for child in element.children :
        sequence.stack.append( child )

    try:

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



  def putLFN( self, lfn, session ):
    """ put a lfn into datbase
        if the lfn's name is already in data base
    """
    try:
      instance = session.query( LFN ).filter_by( name = lfn.name ).first()
      if not instance:
        instance = LFN( lfn.name )
        session.add( instance )
        session.commit()

      return S_OK( instance )

    except Exception, e:
      session.rollback()
      gLogger.error( "putLFN: unexpected exception %s" % e )
      return S_ERROR( "putLFN: unexpected exception %s" % e )

    finally:
      session.close



  def getLFNSequence(self, lfn):
    """
      get all sequence about a lfn's name
    """
    session = self.DBSession()
    try:
      operations = session.query( Sequence, OperationFile, StatusOperation ).join( OperationFile ).join( StatusOperation ).join( LFN ).filter( LFN.name == lfn ).all()
      for row in operations :
        print "%s %s %s %s %s %s %s" % (row.Sequence.ID, row.OperationFile.ID, row.OperationFile.creationTime,
                                       row.OperationFile.name, lfn, row.OperationFile.caller, row.StatusOperation.status )

    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close



  def getLFNOperation(self, lfn):
    """
      get all operation about a lfn's name
    """
    session = self.DBSession()
    try:
      operations = session.query( OperationFile, StatusOperation ).join( StatusOperation ).join( LFN ).filter( LFN.name == lfn ).all()
      print operations
      for row in operations :
        print "%s %s %s %s %s %s" % ( row.OperationFile.ID, row.OperationFile.creationTime,
                                       row.OperationFile.name, lfn, row.OperationFile.caller, row.StatusOperation.status )

    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.close










