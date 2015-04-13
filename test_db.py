from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB, BigInteger, distinct
from sqlalchemy.orm import mapper,scoped_session, sessionmaker

metadata = MetaData()
operationFileTable = Table( 'OperationFile', metadata,
                   Column( 'ID', Integer, primary_key = True ),
                   Column( 'CreationTime', DateTime ),
                   #Column( 'who', Integer,
                    #       ForeignKey( 'Who.ID', ondelete = 'CASCADE' ),
                     #      nullable = False ),
                   Column( 'Status', Enum( 'Done', 'Failed' ), server_default = 'Failed' ),
                   Column( 'LFN', String( 255 ), index = True ),
                   Column( 'SESource', String( 255 ) ),
                   Column( 'SEDestination', String( 255 ) ),
                   Column( 'blob', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )

engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB' )
metadata.bind = engine
DBSession = sessionmaker(engine )
try:
    metadata.create_all( engine )
except Exception, e:
	print "error", e


