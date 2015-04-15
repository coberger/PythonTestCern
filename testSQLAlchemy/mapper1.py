from sqlalchemy import *
from sqlalchemy.orm import mapper, sessionmaker

db = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB' )

# db.echo = True

metadata = MetaData( db )

users = Table( 'users', metadata, autoload = True )
emails = Table( 'emails', metadata, autoload = True )

# These are the empty classes that will become our data classes
class User( object ):
    pass
class Email( object ):
    pass


def run( stmt ):
    rs = stmt.execute()
    for row in rs:
        print row


usermapper = mapper( User, users )
emailmapper = mapper( Email, emails )

Session = sessionmaker( bind = db )
session = Session()

s = users.select( users.c.name == 'John' )
run( s )

print ''
print 'fred'

fred = User()
fred.name = 'Fred'
fred.age = 37

print "About to commit() without an add()..."
session.commit()  # Will *not* save Fred's data yet
s = users.select( users.c.name == 'Fred' )
run( s )


session.add( fred )
session.commit()  # Now Fred's data will be saved

s = users.select( users.c.name == 'Fred' )
run( s )

session.delete( fred )
session.flush()


