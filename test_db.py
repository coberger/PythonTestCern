from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, BLOB, BigInteger, distinct
from sqlalchemy.orm import mapper,scoped_session, sessionmaker, relationship, contains_eager, aliased, joinedload
from sqlalchemy.ext.declarative import declarative_base




Base = declarative_base()

class Parent(Base):
    __tablename__ = 'parent'
    id = Column(Integer, primary_key=True)
    children = relationship("Child",lazy='joined')
    dog_id = Column(Integer, ForeignKey('dog.id'))
    dog2_id = Column(Integer, ForeignKey('dog.id'))
    cat_id = Column(Integer, ForeignKey('cat.id'))
    dog = relationship("Dog", foreign_keys = dog_id,lazy='joined')
    dog2 = relationship("Dog", foreign_keys = dog2_id,lazy='joined')
    cat = relationship("Cat",lazy='joined')


class Child(Base):
    __tablename__ = 'child'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('parent.id'))
    babies = relationship("Baby",lazy='joined')
    brother_id = Column(Integer, ForeignKey('child.id'))
    brother = relationship("Child",lazy='joined')

class Baby(Base):
    __tablename__ = 'baby'
    id = Column(Integer, primary_key=True)
    child_id = Column(Integer, ForeignKey('child.id'))
    
class Dog(Base):
    __tablename__ = 'dog'
    id = Column(Integer, primary_key=True)

class Cat(Base):
    __tablename__ = 'cat'
    id = Column(Integer, primary_key=True)

engine = create_engine( 'mysql://Dirac:corent@127.0.0.1/testDiracDB', echo =True )
DBSession = sessionmaker(engine )
Base.metadata.create_all(engine)

def insert():
  session = DBSession()
  p = Parent()
  c = Child()
  c1 = brother()
  b = Baby()
  d = Dog()
  d2 = Dog()
  c.babies.append(b)
  p.children.append(c)
  p.dog = d
  p.dog2 = d2
  session.add(p)
  session.commit()
  session.close()

def getFromDB():
  session = DBSession()
  d_alias = aliased(Dog)
  d2_alias = aliased(Dog)
  query = session.query( Parent )
  rows = query.all()
  session.expunge_all()
  session.close()
  return rows

insert()
rows = getFromDB()
for parent in rows:
  print "parent %s dog %s cat %s"%(parent.id, parent.dog.id if parent.dog else '',parent.cat.id if parent.cat else '')
  for child in parent.children:
    print "  child %s"%child.id
    for baby in child.babies:
      print "    baby %s"%baby.id
  
