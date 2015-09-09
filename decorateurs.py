def pain(func):
    def wrapper():
        print "</''''''\>"
        func()
        print "<\______/>"
    return wrapper

def ingredients(func):
    def wrapper():
        print "#tomates#"
        func()
        print "~salade~"
    return wrapper

@pain
@ingredients
def sandwich(nourriture="--jambon--"):
    print nourriture


def decorator( func ):
  def wrapper():
    print 'Before call of function %s' % func.__name__
    func()
    print 'After call of function %s' % func.__name__
    print
  return wrapper

@decorator
def hello():
  print 'Hello world'


hello()
print hello

hello()
print hello

