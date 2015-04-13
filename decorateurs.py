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


sandwich()
