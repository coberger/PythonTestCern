def createur_de_decorateur_avec_arguments(decorator_arg1, decorator_arg2):
    print "createur_de_decorateur, appele une seule fois, arg :", decorator_arg1, decorator_arg2
 
    def mon_decorateur(func):
        print "mon_decorateur arg :", decorator_arg1, decorator_arg2
 
        def wrapped(function_arg1, function_arg2) :
            print ("wrapper :"
                  ""
                  "arg du deco {0} {1}\n"
                  "arg de la fonction {2} {3}"
                  .format(decorator_arg1, decorator_arg2,
                          function_arg1, function_arg2))
            return func(function_arg1, function_arg2)
 
        return wrapped
 
    return mon_decorateur
 
@createur_de_decorateur_avec_arguments("toto", "tata")
def fonction_decoree_avec_arguments(function_arg1, function_arg2):
    print ("function arg: {0}"
           " {1}".format(function_arg1, function_arg2))


fonction_decoree_avec_arguments("titi","tutu")

fonction_decoree_avec_arguments("titi","tutu")
