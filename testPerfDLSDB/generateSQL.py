import os
import random
import datetime

nb_se = 1000
nb_file = 1000000
nb_caller = 20
nb_group = 20
nb_host = 20
nb_user = 20
nb_method = 30
nb_sequences = 15000000
# number of method calls per sequence
nb_method_calls = 5
min_nb_actions = 1
max_nb_actions = 10
print "SET FOREIGN_KEY_CHECKS = 0;"
print "SET UNIQUE_CHECKS = 0;"
print "SET AUTOCOMMIT = 0;"

def generate_sub_tables():
  print "START TRANSACTION;"
  for i in range(nb_caller):
    print "INSERT INTO DLCaller (name) VALUES ('%s');"%('caller' + str(i))
       
  for i in range(nb_group):
    print "INSERT INTO DLGroup (name) VALUES ('%s');"%('group' + str(i))
         
  for i in range(nb_user):
    print "INSERT INTO DLUserName (name) VALUES ('%s');"%('userName' + str(i))
         
  for i in range(nb_host):
    print "INSERT INTO DLHostName (name) VALUES ('%s');"%('hostName' + str(i))
       
  for i in range(nb_method):
    print "INSERT INTO DLMethodName (name) VALUES ('%s');"%('class.methodName' + str(i))
       
  for i in range(nb_se):
    print "INSERT INTO DLStorageElement (name) VALUES ('%s');"%('se' + str(i))
         
  for i in range(nb_file):
    print "INSERT INTO DLFile (name) VALUES ('%s');"%('File' + str(i)) 
  print "COMMIT;"
  

def generate_sequences():
  toPrint = True
  print "START TRANSACTION;"
  seq = "INSERT INTO DLSequence (callerID, groupID, userNameID, hostNameID) VALUES "
  mc= 'INSERT INTO DLMethodCall (creationTime, methodNameID, parentID, sequenceID, rank) VALUES'
  for i in range(nb_sequences) :
    toPrint = True
    seq += '(%s, %s, %s, %s),' %( random.randint(1,nb_caller),random.randint(1,nb_group),random.randint(1,nb_user),random.randint(1,nb_host))
    for j in range(nb_method_calls):
      mc += " ('%s', %s, %s, %s, %s)," %(datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint(1,30), 'null',  i+1 ,0 )
      for a in range(random.randint(1,10)) :
          actions +="(%s, %s, '%s', %s, %s, '%s', '%s', %s),"\
            %((cpt * 1000000*5)+(y*10000)+(w*5)+mc+1 , random.randint(1,nb_file), 'Successful',random.randint(1,nb_se),random.randint(1,nb_se),
              'extra','errorMessage',random.randint(1,999) )
      for x in range(4):
        mc += " ('%s', %s, %s, %s, %s),"\
         %(datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint(1,30), ((i+1)*nb_method_calls)+1, i+1 ,x)
    if (i % 1000) == 0 :
      seq = seq[:-1]
      seq += ';'
      mc = mc[:-1]
      mc += ';'
      print seq
      print mc
      print "COMMIT;"
      print "START TRANSACTION;"
      seq = "INSERT INTO DLSequence (callerID, groupID, userNameID, hostNameID) VALUES "
      mc= 'INSERT INTO DLMethodCall (creationTime, methodNameID, parentID, sequenceID, rank) VALUES'
      toPrint = False
  if toPrint :
    print seq
    print mc
  print "COMMIT;"

for cpt in range (15):
  mc= 'INSERT INTO DLMethodCall (creationTime, methodNameID, parentID, sequenceID, rank) VALUES'
  for  y in  range(999999):
      mc += " ('%s', %s, %s, %s, %s)," %(datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint(1,30), 'null',  cpt * 1000000 +y+1 ,0 )
      for x in range(4):
        mc += " ('%s', %s, %s, %s, %s)," %(datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint(1,30), (cpt * 1000000*5)+(y*5)+x+1, cpt * 1000000+y+1 ,x)
   
  mc += " ('%s', %s, %s, %s, %s)," %(datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint(1,30), 'null',  (cpt+1) * 1000000 ,0 )
  for x in range(3):
    mc += " ('%s', %s, %s, %s, %s)," %(datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint(1,30), ((cpt) * 1000000*5)+999999*5+x+1, (cpt+1) * 1000000 ,x  )
  mc += " ('%s', %s, %s, %s, %s);" %(datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint(1,30), ((cpt) * 1000000*5)+999999*5+4, (cpt+1) * 1000000 ,3 ) 
  print mc
  print "COMMIT;"
  print "START TRANSACTION;"


for cpt in range(3,15) :
  for  y in  range(100):
    actions = "INSERT INTO `DLAction` (`methodCallID`, `fileID`, status, `srcSEID`, `targetSEID`, extra, `errorMessage`, `errorCode`) VALUES"
    for w in range(10000):
      for mc in range(5) :
        for a in range(random.randint(1,10)) :
          actions +="(%s, %s, '%s', %s, %s, '%s', '%s', %s),"\
            %((cpt * 1000000*5)+(y*10000)+(w*5)+mc+1 , random.randint(1,nb_file), 'Successful',random.randint(1,nb_se),random.randint(1,nb_se),
              'extra','errorMessage',random.randint(1,999) )
    actions = actions[:-1]
    actions += ';'
    print actions
    print "COMMIT;"
    print "START TRANSACTION;"
  
   
print "SET FOREIGN_KEY_CHECKS = 1;"
print "SET UNIQUE_CHECKS = 1;"
print "SET AUTOCOMMIT = 1;"