'''
Created on Sep 4, 2015

@author: coberger
'''

import random, time
import sys
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DataLogging.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DataLogging.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodName import DLMethodName

maxDuration = 1800  # 30mn

hostname = 'volhcb38.cern.ch'
portList = [ 9166]
ind = random.randint( 0, len( portList ) - 1 )
port = portList[ind]
servAddress = 'dips://volhcb38.cern.ch:9169/DataManagement/DataLoggingBisBisBis' 


randomMax = 100000
randomMethocall = 20

dictLong = {'files': '/lhcb/data/file', 'targetSE': '/SE/Target/se',
 'blob': 'physicalFile = blablablablablabla ,fileSize = 6536589', 'srcSE': '/SE/SRC/src'}

def makeSequence():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' + str( random.randint( 0, 20 ) ) )
  calls = []
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()

  files = []
  for x in range( 4 ):
    files.append( dictLong['files'] + str( random.randint( 0, randomMax ) ) + '.data' )

  sources = []
  for x in range( 4 ):
    sources.append( dictLong['srcSE'] + str( random.randint( 0, randomMax ) ) )

  targets = []
  for x in range( 4 ):
    targets.append( dictLong['targetSE'] + str( random.randint( 0, randomMax ) ) )

  for call in calls :
    for x in range( 2 ):
      call.addAction( DLAction( DLFile( files[x * 2] ) , 'Successful' ,
              DLStorageElement( sources[x * 2] ),
               DLStorageElement( targets[x * 2] ),
              dictLong['blob'], 'errorMessage', random.randint( 0, 1050 ) ) )
      call.addAction( DLAction( DLFile( files[x * 2 + 1 ] ) , 'Failed',
              DLStorageElement( sources[x * 2 + 1 ] ),
               DLStorageElement( targets[x * 2 + 1] ),
              dictLong['blob'], 'errorMessage', random.randint( 0, 1050 ) ) )
  return sequence


client = DataLoggingClient( url = servAddress )

for i in range( 3000000 ) :
  seq = makeSequence()
  res = client.insertSequence( seq, directInsert = True )
  if not res['OK']:
    print 'error %s' % res['Message']
