'''
  Name: Hidde Hensel
  Studentnr: 6379176
  
  Name: Mike Trieu
  Studentnr: 6366295 / 10105093
  
  Map and reduce function for testing purposes
'''

def dbMap(doc):
    emit(doc, 1)
    emit(doc, 2)
    
def dbReduce(key, values):
    return sum(values)