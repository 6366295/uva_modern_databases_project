def dbMap(doc):
    emit(doc, 1)
    emit(doc, 2)
    
def dbReduce():
    print(2)