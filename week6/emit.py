'''
  Name: Hidde Hensel
  Studentnr: 6379176
  
  Name: Mike Trieu
  Studentnr: 6366295 / 10105093
  
  Emit function in this file is loaded in asteval interpreter
  emit_dict is set in the asteval symtable in rest_interface.py
'''

def emit(key, value):
    if key in emit_dict:
        emit_dict[key].append(value)
    else:
        emit_dict[key] = [value]