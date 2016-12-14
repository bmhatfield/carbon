
internalStats = {}

def increment(stat, increase=1):
  try:
    internalStats[stat] += increase
  except KeyError:
    internalStats[stat] = increase

def max(stat, newval):
  try:
    if internalStats[stat] < newval:
      internalStats[stat] = newval
  except KeyError:
    internalStats[stat] = newval

def append(stat, value):
  try:
    internalStats[stat].append(value)
  except KeyError:
    internalStats[stat] = [value]

def statsSnapshot():
  return internalStats.copy()

def clearStats():
  internalStats.clear()
