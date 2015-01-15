__author__ = 'arkilic'

from fileStore.api.analysis import find, find_last
import time

start = time.time()
base = find_last()
end = time.time()

print "Query time to get the last FileBase insert...: ", str((end-start)*1000) , ' milliseconds'

print base.id