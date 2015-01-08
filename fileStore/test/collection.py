__author__ = 'arkilic'

from fileStore.api.collection import create_file_link, create_file_attributes, create_event_list


print create_file_link('a', 'b', 'c')

print create_file_attributes(shape='2000x2000', dtype="float32", total_bytes=193535 )