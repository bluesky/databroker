__author__ = 'arkilic'


from filestore.api.collection import save_file_event_link, save_file_base, save_file_attributes


base = save_file_base(spec='some spec', file_path='/tmp/filestore/dummy/file/path', custom={'some_info': 'info'})

print base.id, base.file_path, base.spec

save_file_attributes(file_base=base, dtype='float32', shape='1000x1000')



save_file_event_link(file_base=base, event_id='54b59cf5fa44833081ba8282')
# Note:This is not a real id inside metadataStore.No need to blame Arman for referring to non-existent documents
