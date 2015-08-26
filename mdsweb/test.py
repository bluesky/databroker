__author__ = 'arkilic'
from mdsweb.database.commands import db_connect
from tornado.ioloop import IOLoop

def my_callback(result, error):
    print('result %s' % repr(result))
    IOLoop.instance().stop()


#async insert example. This sends the document to database and returns prior to write.
#if succeeded, result is printed and error is None. If error, raises Exception
#this should help individual write speed ;)
document = {"key": 1}
client, conn = db_connect('test', '127.0.0.1', 27017)

conn.test_collection.insert(document, callback=my_callback)
IOLoop.instance().start()