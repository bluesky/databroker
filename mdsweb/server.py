from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
from tornado import gen
import simplejson as json
import urllib
import motor

__author__ = 'arkilic'

def db_connect(database ,host, port):
    """Helper function to deal with stateful connections to motor. Connection established lazily.
    Asycnc so do not treat like mongonengine connection pool.
    Tornado needs both client and database so this routine returns both. Once we figure out how to use tornado and motor
    properly, we may need to fix this.

    Parameters
    ----------
    database: str
        The name of the database that data is stored
    host: str
        Name/address of the server that mongo daemon lives
    port: int
        Port num of the server

    Returns
    -------
    db: motor.MotorDatabase
        Async database object
    """
    client = motor.MotorClient()
    db = client[database]
    return db


class RunStartHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        db = self.settings['db']
        try:
            data = json.loads(self.request.body)
        except:
            data = json.loads(urllib.unquote_plus(self.request.body))
        msg = {"lioncub":"gus"}
        result = yield db.messages.insert({'msg': msg})
        self.write("success")


db = db_connect("test2", '127.0.0.1', 27017) #TODO: Replace with configured one
application = tornado.web.Application([
    (r'/run_start', RunStartHandler)], db=db)
application.listen(7777)
tornado.ioloop.IOLoop.instance().start()