import tornado.web
import ujson
import pymongo.cursor


def unpack_params(handler):
    """Unpacks the queries from the body of the header
    Parameters
    ----------
    handler: tornado.web.RequestHandler
        Handler for incoming request to collection

    Returns dict
    -------
        Unpacked query in dict format.
    """
    if isinstance(handler, tornado.web.RequestHandler):
        return ujson.loads(list(handler.request.arguments.keys())[0])
    else:
        raise TypeError("Handler provided must be of tornado.web.RequestHandler type")


def return2client(handler, payload):
    """Dump the result back to client's open socket. No need to worry about package size
    or socket behavior as tornado handles this for us

    Parameters
    -----------
    handler: tornado.web.RequestHandler
        Request handler for the collection of operation(post/get)
    payload: dict, list
        Information to be sent to the client
    """
    if isinstance(payload, pymongo.cursor.Cursor):
            l = []
            for p in payload:
                del(p['_id'])
                l.append(p)
            handler.write(ujson.dumps(l))
    elif isinstance(payload, list):
            handler.write(ujson.dumps(list))

    elif isinstance(payload, dict):
        try:
            del(payload['_id'])
        except KeyError:
            pass
        handler.write(ujson.dumps(list(payload)))
    else:
        handler.write('[')
        d = next(payload)
        while True:
            try:
                # del(d['_id'])
                handler.write(ujson.dumps(d))
                d = next(payload)
                handler.write(',')
            except StopIteration:
                break
        handler.write(']')
    handler.finish()

def report_error(code, status, m_str=''):
    """Compose and raise an HTTPError message"""
    fmsg = status + str(m_str)
    raise tornado.web.HTTPError(code, fmsg)


def transmit_list(handler, t_list):
    """Encodes and transmits a list response on the wire"""
    handler.write(ujson.dumps(t_list))
    handler.finish()
