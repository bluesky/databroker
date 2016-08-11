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


def report_error(code, status, m_str=''):
    """Compose and raise an HTTPError message"""
    fmsg = str(status) + ' ' + str(m_str)
    raise tornado.web.HTTPError(status_code=code, reason=fmsg)
