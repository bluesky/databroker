__author__ = 'arkilic'
import tornado.web, tornado.ioloop
import motor

class NewMessageHandler(tornado.web.RequestHandler):
    def get(self):
        """Show a 'compose message' form."""
        self.write('''
        <form method="post">
            <input type="text" name="msg">
            <input type="submit">
        </form>''')

    # Method exits before the HTTP request completes, thus "asynchronous"
    @tornado.web.asynchronous
    def post(self):
        """Insert a message."""
        msg = self.get_argument('msg')

        # Async insert; callback is executed when insert completes
        self.settings['db'].messages.insert(
            {'msg': msg},
            callback=self._on_response)

    def _on_response(self, result, error):
        if error:
            raise tornado.web.HTTPError(500, error)
        else:
            self.redirect('/')


class MessagesHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        """Display all messages."""
        self.write('<a href="/compose">Compose a message</a><br>')
        self.write('<ul>')
        db = self.settings['db']
        db.messages.find().sort([('_id', -1)]).each(self._got_message)

    def _got_message(self, message, error):
        if error:
            raise tornado.web.HTTPError(500, error)
        elif message:
            self.write('<li>%s</li>' % message['msg'])
        else:
            # Iteration complete
            self.write('</ul>')
            self.finish()

db = motor.MotorClient().test

application = tornado.web.Application(
    [
        (r'/compose', NewMessageHandler),
        (r'/', MessagesHandler)
    ],
    db=db
)

print('Listening on http://localhost:8888')
application.listen(8888)
tornado.ioloop.IOLoop.instance().start()
