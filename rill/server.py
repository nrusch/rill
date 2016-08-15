from rill.engine.utils import patch
patch()

import json
from collections import OrderedDict
import argparse
import logging
import gevent
import geventwebsocket

from rill.plumbing import Client, Message
from rill.runtime import DEFAULTS


class WebSocketRuntimeApplication(geventwebsocket.WebSocketApplication):
    """
    Web socket application that hosts a single ``Runtime`` instance.
    An instance of this class receives messages over a websocket, delegates
    message payloads to the appropriate ``Runtime`` methods, and sends
    responses where applicable.
    Message structures are defined by the FBP Protocol.
    """

    def __init__(self, ws):
        print('init')
        super(WebSocketRuntimeApplication, self).__init__(ws)

        self.logger = logging.getLogger('{}.{}'.format(
            self.__class__.__module__, self.__class__.__name__))

    # WebSocketApplication overrides --

    @staticmethod
    def protocol_name():
        """
        WebSocket sub-protocol
        """
        return 'noflo'

    def on_open(self):
        print 'im open yo'
        self.client = Client(self.on_response)
        self.client.connect("tcp://localhost", 5556)

    def on_close(self, reason):
        print('im closed', reason)
        self.client.disconnect()
        self.client = None

    def on_message(self, message, **kwargs):
        self.logger.debug('INCOMING: {}'.format(message))
        print('got message', message)
        if message:
            self.client.send(Message(**json.loads(message)))

    def on_response(self, msg):
        self.logger.debug("OUTCOMING: %r" % msg)
        self.ws.send(json.dumps(msg.to_dict()))


def websocket_application_task():
    """
    This greenlet runs the websocket server that responds to remote commands
    that inspect/manipulate the Runtime.
    """
    host = DEFAULTS['host']
    port = DEFAULTS['port']
    address = 'ws://{}:{:d}'.format(host, port)

    print('Runtime listening at {}'.format(address))
    r = geventwebsocket.Resource(
        OrderedDict([('/', WebSocketRuntimeApplication)]))
    server = geventwebsocket.WebSocketServer(('', port), r)
    server.serve_forever()


if __name__ == "__main__":
    gevent.wait([gevent.spawn(websocket_application_task)])

