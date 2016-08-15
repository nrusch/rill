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

    argp = argparse.ArgumentParser(
        description='Rill websocket server forwards messages through zermq '
                    'to rill runtime')
    argp.add_argument(
        '--host', default=DEFAULTS['host'], metavar='HOSTNAME',
        help='Listen host for websocket (default: %(host)s)' % DEFAULTS)
    argp.add_argument(
        '--port', type=int, default=DEFAULTS['port'], metavar='PORT',
        help='Listen port for websocket (default: %(port)d)' % DEFAULTS)

    args = argp.parse_args()

    host = args.host
    port = args.port
    address = 'ws://{}:{:d}'.format(host, port)

    print('Runtime listening at {}'.format(address))
    r = geventwebsocket.Resource(
        OrderedDict([('/', WebSocketRuntimeApplication)]))
    server = geventwebsocket.WebSocketServer(('', port), r)
    server.serve_forever()


def main():
    gevent.wait([gevent.spawn(websocket_application_task)])

if __name__ == "__main__":
    main()
