from __future__ import absolute_import, print_function

import os
import binascii
import copy
import uuid
import traceback
from posixpath import join

from typing import Union, Optional

import zmq.green as zmq
import zmq.green.eventloop.zmqstream as zmqstream
# Always serializes to bytes instead of unicode for zeromq compatibility
import zmq.utils.jsonapi as json
from zmq.utils.strtypes import bytes, unicode, asbytes


# If no server replies within this time, abandon request
GLOBAL_TIMEOUT = 4000  # msecs
# Number of servers we will talk to
SERVER_MAX = 2

SNAPSHOT_PORT_OFFSET = 0


def is_socket_type(socket, typ):
    if isinstance(socket, zmqstream.ZMQStream):
        return socket.socket.type == typ
    else:
        return socket.type == typ


def zpipe(ctx):
    """build inproc pipe for talking to threads

    mimic pipe used in czmq zthread_fork.

    Returns a pair of PAIRs connected via inproc
    """
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b


def dump(msg_or_socket):
    """Receives all message parts from socket, printing each frame neatly"""
    if isinstance(msg_or_socket, zmq.Socket):
        # it's a socket, call on current message
        msg = msg_or_socket.recv_multipart()
    else:
        msg = msg_or_socket
    print("----------------------------------------")
    for part in msg:
        print("[%03d]" % len(part), end=' ')
        is_text = True
        try:
            print(part.decode('ascii'))
        except UnicodeDecodeError:
            print(r"0x%s" % (binascii.hexlify(part).decode('ascii')))


class Message(object):
    """
    Holds the properties of a FBP message.

    This class is clever about updates to payloads to avoid reserializing
    data.
    """
    def __init__(self, protocol, command, payload, id=None, revision=None,
                 identity=None):
        """

        Parameters
        ----------
        protocol : bytes
        command : bytes
        payload : Any
        id : Optional[Union[uuid.UUID, bytes]]
        revision : Optional[int]
        identity : Optional[bytes]
        """
        self.protocol = protocol
        self.command = command
        self._raw_payload = None
        self._payload = payload
        if isinstance(id, uuid.UUID):
            self._id = id.bytes
        elif isinstance(id, bytes) or id is None:
            self._id = id
        else:
            raise TypeError("id must be UUID or byte string, got %s" %
                            type(id))
        self.revision = revision
        self._graph_id = None
        self.identity = identity

    def __repr__(self):
        s = "%s/%s" % (self.protocol, self.command)
        if self._payload is not None:
            s += ", %r" % self._payload
        elif self._raw_payload is not None:
            s += ", %s" % self._raw_payload

        if self._id is not None:
            s += ", id=%s" % uuid.UUID(bytes=self._id)
        if self.revision is not None:
            s += ", rev=%05d" % self.revision
        return s

    @classmethod
    def from_frames(cls, protocol, command, raw_payload, message_id,
                    revision=None):
        """
        Create a Message from received frames.

        >>> Message(*socket.recv_multipart())
        """
        parts = protocol.split('/')
        if len(parts) == 2:
            protocol, graph_id = parts
        else:
            protocol = protocol
            graph_id = None
        if revision is not None:
            revision = int(revision)
        msg = cls(protocol, command, None, message_id, revision)
        msg._raw_payload = raw_payload
        msg._graph_id = graph_id
        return msg

    @property
    def id(self):
        """
        message id

        Returns
        -------
        bytes
        """
        if self._id is None:
            self._id = uuid.uuid1().bytes
        return self._id

    @property
    def graph_id(self):
        if self._graph_id is None and self.protocol in {'network', 'graph'}:
            if 'graph' in self.payload:
                self._graph_id = self.payload['graph']
            elif self.protocol == 'graph' and 'id' in self.payload:
                self._graph_id = self.payload['id']
        return self._graph_id

    @property
    def payload(self):
        """
        deserialized payload
        """
        if self._payload is None:
            assert self._raw_payload is not None
            self._payload = json.loads(self._raw_payload)
        return self._payload

    @payload.setter
    def payload(self, payload):
        self._raw_payload = None  # reset
        self._payload = payload

    @property
    def raw_payload(self):
        """
        serialized payload

        Returns
        -------
        bytes
        """
        if self._raw_payload is None:
            assert self._payload is not None
            self._raw_payload = json.dumps(self._payload)
        return self._raw_payload

    @raw_payload.setter
    def raw_payload(self, payload):
        self._payload = None  # reset
        self._raw_payload = payload

    def replace(self, **kwargs):
        """
        create a copy of the current Message, replacing the specified
        attributes
        """
        msg = copy.copy(self)
        for attr, value in kwargs.items():
            setattr(msg, attr, value)
        return msg

    def to_dict(self):
        return {
            'protocol': self.protocol,
            'command': self.command,
            'payload': self.payload,
            'message_id': self.id
        }

    def sendto(self, socket, prefix=None):
        """
        Send this Message to a socket.

        Parameters
        ----------
        prefix : bytes
            Additional frame to insert at the beginning of the message. Usually
            used for the identity.
        """
        # For PUB sockets we add the graph id to the first frame for
        # subscriptions to match against
        # FIXME: always add the graph_id to the first frame, because this lets
        # us route the message without deserializing the payload.
        if is_socket_type(socket, zmq.PUB):
            assert self.graph_id is not None
            # FIXME: we may need to encode the graph_id if it's unicode
            key = join(self.protocol, self.graph_id)
        else:
            key = self.protocol

        frames = [
            bytes(key),
            bytes(self.command),
            self.raw_payload,
            self.id
        ]
        if prefix:
            frames.insert(0, prefix)

        if self.revision is not None:
            frames.append(bytes(self.revision))

        socket.send_multipart(frames)


class MessageDispatcher(object):
    """
    Utility class to simplify sending messages to the correct sockets.

    By encapsulating this functionality, it can be passed from the server to
    other classes.
    """
    def __init__(self, publish_socket, response_socket, revision=0):
        # current revision of runtime state. used to ensure sync between
        # snapshot and subsequent publishes
        self.revision = revision
        # sockets we're sending output changes on
        # publish update to all clients:
        self.publish_socket = publish_socket
        # respond to the client that issued the update (e.g. for errors):
        self.response_socket = response_socket

    def send_info(self, protocol, command, payload):
        """
        Create a message with the current revision and send it to all clients.

        Messages sent via `send_info` originate on the runtime.  They
        represent ephemeral data.

        For message that resulted in state changes, see `send_revision`.
        """
        msg = Message(
            protocol=protocol,
            command=command,
            payload=payload,
            revision=self.revision)
        msg.sendto(self.publish_socket)

    def send_revision(self, msg):
        """
        Given a client message that has been successfully applied to the
        runtime state, increment the revision, add it to the message, and
        propagate it to all clients (including the originator of the
        message).

        Messages sent via `send_revision` are expected to not have originated
        from a client, and represent a change of state.

        Parameters
        ----------
        msg : Message
        """
        assert msg.identity is not None
        self.revision += 1
        msg.revision = self.revision
        # re-publish to all clients with a revision number
        # print("Re-publishing with key %r" % key)
        msg.sendto(self.publish_socket)

    def send_error(self, failed_msg, err, trace=None):
        """
        Send an error back to the client.

        Parameters
        ----------
        failed_msg : Message
        err : Exception
        trace : str
        """
        assert failed_msg.identity is not None
        msg = Message(
            protocol=failed_msg.protocol,
            command=b'error',
            payload={
                'message': err.message,
                'stack': trace or traceback.format_exc(),
                'request_id': failed_msg.id
            },
            revision=self.revision)
        msg.sendto(self.response_socket, failed_msg.identity)


if __name__ == '__main__':
    def run_server():
        from rill.runtime.core import Runtime
        from rill.runtime.server import RuntimeServer
        runtime = Runtime()
        runtime.register_module('rill.components.merge')
        runtime.register_module('tests.components')
        client = RuntimeServer(runtime)
        client.start()


    def run_client():
        import gevent
        from rill.runtime.client import RuntimeClient
        import uuid
        def on_recv(msg):
            print("RECV: %r" % msg)

        # Create and connect client
        client = RuntimeClient(on_recv)
        # client.graph = b''
        client.connect("tcp://localhost", 5556)
        # client.connect("tcp://localhost", 5566)
        print("done connecting")
        from tests.test_runtime import get_graph
        graph_id = str(uuid.uuid1())
        graph_name = 'My Graph'
        graph = get_graph(graph_name)[0]

        print("sending graph")
        for command, payload in get_graph_messages(graph, graph_id):
            client.send(Message(b'graph', command, payload))
        gevent.joinall([client.agent])


    import sys
    if sys.argv[1] == 'server':
        run_server()
    else:
        run_client()
