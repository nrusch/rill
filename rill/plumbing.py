#None based on The Clustered Hashmap Protocol: http://rfc.zeromq.org/spec:12/CHP/

from __future__ import print_function
import os
import logging
import time
import binascii
import functools
import datetime
import copy
import uuid
import traceback

import gevent
import zmq.green as zmq
from zmq.green.eventloop.ioloop import IOLoop, PeriodicCallback
import zmq.green.eventloop.zmqstream as zmqstream
import zmq.utils.jsonapi as json
from zmq.utils.strtypes import bytes, unicode, asbytes

from posixpath import join

from rill.events.listeners.memory import get_graph_messages
from rill.engine.exceptions import FlowError


# If no server replies within this time, abandon request
GLOBAL_TIMEOUT = 4000  # msecs
# Server considered dead if silent for this long
SERVER_TTL = 5.0  # secs
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
    def __init__(self, protocol, command, payload, id=None, revision=None):
        self.protocol = protocol
        self.command = command
        self._raw_payload = None
        self._payload = payload
        self._id = id
        self.revision = revision
        self.graph_id = None

    def __repr__(self):
        s = "%s/%s" % (self.protocol, self.command)
        if self._payload is not None:
            s += ", %r" % self._payload
        elif self._raw_payload is not None:
            s += ", %s" % self._raw_payload

        if self._id is not None:
            s += ", id=%s" % self._id
        if self.revision is not None:
            s += ", rev=%05d" % self.revision
        return s

    @classmethod
    def from_frames(cls, protocol, command, raw_payload, message_id, revision=None):
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
        msg.graph_id = graph_id
        return msg

    @property
    def id(self):
        """
        message id
        """
        if self._id is None:
            self._id = bytes(uuid.uuid1())
        return self._id

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
        """
        # For PUB sockets we add the graph id to the first frame for
        # subscriptions to match against
        if is_socket_type(socket, zmq.PUB) and (self.protocol in ['network', 'graph']):
            if self.graph_id is None:
                if 'graph' in self.payload:
                    self.graph_id = self.payload['graph']
                elif self.protocol == 'graph' and 'id' in self.payload:
                    self.graph_id = self.payload['id']
            assert self.graph_id is not None
            # FIXME: we may need to encode the graph_id if it's unicode
            key = join(self.protocol, self.graph_id)
        else:
            key = self.protocol

        frames = [
            bytes(key),
            bytes(self.command),
            bytes(self.raw_payload),
            bytes(self.id)
        ]
        if prefix:
            frames.insert(0, prefix)

        if self.revision is not None:
            frames.append(bytes(self.revision))

        socket.send_multipart(frames)


class Client(object):
    def __init__(self, on_recv):
        self.ctx = zmq.Context()
        # pipe to client agent
        self.pipe, peer = zpipe(self.ctx)
        # agent in a thread
        self.agent = gevent.spawn(client_agent_loop, self.ctx, peer, on_recv)
        # cache of our graph value
        self.graph = None

    def watch_graph(self, graph, message_id, sync=True):
        """
        Receive update messages for a graph.

        Sends [SUBSCRIBE][graph] to the agent
        """
        self.graph = graph
        # FIXME: we should probably just use json here
        self.pipe.send_multipart([
            b"SUBSCRIBE", bytes(graph), bytes(message_id), bytes(int(sync))])

    def connect(self, address, port):
        """
        Connect to new runtime server endpoint

        Sends [CONNECT][address][port] to the agent
        """
        self.pipe.send_multipart(
            [b"CONNECT",
             (address.encode() if isinstance(address, str) else address),
             b'%d' % port])

    def disconnect(self):
        self.agent.kill()

    def send(self, msg):
        """
        Send a message to the runtime

        Sends [SEND][proto][command][payload] to the agent
        """

        # FIXME: it would be great if we didn't need to deserialize the payload coming from the websocket

        # FIXME: handle removegraph
        if msg.protocol == 'graph' and msg.command in ('addgraph', 'clear'):
            # 'clear': creates a new graph or wipes an existing graph
            # 'addgraph': creates a new graph or fails if it exists
            print(msg.payload)
            # subscribe to the graph
            self.watch_graph(msg.payload['id'], message_id=msg.id, sync=False)
        elif (msg.protocol, msg.command) == ('graph', 'watch'):
            # subscribe to the graph: this will trigger a state sync
            self.watch_graph(msg.payload['id'], message_id=msg.id, sync=True)
            # no changes are required server-side: nothing else left to do.
            return

        logging.info("I: sending %s" % msg)
        msg.sendto(self.pipe, prefix=b'SEND')


class ClientConnection(object):
    """
    One connection from Client to RuntimeServer
    """
    expiry = 0  # Expires at this time
    requests = 0  # How many snapshot requests made?

    def __init__(self, ctx, address, port):
        # server address
        self.address = address
        # server port
        self.port = port

        # outgoing updates (responses for snapshots and errors)
        self.publisher = ctx.socket(zmq.DEALER)
        self.publisher.linger = 0
        self.publisher.connect("%s:%i" % (address.decode(), port))

        # Incoming updates from server (one-to-many)
        # NOTE:
        # Even if you synchronize a SUB and PUB socket, you may still lose
        # messages. It's due to the fact that internal queues aren't created
        # until a connection is actually created. If you can switch the
        # bind/connect direction so the SUB socket binds, and the PUB socket
        # connects, you may find it works more as you'd expect.
        self.subscriber = ctx.socket(zmq.SUB)
        # FIXME: add heartbeat
        # self.subscriber.setsockopt(zmq.SUBSCRIBE, b'HUGZ')
        self.subscriber.setsockopt(zmq.SUBSCRIBE, b'component')
        self.subscriber.connect("%s:%i" % (address.decode(), port + 1))
        self.subscriber.linger = 0

        # subscribed graph
        self.graph = None

    def watch_graph(self, graph, subtopics=(b'graph', b'network')):
        if self.graph is not None:
            for subtopic in subtopics:
                self.subscriber.setsockopt(zmq.UNSUBSCRIBE,
                                           join(subtopic, self.graph))
        print("Watching graph %r" % graph)
        self.graph = graph
        for subtopic in subtopics:
            self.subscriber.setsockopt(zmq.SUBSCRIBE, join(subtopic, graph))


# Client States
STATE_INITIAL = 0  # Before asking server for state
STATE_SYNCING = 1  # Getting state from server
STATE_ACTIVE = 2  # Getting new updates from server


class ClientAgent(object):
    """
    Background client agent
    """
    def __init__(self, ctx, pipe):
        self.ctx = ctx
        # socket to talk back to application
        self.pipe = pipe
        self.state = STATE_INITIAL
        # connected RuntimeServer
        self.connection = None
        # subscribed graph: used to trigger a subscription change
        self.graph = None
        # revision of last msg processed
        self.revision = 0

    def handle_message(self):
        msg = self.pipe.recv_multipart()
        command = msg.pop(0)

        if command == b"CONNECT":
            address = msg.pop(0)
            port = int(msg.pop(0))
            assert self.connection is None
            self.connection = ClientConnection(self.ctx, address, port)
        elif command == b"SEND":
            # push message to the server
            print("sending message to server")
            self.connection.publisher.send_multipart(msg)
        elif command == b"SUBSCRIBE":
            graph, message_id, sync = msg
            self.connection.watch_graph(graph)
            if bool(int(sync)):
                # trigger sync
                self.graph = graph
                self.message_id = message_id


def client_agent_loop(ctx, pipe, on_recv):
    agent = ClientAgent(ctx, pipe)
    conn = None

    while True:
        # poller for both the pipe and the active server
        poller = zmq.Poller()
        poll_timer = None

        # choose a server socket
        server_sockets = []
        if agent.state == STATE_INITIAL:
            # In this state we ask the server for a snapshot,
            if agent.connection:
                conn = agent.connection
                print("I: waiting for server at %s:%d..." % (conn.address, conn.port))
                # FIXME: why 2?  I think this may have to do with MAX_SERVER
                if conn.requests < 2:
                    Message(b'internal', b'startsync', b'').sendto(conn.publisher)
                    conn.requests += 1
                conn.expiry = time.time() + SERVER_TTL
                print("switching to sync state")
                agent.state = STATE_SYNCING
                server_sockets = [conn.publisher]
        elif agent.state == STATE_SYNCING:
            # In this state we read from snapshot and we expect
            # the server to respond.
            server_sockets = [conn.publisher]
        elif agent.state == STATE_ACTIVE:
            if agent.graph:
                print("switching to graph sync state")
                Message(
                    'internal', 'startsync', agent.graph, agent.message_id
                ).sendto(conn.publisher)
                # wipe the graph subscription request so that we don't get
                # here unless the graph has changed
                agent.graph = None
                agent.message_id = None
                conn.expiry = time.time() + SERVER_TTL
                agent.state = STATE_SYNCING
                server_sockets = [conn.publisher]
            else:
                # In this state we read from subscriber.
                server_sockets = [conn.subscriber, conn.publisher]

        # we don't process messages from the client until we're done syncing.
        if agent.state != STATE_SYNCING:
            poller.register(agent.pipe, zmq.POLLIN)
        if len(server_sockets):
            # we have a second socket to poll:
            for server_socket in server_sockets:
                poller.register(server_socket, zmq.POLLIN)

        if conn is not None:
            poll_timer = 1e3 * max(0, conn.expiry - time.time())

        # ------------------------------------------------------------
        # Poll loop
        try:
            items = dict(poller.poll(poll_timer))
        except:
            raise  # DEBUG
            break  # Context has been shut down

        if len(items.keys()):
            for socket in items.keys():
                if socket is agent.pipe:
                    print("Control message")
                    agent.handle_message()
                else:
                    server_socket = socket
                    print("Server message")
                    msg = Message.from_frames(*server_socket.recv_multipart())
                    # Anything from server resets its expiry time
                    conn.expiry = time.time() + SERVER_TTL
                    if agent.state == STATE_SYNCING:
                        conn.requests = 0
                        if (msg.protocol, msg.command) == ('internal', 'endsync'):
                            # done syncing
                            assert isinstance(msg.payload, int)
                            agent.revision = msg.payload
                            print("switching to active state")
                            agent.state = STATE_ACTIVE
                            logging.info("I: received from %s:%d snapshot=%d",
                                         conn.address, conn.port, agent.revision)
                            # FIXME: send componentsready?
                            # self.send('component', 'componentsready')
                        else:
                            logging.info("I: received from %s:%d %s %d",
                                conn.address, conn.port, msg, agent.revision)
                            on_recv(msg)

                    elif agent.state == STATE_ACTIVE:
                        # Receive message published from server.
                        # Discard out-of-revision updates, incl. hugz
                        print("msg %r" % msg)
                        if (
                            msg.revision > agent.revision or
                            msg.command == 'error' or
                            msg.command == 'log' or
                            msg.protocol == 'component'
                        ):
                            agent.revision = msg.revision

                            on_recv(msg)

                            logging.info("I: received from %s:%d %s",
                                         conn.address, conn.port, msg)
                        else:
                            print("Sequence is too low: %d < %d" %
                                    (msg.revision, agent.revision))
                            # if kvmsg.key != b"HUGZ":
                            #     logging.info("I: received from %s:%d %s=%d %s",
                            #                  server.address, server.port, 'UPDATE',
                            #                  agent.revision, kvmsg.key)
                    else:
                        raise RuntimeError("This should not be possible")
        else:
            gevent.sleep(0)
        # FIXME: add heartbeat back?
        # else:
        #     # Server has died, failover to next
        #     print("I: server at %s:%d didn't give hugz" % (server.address, server.port))
        #     agent.cur_server = (agent.cur_server + 1) % len(agent.connections)
        #     agent.state = STATE_INITIAL


class RuntimeHandler(object):
    """
    Utility class for processing messages into changes to a Runtime
    """
    def __init__(self, runtime, socket, responder):
        from rill.engine.component import _logger, RuntimeComponentHandler
        _logger.addHandler(RuntimeComponentHandler(self))

        self.runtime = runtime
        # current revision of runtime state. used to ensure sync between
        # snapshot and subsequent publishes
        self.revision = 0
        # socket we're sending output changes on
        self.socket = socket
        self.responder = responder
        self.logger = logging.getLogger('{}.{}'.format(
            self.__class__.__module__, self.__class__.__name__))

    def send_revision(self, msg):
        """
        Increment the revision, add it to the message, and send it on
        `self.socket`

        Parameters
        ----------
        msg : Message
        """
        self.revision += 1
        msg.revision = self.revision
        # re-publish to all clients with a revision number
        # print("Re-publishing with key %r" % key)
        msg.sendto(self.socket)

    def send_log_record(self, record):
        if getattr(record, 'graph', False):
            Message(
                protocol='network',
                command='log',
                payload={
                    'graph': record.graph,
                    'message': record.msg % record.args
                },
                revision=self.revision
            ).sendto(self.socket)

    def handle_message(self, msg, identity):
        """
        Main entry point for handing a message

        Parameters
        ----------
        msg : Message
        """
        from rill.runtime import RillRuntimeError

        dispatch = {
            # 'runtime': self.handle_runtime,
            # 'component': self.handle_component,
            'graph': self.handle_graph,
            'network': self.handle_network
        }
        print("--IN--")
        print(repr(msg))

        # FIXME: use the json-schema files from FBP protocol to validate
        # message structure
        try:
            handler = dispatch[msg.protocol]
        except KeyError:
            # FIXME: send error?
            self.logger.warn("Subprotocol '{}' "
                             "not supported".format(msg.protocol))
            return

        try:
            handler(msg)
        except (FlowError, RillRuntimeError) as err:
            self.send_error(msg.protocol, err, msg.id, identity)

    # Utilities --

    # def send(self, protocol, command, payload, message_id=None):
    #     """
    #     Send a message to UI/client
    #     """
    #     message = {'protocol': protocol,
    #                'command': command,
    #                'payload': payload,
    #                'id': message_id or str(uuid.uuid4())}
    #     print("--OUT--")
    #     import pprint
    #     pprint.pprint(message)
    #     # FIXME: what do we do when the socket closes or is dead?
    #     try:
    #         self.ws.send(json.dumps(message))
    #     except geventwebsocket.WebSocketError as err:
    #         print(err)
    #
    # def send_error(self, protocol, message):
    #     data = {
    #         'message': message,
    #         'stack': traceback.format_exc()
    #     }
    #     self.send(protocol, 'error', data)

    # Protocol send/responses --

    # def handle_runtime(self, command, payload, message_id):
    #     # tell UI info about runtime and supported capabilities
    #     if command == 'getruntime':
    #         payload = self.runtime.get_runtime_meta()
    #         # self.logger.debug(json.dumps(payload, indent=4))
    #         self.send('runtime', 'runtime', payload)
    #
    #     # network:packet, allows sending data in/out to networks in this
    #     # runtime can be used to represent the runtime as a FBP component
    #     # in bigger system "remote subgraph"
    #     elif command == 'packet':
    #         # We don't actually run anything, just echo input back and
    #         # pretend it came from "out"
    #         payload['port'] = 'out'
    #         self.send('runtime', 'packet', payload)
    #
    #     else:
    #         self.logger.warn("Unknown command '%s' for protocol '%s' " %
    #                          (command, 'runtime'))

    # def handle_component(self, command, payload, message_id):
    #     """
    #     Provide information about components.
    #     Parameters
    #     ----------
    #     command : str
    #     payload : dict
    #     """
    #     if command == 'list':
    #         for spec in self.runtime.get_all_component_specs():
    #             self.send('component', 'component', spec)
    #
    #         self.send('component', 'componentsready', None)
    #     # Get source code for component
    #     elif command == 'getsource':
    #         raise TypeError("HEREREREREHRERERE")
    #         component_name = payload['name']
    #         source_code = self.runtime.get_source_code(component_name)
    #
    #         library_name, short_component_name = component_name.split('/', 1)
    #
    #         payload = {
    #             'name': short_component_name,
    #             'language': 'python',
    #             'library': library_name,
    #             'code': source_code,
    #             #'tests': ''
    #             'secret': payload.get('secret')
    #         }
    #         self.send('component', 'source', payload)
    #     else:
    #         self.logger.warn("Unknown command '%s' for protocol '%s' " %
    #                          (command, 'component'))

    def handle_graph(self, msg):
        """
        Modify our graph representation to match that of the UI/client

        Parameters
        ----------
        msg: Message
        """
        command = msg.command
        payload = msg.payload
        message_id = msg.id

        def get_graph():
            try:
                if command in ['clear', 'addgraph']:
                    return payload['id']
                else:
                    return payload['graph']

            except KeyError:
                raise FlowError('No graph specified')

        # def update_subnet(graph_id):
        #     spec = self.runtime.register_subnet(graph_id)
        #     self.send(
        #         'component',
        #         'component',
        #         spec
        #     )

        # New graph
        send_component = False
        if command == 'clear':
            self.runtime.new_graph(
                payload['id'],
                payload.get('description', None),
                payload.get('metadata', None)
            )
        if command == 'addgraph':
            send_component = True
            self.runtime.new_graph(
                payload['id'],
                payload.get('description', None),
                payload.get('metadata', None),
                overwrite=False
            )
        # Nodes
        elif command == 'addnode':
            self.runtime.add_node(get_graph(), payload['id'],
                                  payload['component'],
                                  payload.get('metadata', {}))
        elif command == 'removenode':
            self.runtime.remove_node(get_graph(), payload['id'])
        elif command == 'renamenode':
            self.runtime.rename_node(get_graph(), payload['from'],
                                     payload['to'])
        # Edges/connections
        elif command == 'addedge':
            self.runtime.add_edge(get_graph(), payload['src'],
                                             payload['tgt'],
                                             payload.get('metadata', {}))
        elif command == 'removeedge':
            self.runtime.remove_edge(get_graph(), payload['src'],
                                     payload['tgt'])
        # IIP / literals
        elif command == 'addinitial':
            self.runtime.initialize_port(get_graph(), payload['tgt'],
                                         payload['src']['data'])
        elif command == 'removeinitial':
            self.runtime.uninitialize_port(get_graph(),
                                                 payload['tgt'])
        # Exported ports
        elif command in ('addinport', 'addoutport'):
            send_component = True
            self.runtime.add_export(get_graph(), payload['node'],
                                    payload['port'], payload['public'], payload['metadata'])
            # update_subnet(get_graph())
        elif command == 'removeinport':
            send_component = True
            self.runtime.remove_inport(get_graph(), payload['public'])
            # update_subnet(get_graph())
        elif command == 'removeoutport':
            send_component = True
            self.runtime.remove_outport(get_graph(), payload['public'])
            # update_subnet(get_graph())
        elif command == 'changeinport':
            self.runtime.change_inport(
                get_graph(), payload['public'], payload['metadata'])
        elif command == 'changeoutport':
            self.runtime.change_outport(
                get_graph(), payload['public'], payload['metadata'])
        elif command == 'renameinport':
            send_component = True
            self.runtime.rename_inport(
                get_graph(), payload['from'], payload['to'])
        elif command == 'renameoutport':
            send_component = True
            self.runtime.rename_outport(
                get_graph(), payload['from'], payload['to'])
        # Metadata changes
        elif command == 'changenode':
            self.runtime.set_node_metadata(get_graph(),
                                           payload['id'],
                                           payload['metadata'])
        elif command == 'changeedge':
            self.runtime.set_edge_metadata(get_graph(),
                                           payload['src'],
                                           payload['tgt'],
                                           payload['metadata'])
        elif command == 'addgroup':
            self.runtime.add_group(
                get_graph(),
                payload['name'],
                payload['nodes'],
                payload.get('metadata', {})
            )
        elif command == 'removegroup':
            self.runtime.remove_group(
                get_graph(),
                payload['name']
            )
        elif command == 'renamegroup':
            self.runtime.rename_group(
                get_graph(),
                payload['from'],
                payload['to']
            )
        elif command == 'changegroup':
            self.runtime.change_group(
                get_graph(),
                payload['name'],
                payload.get('nodes', None),
                payload.get('metadata', {})
            )

        # elif command == 'getgraph':
        #     send_ack = False
        #     graph_id = payload['id']
        #     try:
        #         graph = self.runtime.get_graph(graph_id)
        #         graph_messages = get_graph_messages(
        #             graph, graph_id)
        #         for command, payload in graph_messages:
        #             self.send('graph', command, payload)
        #     except FlowError as ex:
        #         self.runtime.new_graph(graph_id)
        #
        # elif command == 'list':
        #     send_ack = False
        #     for graph_id in self.runtime._graphs.keys():
        #         self.send('graph', 'graph', {
        #             'id': graph_id
        #         })
        #
        #     self.send('graph', 'graphsdone', None)

        elif command == 'changegraph':
            self.runtime.change_graph(
                get_graph(),
                payload.get('description', None),
                payload.get('metadata', None)
            )

        elif command == 'renamegraph':
            self.runtime.rename_graph(payload['from'], payload['to'])

        else:
            self.logger.warn("Unknown command '%s' for protocol '%s'" %
                             (command, 'graph'))
            # FIXME: quit? dump message?
            return

        self.send_revision(msg)
        if send_component:
            message = Message(
                protocol='component',
                command='component',
                payload=self.runtime._component_types[
                    'abc/{}'.format(get_graph())]['spec'],
                revision = self.revision
            )
            message.sendto(self.socket)

    def get_network_status(self, graph_id):
        started, running = self.runtime.get_status(graph_id)
        return {
            'graph': graph_id,
            'started': started,
            'running': running,
            'time': datetime.datetime.now().isoformat()
            # 'debug': True,
        }

    def send_network_status(self, msg, command):
        status = self.get_network_status(msg.payload['graph'])
        self.send_revision(msg.replace(command=command, payload=status))

    def send_network_data(self, connection, outport, inport, packet):
        edge_id = '{}.{}{} -> {}.{}{}'.format(
            outport.component.name, outport.name,
            '[{}]'.format(outport.index) if outport.index else '',
            inport.component.name, inport.name,
            '[{}]'.format(inport.index) if inport.index else '')

        msg = Message('network', 'data', {
            'src': {
                'node': outport.component.name,
                'port': outport.name,
                'index': outport.index
            },
            'tgt': {
                'node': inport.component.name,
                'port': inport.name,
                'index': inport.index
            },
            'id': edge_id,
            'graph': inport.component.network.graph.name
        })
        self.send_revision(msg)

    def handle_network(self, msg):
        """
        Start / Stop and provide status messages about the network.

        Parameters
        ----------
        msg: Message
        """

        command = msg.command
        payload = msg.payload
        graph_id = payload['graph']
        # FIXME: add message_id to started/stopped?
        # FIXME: change 'started'/'stopped' to 'status' for symmetry with handle_snapshot?
        # if command == 'getstatus':
        #     send_status('status', graph_id, timestamp=False)
        if command == 'start':
            callback = functools.partial(
                self.send_network_status, msg, 'stopped')
            self.runtime.start(graph_id, callback)
            reply = 'started'
        elif command == 'stop':
            self.runtime.stop(graph_id)
            reply = 'stopped'
        # elif command == 'debug':
        #     self.runtime.set_debug(graph_id, payload['enable'])
        #     self.send('network', 'debug', payload)
        else:
            self.logger.warn("Unknown command '%s' for protocol '%s'" %
                             (command, 'network'))
            # FIXME: quit? dump message?
            return

        self.send_network_status(msg, reply)

    def send_error(self, protocol, err, message_id, identity):
        Message(protocol, 'error', {
            'message': err.message,
            'stack': traceback.format_exc(),
            'request_id': message_id
        }, revision=self.revision).sendto(self.responder, identity)


class RuntimeServer(object):
    """
    Server managing the runtime state
    """

    def __init__(self, runtime, port=5556):
        self.runtime = runtime

        self.port = port
        # Context wrapper
        self.ctx = zmq.Context()
        # IOLoop reactor
        self.loop = IOLoop.instance()

        # Set up our client server sockets

        # Publish updates to clients
        self.publisher = self.ctx.socket(zmq.PUB)
        # Collect updates and snapshot requests from clients
        self.collector = self.ctx.socket(zmq.ROUTER)

        self.collector.bind("tcp://*:%d" % (self.port))
        self.publisher.bind("tcp://*:%d" % (self.port + 1))

        # Wrap sockets in ZMQStreams for IOLoop handlers
        self.publisher = zmqstream.ZMQStream(self.publisher)  # only necessary for heartbeat
        self.collector = zmqstream.ZMQStream(self.collector)

        # Register handlers with reactor
        self.collector.on_recv(self.handle_message)

        self.handler = RuntimeHandler(runtime, self.publisher, self.collector)

    def start(self):
        print("Server listening on port %d" % self.port)
        # Run reactor until process interrupted
        try:
            self.runtime.send_network_data.event.listen(
                self.handler.send_network_data)

            self.loop.start()

        except KeyboardInterrupt:
            pass

    def stop(self):
        self.runtime.send_network_data.event.remove_listener(
            self.handler.send_network_data)

        self.loop.stop()

    # def publish(self, key, command, payload, id, revision):
    #     self.publisher.send_multipart(
    #         [key, command, payload, id, bytes(revision)])

    def handle_message(self, msg):
        identity = msg.pop(0)
        msg = Message.from_frames(*msg)

        if msg.protocol == 'internal':
            self.handle_snapshot(msg, identity)
        else:
            self.handle_collect(msg, identity)

    def handle_snapshot(self, msg, identity):
        """snapshot requests"""
        from rill.runtime import RillRuntimeError

        print("handle_snapshot: %r" % msg)

        if (msg.protocol, msg.command) == ('internal', 'startsync'):
            if msg.payload:
                graph_id = msg.payload
                print("Graph id: %s" % graph_id)

                try:
                    graph = self.runtime.get_graph(graph_id)
                    for command, payload in get_graph_messages(graph, graph_id):
                        Message(b'graph', command, payload).sendto(
                            self.collector, identity)

                    # send the network status
                    status = self.handler.get_network_status(graph_id)
                    Message(b'network', b'status', status).sendto(
                        self.collector, identity)

                except RillRuntimeError as err:
                    self.send_error('graph', err, msg.id, identity)

            else:
                # initial connection
                meta = self.runtime.get_runtime_meta()
                Message(b'runtime', b'runtime', meta).sendto(
                    self.collector, identity)

                # send list of component specs
                # FIXME: move this under 'runtime' protocol?
                for spec in self.runtime.get_all_component_specs():
                    Message(b'component', b'component', spec).sendto(
                        self.collector, identity)

                # send list of graphs
                # FIXME: move this under 'runtime' protocol?
                # FIXME: notify subscribers about new graphs in handle_collect
                for graph_id in self.runtime._graphs.keys():
                    Message(b'graph', b'graph', {'id': graph_id}).sendto(
                        self.collector, identity)

        else:
            print("E: bad request, aborting")
            dump(msg)
            self.loop.stop()
            return

        # Now send END message with revision number
        logging.info("I: Sending state snapshot=%d" % self.handler.revision)
        Message(b'internal', b'endsync', self.handler.revision).sendto(
            self.collector, identity)

    def handle_collect(self, msg, identity):
        """
        handle messages pushed from client
        """
        print("handle_collect: %s" % str(msg))

        # FIXME: should the revision be per-graph?
        self.handler.handle_message(msg, identity)

    def send_error(self, protocol, err, message_id, identity):
        Message(protocol, 'error', {
            'message': err.message,
            'stack': traceback.format_exc(),
            'request_id': message_id
        }, revision=self.handler.revision).sendto(
            self.collector, identity)


def run_server():
    from rill.runtime import Runtime
    runtime = Runtime()
    runtime.register_module('rill.components.merge')
    runtime.register_module('tests.components')
    client = RuntimeServer(runtime)
    client.start()


def run_client():
    import uuid
    def on_recv(msg):
        print("RECV: %r" % msg)
    # Create and connect client
    client = Client(on_recv)
    # client.graph = b''
    client.connect("tcp://localhost", 5556)
    # client.connect("tcp://localhost", 5566)
    print("done connecting")
    from tests.test_runtime import get_graph
    graph_id = str(uuid.uuid1())
    graph_name = 'My Graph'
    graph, gen, passthru, outside = get_graph(graph_name)

    print("sending graph")
    for command, payload in get_graph_messages(graph, graph_id):
        client.send(Message('graph', command, payload))
    gevent.joinall([client.agent])


if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'server':
        run_server()
    else:
        run_client()
