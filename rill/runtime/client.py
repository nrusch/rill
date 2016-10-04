from __future__ import print_function

import logging
import time

from posixpath import join

import gevent
import zmq.green as zmq
from zmq.utils.strtypes import bytes, unicode, asbytes

from rill.runtime.plumbing import zpipe, Message

# Server considered dead if silent for this long
SERVER_TTL = 5.0  # secs

__all__ = ['RuntimeClient']


class RuntimeClient(object):
    """
    ZMQ RuntimeClient for communicating with RuntimeServer
    """
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
    One connection from RuntimeClient to RuntimeServer
    """
    expiry = 0  # Expires at this time
    requests = 0  # How many snapshot requests made?

    def __init__(self, ctx, address, port):
        # server address
        self.address = address
        # server port
        self.port = port

        # outgoing updates (and responses for snapshots and errors)
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


# RuntimeClient States
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
                    b'internal', b'startsync', agent.graph, agent.message_id
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
