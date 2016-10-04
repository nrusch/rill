from __future__ import print_function

import logging

import zmq.green as zmq
from zmq.green.eventloop.ioloop import IOLoop
import zmq.green.eventloop.zmqstream as zmqstream

from rill.runtime.plumbing import Message, MessageDispatcher, dump
IOLoop.configure(IOLoop)


class RuntimeServer(object):
    """
    Server managing the runtime state
    """

    def __init__(self, runtime, port=5556):
        from rill.runtime.handlers.runtime import RuntimeHandler

        self.runtime = runtime

        self.port = port
        # Context wrapper
        self.ctx = zmq.Context()
        # IOLoop reactor
        self.loop = IOLoop.instance()

        if not isinstance(self.loop, IOLoop):
            raise TypeError("Expected %s, got %s" % (IOLoop, type(self.loop)))

        # Set up our client server sockets

        # Publish updates to clients
        self.publisher = self.ctx.socket(zmq.PUB)
        # Collect updates and snapshot requests from clients, and send back
        # errors
        self.collector = self.ctx.socket(zmq.ROUTER)

        self.collector.bind("tcp://*:%d" % (self.port))
        self.publisher.bind("tcp://*:%d" % (self.port + 1))

        # Wrap sockets in ZMQStreams for IOLoop handlers
        self.publisher = zmqstream.ZMQStream(self.publisher)  # only necessary for heartbeat
        self.collector = zmqstream.ZMQStream(self.collector)

        # Register handlers with reactor
        self.collector.on_recv(self.handle_message)

        self.dispatcher = MessageDispatcher(self.publisher, self.collector)
        self.handler = RuntimeHandler(self.dispatcher, runtime)

    def start(self):
        print("Server listening on port %d" % self.port)
        # Run reactor until process interrupted
        try:
            # FIXME: move this to the RuntimeHandler
            self.runtime.send_network_data.event.listen(
                self.handler._send_network_data)

            self.loop.start()

        except KeyboardInterrupt:
            pass

    def stop(self):
        # FIXME: move this to the RuntimeHandler
        self.runtime.send_network_data.event.remove_listener(
            self.handler._send_network_data)

        self.loop.stop()

    # --

    def handle_message(self, msg_frames):
        """
        Message recevied callback (from ROUTER socket).

        Parameters
        ----------
        msg_frames : List[bytes]
        """
        # first frame of a router message is the identity of the dealer
        identity = msg_frames.pop(0)
        msg = Message.from_frames(*msg_frames)
        msg.identity = identity

        if msg.protocol == 'internal':
            self.handle_snapshot(msg)
        else:
            self.handle_collect(msg)

    def handle_snapshot(self, msg):
        """snapshot requests"""
        from rill.runtime.core import RillRuntimeError

        print("handle_snapshot: %r" % msg)

        if (msg.protocol, msg.command) == ('internal', 'startsync'):
            if msg.payload:
                # sync graph state
                graph_id = msg.payload
                print("Graph id: %s" % graph_id)

                try:
                    for msg in self.handler.get_graph_messages(graph_id):
                        msg.sendto(self.collector, msg.identity)

                    # send the network status
                    status = self.handler.get_network_status(graph_id)
                    Message(b'network', b'status', status).sendto(
                        self.collector, msg.identity)

                except RillRuntimeError as err:
                    self.dispatcher.send_error(msg, err)

            else:
                # sync runtime state
                # initial connection
                meta = self.runtime.get_runtime_meta()
                Message(b'runtime', b'runtime', meta).sendto(
                    self.collector, msg.identity)

                # send list of component specs
                # FIXME: move this under 'runtime' protocol?
                for msg in self.handler.get_all_component_specs():
                    msg.sendto(self.collector, msg.identity)

                # send list of graphs
                # FIXME: move this under 'runtime' protocol?
                # FIXME: notify subscribers about new graphs in handle_collect
                for graph_id in self.runtime._graphs.keys():
                    graph = self.runtime.get_graph(graph_id)
                    Message(b'graph', b'graph', {
                        'id': graph_id,
                        'metadata': graph.metadata
                    }).sendto(self.collector, msg.identity)

        else:
            print("E: bad request, aborting")
            dump(msg)
            self.loop.stop()
            return

        # Now send END message with revision number
        logging.info("I: Sending state snapshot=%d" % self.dispatcher.revision)
        Message(b'internal', b'endsync', self.dispatcher.revision).sendto(
            self.collector, msg.identity)

    def handle_collect(self, msg):
        """
        handle messages pushed from client
        """
        print("handle_collect: %s" % str(msg))

        # FIXME: should the revision be per-graph?
        self.handler.handle_message(msg)
