import subprocess
import sys
from rill.handlers.base import GraphHandler
from rill.engine.exceptions import FlowError
from rill.compat import *


class SandboxedGraphHandler(GraphHandler):

    # FIXME: there are a couple of approaches to get responses back from the subprocess
    # - have it send messages back to us, and then we forward it to a
    #   responder function passed in from the RuntimeServer, where it will get
    #   a revision before being sent to clients on the pub socket
    # - have it send messages the pub socket, communicating directly with
    #   clients.
    # the latter cuts out the middle-man, but requires the InMemoryGraphDispatcher
    # to be capable of doing more, such as tracking the revision. it also means
    # the revision has to be per-graph, instead of global, which gets complicated
    def __init__(self, dispatcher):
        super(SandboxedGraphHandler, self).__init__(dispatcher)
        self._graphs = {}

    def _forward(self, msg):
        """
        Forward message to sub-process
        """
        msg.sendto(self._graph[msg.graph_id]['socket'])

    def recv_message(self, msg):
        """
        Handle a FBP graph message

        Parameters
        ----------
        msg: rill.plumbing.Message
        """
        assert msg.protocol in {'graph', 'network'}

        # FIXME: should we use REQ/REP so that we can confirm that each message succeeded?
        # we want this to be a drop-in replacement for InMemoryGraphDispatcher:
        # that means both need to raise an exception here on failure, or both need
        # to handle sending error messages themselves.  sending our own error
        # messages seems easier
        if msg.protocol == 'graph' and msg.command in {'clear', 'addgraph'}:
            self.new_graph(msg)
        else:
            self._forward(msg)

    def new_graph(self, msg):
        """
        Create a new graph.
        """
        context = zmq.Context()
        # FIXME: should we use REQ/REP so that we can confirm that each message succeeded?
        socket = context.socket(zmq.PUSH)
        port = socket.bind_to_random_port("tcp://*")

        # FIXME: use a separate greenlet to monitor the subprocess for when it terminates?
        proc = subprocess.Popen([sys.executable, '-m',
                                 'rill.events.dispatchers.sandbox',
                                 bytes(port), bytes(msg.graph_id)])
        # FIXME: wait till the subprocess is ready? will zmq queue up messages
        # if they are sent before the subproc has connected to the socket?
        self._graphs[msg.graph_id] = {
            'port': port,
            'proc': proc,
            'socket': socket
        }


if __name__ == '__main__':
    import gevent
    import zmq.green as zmq
    from rill.events.dispatchers.memory import InMemoryGraphDispatcher
    from plumbing import Message


    def client(recv_port, graph_id):
        context = zmq.Context()
        socket_pull = context.socket(zmq.PULL)
        socket_pull.connect("tcp://localhost:%s" % recv_port)
        print "Connected to server with port %s" % recv_port
        # Initialize poll set
        poller = zmq.Poller()
        poller.register(socket_pull, zmq.POLLIN)

        # even though InMemoryGraphDispatcher can handle multiple graphs, there
        # will be one, because we want one subprocess per graph.

        def responder(msg):
            msg.sendto(socket)

        dispatcher = InMemoryGraphDispatcher(responder=responder)
        dispatcher._new_graph(graph_id)

        should_continue = True
        while should_continue:
            socks = dict(poller.poll())
            if socket_pull in socks and socks[socket_pull] == zmq.POLLIN:
                msg = Message.from_frames(*socket_pull.recv_multipart())
                try:
                    dispatcher.recv_message(msg)
                except FlowError:
                    # FIXME: should we use REQ/REP so that we can confirm that each message
                    # is successfully applied?
                    raise

                # print "Recieved control command: %s" % message
                # if message == "Exit":
                #     print "Recieved exit command, client will stop recieving messages"
                #     should_continue = False

