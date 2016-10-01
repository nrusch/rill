from abc import ABCMeta, abstractmethod
from collections import deque

from typing import Any, Union, Iterator, Tuple

from rill.engine.status import StatusValues
from rill.engine.port import (Port, ArrayPort, BasePortCollection,
                              PortInterface, IN_NULL)
from rill.engine.exceptions import FlowError
from rill.engine.types import Stream
from rill.utils import NOT_SET
from rill.compat import *
from rill.utils.observer import supports_listeners

import gevent
import gevent.event
import gevent.pool


@add_metaclass(ABCMeta)
class InputInterface(PortInterface):
    """
    Enforces a common interface for all classes which receive packets
    """
    kind = 'in'

    @property
    def receiver(self):
        """
        Returns
        -------
        ``rill.engine.runner.ComponentRunner``
        """
        return self.component._runner

    @abstractmethod
    def receive(self):
        """
        Receive the next available packet from this InputInterface.

        The thread is suspended if no packets are currently available.
        At the end of input (when all upstream threads have closed their
        connected OutPorts), `None` is returned.

        Returns
        -------
        packet : Union[``rill.engine.packet.Packet``, None]
            next available packet
        """
        raise NotImplementedError

    # FIXME: use name consume_one() to match Packet.consume()? or provide a 2nd util?
    def receive_once(self, default=None):
        """
        Receive a packet, drop it, close the port, and return the packet's
        contents

        Parameters
        ----------
        default : Any
            default value if there was no packet to receive

        Returns
        -------
        Any
            packet contents
        """
        # p = self.receive()
        # if p is not None:
        #     value = p.get_contents()
        #     self.receiver.drop(p)
        # else:
        #     value = default
        # self.close()
        # return value

        try:
            return next(self.iter_contents())
        except StopIteration:
            return default
        finally:
            self.close()

    def iter_packets(self):
        """
        Iterate over received packets.

        Returns
        -------
        Iterator[``rill.engine.packet.Packet``]
        """
        while True:
            p = self.receive()
            if p is None:
                break
            yield p

    __iter__ = iter_packets

    def iter_contents(self):
        """
        Iterate over the content of received packets.

        This necessarily drops each packet that is received.

        Returns
        -------
        Iterator[Any]
        """
        for p in self.iter_packets():
            content = p.get_contents()
            self.component.drop(p)
            # FIXME: do we want to return the results of validation? this would
            # allow things like automatically casting int to float
            yield self.validate_packet_contents(content)

            # FIXME: there's some lock stuff in here.  I don't think it applies because the component attr is now static.
            # def set_component(self, receiver):
            #     """
            #     Set the receiver.
            #
            #     The receiver is the Component of the OutputPort connected to this
            #     InputInterface.  A connection can have multiple
            #     """
            #     # added for subnet support
            #     if self.receiver is None:
            #         # called by Component.network.connect()
            #         self.receiver = receiver
            #     else:
            #         # always use the same lock for subnet ports
            #         receiver._lock = self.receiver._lock
            #         self.receiver = receiver


class InputPort(Port, InputInterface):
    """
    An ``InputPort`` receives packets via a ``BaseConnection``.
    """

    def __init__(self, component, name, default=NOT_SET, static=False,
                 **kwargs):
        super(InputPort, self).__init__(component, name, **kwargs)
        self.default = default
        self.auto_receive = static
        # type: BaseConnection
        self._connection = None

        self.open.event.listen(component.port_opened)
        self.close.event.listen(component.port_closed)

    @supports_listeners
    def open(self):
        if not self.is_connected() and self.default is not NOT_SET:
            self.initialize(self.default)
        if self.is_connected():
            self._connection.open()

        self.open.event.emit(self)

    @supports_listeners
    def close(self):
        if self.is_connected():
            self._connection.close()

        self.close.event.emit(self)

    def is_closed(self):
        if self.is_connected():
            return self._connection.is_closed()
        else:
            return True

    def is_connected(self):
        return self._connection is not None

    def is_initialized(self):
        return self.is_connected() and \
               isinstance(self._connection, InitializationConnection)

    def is_null(self):
        return self.name == IN_NULL

    def receive(self):
        if self.is_connected():
            p = self._connection.receive()
            if p is not None:
                return p

    def initialize(self, static_value):
        """
        Initialize a port to a static value.

        Parameters
        ----------
        static_value : Any
        """
        if self.is_null():
            raise FlowError(
                "Cannot initialize null port: {}".format(self))
        if self.is_connected():
            if not self.is_initialized():
                raise FlowError(
                    "Port cannot have both an initial packet and a "
                    "connection: {}".format(self))
            # else:
            #     raise FlowError(
            #         "Port is already initialized: {}".format(self))
        # a Stream instance indicates that each item in the stream should be
        # yielded as a separate packet.  This is used to differentiate between
        # a port which may expect a single packet to contain a list of data.
        if isinstance(static_value, Stream):
            if self.auto_receive:
                raise FlowError(
                    "Static port initialized with a stream of "
                    "data: {}".format(self))
            content = [self.validate_packet_contents(p) for p in static_value]
        else:
            content = [self.validate_packet_contents(static_value)]

        self._connection = InitializationConnection(content, self)

    def uninitialize(self):
        """
        Remove static value initialization from the port.

        Returns
        -------
        ``InitializationConnection``
            The removed initialization connection
        """
        if self.is_null():
            raise FlowError(
                "Cannot uninitialize null port: {}".format(self))

        if not self.is_initialized():
            raise FlowError(
                "Port is not initialized: {}".format(self))
        conn = self._connection
        self._connection = None
        return conn

    def upstream_count(self):
        """
        Get the upstream packet count.

        Returns
        -------
        int
        """
        if self.is_connected():
            return self._connection.count()
        else:
            return 0

    def is_drained(self):
        """
        Returns True if the connection is drained (closed and empty).
        """
        return self.is_closed() and self.upstream_count() == 0


@add_metaclass(ABCMeta)
class BaseConnection(object):
    def send(self, packet, outport):
        raise NotImplementedError

    @property
    def receiver(self):
        """
        Returns
        -------
        ``rill.engine.runner.ComponentRunner``
        """
        return self.inport.component._runner

    @property
    def sender(self):
        """
        Returns
        -------
        ``rill.engine.runner.ComponentRunner``
        """
        return self.outport.component._runner

    @abstractmethod
    def receive(self):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def is_closed(self):
        raise NotImplementedError

    @abstractmethod
    def is_empty(self):
        raise NotImplementedError

    def is_drained(self):
        """
        Returns True if the connection is drained (closed and empty).
        """
        return self.is_closed() and self.is_empty()


class InitializationConnection(BaseConnection):
    """
    This class provides connections that hold a single object. It is a
    degenerate form of ``Connection``. From the component's point of
    view, it looks like a normal data stream containing one ``Packet``.
    """

    def __init__(self, content, inport):
        """

        Parameters
        ----------
        content : list
        inport
        """
        # the connected InputPort
        self.inport = inport
        self._content = content
        self._content_iter = None
        self._is_closed = True
        self.metadata = {}

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__,
                           self.inport.get_full_name())

    def count(self):
        return 0 if self.is_empty() else 1

    def close(self):
        """
        Close Initialization Connection
        """
        self._is_closed = True
        self._content_iter = None

    def open(self):
        """
        (Re)open Initialization Connection
        """
        self._is_closed = False
        self._content_iter = iter(self._content)

    def is_closed(self):
        return self._is_closed

    def is_empty(self):
        return self._is_closed

    def receive(self):
        """
        On the first call, returns a packet owned by the receiving component.

        Successive calls return None.

        Warning: the object contained in this packet must not be modified.

        See `InputInterface.receive`.
        """
        if not self.is_closed():
            try:
                p = self.inport.component.create(next(self._content_iter))
                self.inport.component.network.receives += 1
                self.receiver.logger.debug("Received Initial: " + str(p),
                                           port=self.inport)
                return p
            except StopIteration:
                self.close()
        return None


class Connection(BaseConnection):
    """
    This class implements buffering between Component threads.

    It represents an edge between one or more ``OutputPort``s and a single
    ``InputPort``.

    One is created behind the scenes whenever two ports are connected. This
    class was founded on Doug Lea's BoundedBufferVST class from his book
    _Concurrent Programming in Java_, page 100.
    """

    def __init__(self):
        # number of connected senders that are not closed. incremented by
        # OutputPort.open()
        self._sender_count = 0
        # the connected InputPort
        # type: rill.engine.outputport.InputPort
        self.inport = None
        # the outport currently sending
        # type: rill.engine.outputport.OutputPort
        self.outport = None
        # all connected OutputPorts
        # type: Set[rill.engine.outputport.OutputPort]
        self.outports = set()
        # packet queue and blocking events
        self._queue = None
        self._not_empty = gevent.event.Event()
        self._not_full = gevent.event.Event()
        # properties
        self.drop_oldest = False
        self.count_packets = False
        self.metadata = {}

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__,
                           self.inport.get_full_name())

    def __getstate__(self):
        data = self.__dict__.copy()
        for k in ('_not_empty', '_not_full'):
            data.pop(k)
        return data

    def __setstate__(self, data):
        for key, value in data.items():
            self.__dict__[key] = value
        self._not_empty = gevent.event.Event()
        self._not_full = gevent.event.Event()

    def count(self):
        return len(self._queue)

    def open(self):
        pass

    def close(self):
        """
        Close the connection.

        This will prevent any more packets from being sent to or received from
        this connection.
        """

        if self.is_closed():
            return

        self.receiver.logger.debug("Close input connection", port=self.inport)

        self._sender_count = 0  # set sender count to zero
        if not self.is_empty():
            self.receiver.logger.warning(
                "{} packets on input connection lost".format(self.count()),
                port=self.inport)
            self._queue.clear()

        # release any senders waiting for slots.
        # they will check for a closed state and exit
        self._not_empty.set()
        self._not_full.set()

    def indicate_sender_closed(self):
        """
        Indicate one sending Component closed
        """
        try:
            self.receiver.trace_locks("sender closed - lock", port=self.inport)
            with self.receiver._lock:
                # -- synchronized (self):
                if not self.is_closed():
                    self._sender_count -= 1

                    if self.is_drained():  # means closed AND empty
                        if self.receiver.status in (StatusValues.DORMANT,
                                                    StatusValues.NOT_STARTED):
                            self.receiver.activate()
                        else:
                            # release any senders waiting for slots.
                            # they will check for a closed state and exit
                            self._not_empty.set()

        finally:
            self.receiver.trace_locks("sender closed - unlock",
                                      port=self.inport)

    def connect(self, inport, outport, capacity):
        """
        Connect an ``InputPort`` to an ``OutputPort``.

        Parameters
        ----------
        inport : ``rill.engine.inputport.InputPort``
        outport : ``rill.engine.outputport.OutputPort``
        capacity : int
            size of the buffer
        """
        if self.outports:
            if capacity != self.capacity():
                # a previous connect specified a destination port with the same
                # name and index.
                raise FlowError(
                    "{}: Connection capacity does not agree with previous "
                    "specification".format(self))
        else:
            self._queue = deque(maxlen=capacity)

        self.inport = inport
        self.outports.add(outport)
        outport._connections.append(self)

    def is_closed(self):
        """
        Returns True if the connection is closed (not necessarily drained).

        A connection is open as long as there are open ``OutputPort``s still
        sending through it.

        Returns
        -------
        bool
        """
        return self._sender_count == 0

    def is_empty(self):
        """
        Returns True if the connection is empty.

        Returns
        -------
        bool
        """
        return self.count() == 0

    def is_full(self):
        """
        Returns True if the connection is full.

        Returns
        -------
        bool
        """
        return self.count() == self.capacity()

    def receive(self):
        """
        See ``InputInterface.receive``.
        """
        self.receiver.logger.debug("Receiving", port=self.inport)

        # receiver.current_connection = self
        if self.is_drained():
            self.receiver.logger.debug("Receive skipped: drained",
                                       port=self.inport)
            return None

        self.receiver.network.receives += 1
        while self.is_empty():
            self.receiver.status = StatusValues.SUSP_RECV
            self.receiver.curr_conn = self
            self.receiver.logger.debug("Receive suspended", port=self.inport)

            self._not_empty.wait()

            if self.receiver.is_terminated() or self.receiver.has_error():
                return None

            self.receiver.logger.debug("Receive resumed", port=self.inport)
            self.receiver.status = StatusValues.ACTIVE

            if self.is_drained():
                # port closed while it was empty
                self.receiver.logger.debug("Receive aborted: drained",
                                           port=self.inport)
                return None

        # if self.is_drained():
        #     self.receiver.logger.debug("Receive drained", port=self.inport)
        #     return None

        # if self.is_full():
        #     # FIXME: use gevent Event?
        #     # notify components waiting to send
        #     # notify_all()
        #     # NOTE: if the queue is full, the sender will be in the is_full loop
        #     # and won't be able to do anything unless drop_oldest is enabled
        #     # gevent.sleep(0.1)
        #     self._not_empty.set()
        #     self._not_empty.clear()

        # consume one packet
        packet = self._queue.popleft()

        self._not_full.set()
        self._not_full.clear()

        packet.set_owner(self.receiver.component)

        if packet.get_contents() is None:
            self.receiver.logger.debug("Received None packet",
                                       port=self.inport)
        else:
            self.receiver.logger.debug("Received: " + str(packet),
                                       port=self.inport)

        if self.count_packets:
            self.receiver.network.incr_packet_count(self)

        self.receiver.network.active = True
        return packet

    @supports_listeners
    def send(self, packet, outport):
        """
        See ``OutputPort.send``

        Parameters
        ----------
        packet : ``rill.engine.packet.Packet``
            the packet to send
        outport : ``rill.engine.ouputport.OutputPort``
            the ``rill.engine.outputport.OutputPort`` on which the packet is to
            be sent

        Returns
        -------
        bool
            whether the packet was succesfully sent
        """

        self.outport = outport

        if self.is_closed():
            self.sender.logger.warning("Send: Inport closed. "
                                       "Failed to deliver packet to {}",
                                       port=outport, args=[self.inport])
            return False

        # self.sender.current_connection = self
        # self.sender.trace_funcs("Sending: " + str(packet))
        while self.is_full():
            if self.drop_oldest:
                self._queue.popleft()
                # self.sender.self.drop(p)
                self.sender.network.drop_olds += 1
                self.sender.logger.debug("Send: Queue full. Dropping old "
                                         "packets waiting for {}",
                                         port=outport, args=[self.inport])
            else:
                self.sender.curr_outport = outport
                self.sender.status = StatusValues.SUSP_SEND
                self.sender.logger.debug("Send: Queue full. Suspending "
                                         "delivery to {}",
                                         port=outport, args=[self.inport])

                # wait for another component to receive a packet
                # FIXME: Threads
                # wait()
                self._not_full.wait()

                self.outport = outport
                self.sender.status = StatusValues.ACTIVE
                self.sender.logger.debug("Send: Resume delivery to {}",
                                         port=outport, args=[self.inport])

        if self.is_closed():
            self.sender.logger.warning("Send: Input closed. "
                                       "Failed to deliver packet to {}",
                                       port=outport, args=[self.inport])
            return False

        self.sender.trace_locks("send - lock", port=outport)
        try:
            with self.receiver._lock:
                packet.clear_owner()
                self._queue.append(packet)
                if self.receiver.status in [
                    StatusValues.DORMANT,
                    StatusValues.NOT_STARTED,
                    StatusValues.SUSP_FIPE]:
                    # start or wake up if necessary
                    self.receiver.activate()
                else:
                    # FIXME: Threads
                    # notify_all()  # notify receiver
                    # other components waiting to send to this connection may
                    # also get notified, but this is handled by while statement
                    self._not_empty.set()
                    self._not_empty.clear()

                outport.sender.status = StatusValues.ACTIVE
                self.sender.network.active = True
        except Exception as e:
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.sender.trace_locks("send - unlock", port=outport)

        self.sender.network.sends += 1
        self.outport = None

        self.send.event.emit(self, outport, self.inport, packet)
        return True

    def capacity(self):
        """
        Get the size of the connection buffer.

        Returns
        -------
        int
        """
        return self._queue.maxlen


class InputArray(ArrayPort, PortInterface):
    _valid_classes = (InputInterface,)
    port_class = InputPort
    kind = 'in'

    def __init__(self, component, name, default=NOT_SET, static=False,
                 **kwargs):
        super(InputArray, self).__init__(component, name, **kwargs)
        self.default = default
        self.auto_receive = static

    def _create_element(self, index):
        return self.port_class(self.component, self.name, index=index,
                               type=self.type, required=self.required,
                               default=self.default, static=self.auto_receive)


class BaseInputCollection(BasePortCollection, InputInterface):
    """Base class for input port collections"""
    _valid_classes = (InputInterface, InputArray)


# class InputCollection(BaseInputCollection, PortInterface):
#     def __iter__(self):
#         return self.iter_ports()


class EagerInputCollection(BaseInputCollection):
    """
    Provides methods for receiving from the first ready port within the
    collection.
    """

    def next_port(self):
        """
        Find the first port that has data.

        Returns
        -------
        Union[``InputPort``, None]
            If all ports are drained, returns None
            Otherwise, suspends until data arrives at a port array element.
        """
        self.receiver.trace_funcs("Starting next_port")
        while True:
            all_drained = True
            for port in self.ports():
                # if port.is_closed():
                #   continue
                #
                if not port.is_empty():
                    self.receiver.trace_funcs(
                        "Ending next_port - returned: {}".format(port))
                    return port
                elif not port.is_drained():
                    all_drained = False
            if all_drained:
                self.receiver.trace_funcs(
                    "Ending next_port - all drained")
                return None
            else:
                self.receiver.trace_locks("gpwd - lock ")
                try:
                    with self.receiver._lock:
                        self.receiver.status = StatusValues.SUSP_FIPE
                        self.receiver.trace_funcs("find IPE with data")
                        self.receiver.trace_locks("fipewd - await")
                        self.receiver._can_go.wait()
                finally:
                    self.receiver.trace_locks("gpwd - unlock ")
                    self.receiver.status = StatusValues.ACTIVE
                    self.receiver.trace_funcs("Active")

    def receive(self):
        """
        Receive on the first port that has data.

        Returns
        -------
        ``rill.engine.packet.Packet``
        """
        port = self.next_port()
        if port is not None:
            return port.receive()


class SynchronizedInputCollection(BaseInputCollection):
    """
    Provides methods for synchronizing the receipt of packets from a collection
    of ports.
    """

    def is_initialized(self):
        return all(p.is_initialized() for p in self.ports())

    # FIXME: cache this
    def static_ports(self):
        ports = self.ports()
        static = [p for p in ports if p.is_initialized()]
        if len(static) != len(ports):
            return static
        else:
            # return no ports if all static to avoid infinite loop in receive
            return []

    def receive(self):
        """
        Receive from ports in a synchronized fashion.

        Closes all the ports when the first drained port is encountered.

        Returns
        -------
        Tuple[``rill.engine.packet.Packet``, ...]
        """
        if not self.ports():
            return

        # result = []
        # for port in self.ports():
        #     packet = port.receive()
        #     if packet is None:
        #         # FIXME: provide option to error if one port still has more data left
        #         self.close()
        #         for pkt in result:
        #             pkt.drop()
        #         return
        #     result.append(packet)

        # get results in parallel
        group = gevent.pool.Group()
        result = group.map(lambda x: x.receive(), self.ports())
        valid = [p for p in result if p is not None]
        if len(valid) != len(result):
            self.close()
            for p in valid:
                p.drop()
            return

        # FIXME: maybe this should be configurable, or explicitly set using repeat()
        # initialization ports are treated like constants: they repeat
        # forever as long as there is a non-static port still open.
        for port in self.static_ports():
            port.open()
        return tuple(result)

    def iter_contents(self):
        """
        Iterate over the content of received packets.

        This necessarily drops each packet that is received.

        Returns
        -------
        Iterator[Tuple[Any, ...]]
        """
        for group in self.iter_packets():
            yield tuple(p.drop() for p in group)
