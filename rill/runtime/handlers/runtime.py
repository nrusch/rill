from __future__ import absolute_import
import functools
import datetime
import logging

from typing import Iterator

from rill.runtime.plumbing import Message
from rill.engine.exceptions import FlowError
from rill.events.listeners.memory import get_graph_messages
from rill.runtime.handlers.base import GraphHandler


class RuntimeComponentLogHandler(logging.Handler):
    # FIXME: what does this do?  It seems wrong that it's a bool. It's supposed to be a lock, or None.
    lock = False

    def __init__(self, runtime_handler):
        """

        Parameters
        ----------
        runtime_handler : RuntimeHandler
        """
        self.runtime_handler = runtime_handler
        super(RuntimeComponentLogHandler, self).__init__()

    def emit(self, record):
        """
        Parameters
        ----------
        record : logging.LogRecord
        """
        self.runtime_handler._send_log_record(record)


class RuntimeHandler(GraphHandler):
    """
    Utility class for processing messages into changes to a Runtime
    """
    def __init__(self, dispatcher, runtime=None):
        from rill.engine.component import _logger
        from rill.runtime.core import Runtime
        _logger.addHandler(RuntimeComponentLogHandler(self))

        super(RuntimeHandler, self).__init__(dispatcher)

        self.runtime = runtime if runtime else Runtime()
        self.runtime.port_opened.event.listen(self._send_port_opened)
        self.runtime.port_closed.event.listen(self._send_port_closed)

    def get_graph_messages(self, graph_id):
        """
        Parameters
        ----------
        graph_id : str

        Returns
        -------
        Iterator[Message]
        """
        for command, payload in get_graph_messages(
                self.runtime.get_graph(graph_id), graph_id):
            yield Message(b'graph', command, payload)

    def get_all_component_specs(self):
        """
        Returns
        -------
        Iterator[Message]
        """
        for spec in self.runtime.get_all_component_specs():
            yield Message(b'component', b'component', spec)

    def handle_message(self, msg):
        """
        Main entry point for handing a message

        Parameters
        ----------
        msg : Message
        """
        from rill.runtime.core import RillRuntimeError

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
            self.dispatcher.send_error(msg, err)

    def handle_graph(self, msg):
        """
        Modify our graph representation to match that of the UI/client

        Parameters
        ----------
        msg: Message
        """
        command = msg.command
        payload = msg.payload

        def get_graph():
            try:
                if command in {'clear', 'addgraph'}:
                    return payload['id']
                else:
                    return payload['graph']

            except KeyError:
                raise FlowError('No graph specified')

        # New graph
        send_component = False
        if command == 'clear':
            self.runtime.new_graph(
                get_graph(),
                payload.get('description', None),
                payload.get('metadata', None))
        elif command == 'addgraph':
            send_component = True
            self.runtime.new_graph(
                get_graph(),
                payload.get('description', None),
                payload.get('metadata', None),
                overwrite=False)
        # Nodes
        elif command == 'addnode':
            self.runtime.add_node(
                get_graph(),
                payload['id'],
                payload['component'],
                payload.get('metadata', None))
        elif command == 'removenode':
            self.runtime.remove_node(
                get_graph(),
                payload['id'])
        elif command == 'renamenode':
            self.runtime.rename_node(
                get_graph(),
                payload['from'],
                payload['to'])
        # Edges/connections
        elif command == 'addedge':
            self.runtime.add_edge(
                get_graph(),
                payload['src'],
                payload['tgt'],
                payload.get('metadata', None))
        elif command == 'removeedge':
            self.runtime.remove_edge(
                get_graph(),
                payload['src'],
                payload['tgt'])
        # IIP / literals
        elif command == 'addinitial':
            self.runtime.initialize_port(
                get_graph(),
                payload['tgt'],
                payload['src']['data'])
        elif command == 'removeinitial':
            self.runtime.uninitialize_port(
                get_graph(),
                payload['tgt'])
        # Exported ports
        elif command in ('addinport', 'addoutport'):
            send_component = True
            self.runtime.add_export(
                get_graph(),
                payload['node'],
                payload['port'],
                payload['public'],
                payload.get('metadata', None))
            # update_subnet(get_graph())
        elif command == 'removeinport':
            send_component = True
            self.runtime.remove_inport(
                get_graph(),
                payload['public'])
        elif command == 'removeoutport':
            send_component = True
            self.runtime.remove_outport(
                get_graph(),
                payload['public'])
        elif command == 'changeinport':
            self.runtime.change_inport(
                get_graph(),
                payload['public'],
                payload['metadata'])
        elif command == 'changeoutport':
            self.runtime.change_outport(
                get_graph(),
                payload['public'],
                payload['metadata'])
        elif command == 'renameinport':
            send_component = True
            self.runtime.rename_inport(
                get_graph(),
                payload['from'],
                payload['to'])
        elif command == 'renameoutport':
            send_component = True
            self.runtime.rename_outport(
                get_graph(),
                payload['from'],
                payload['to'])
        # Metadata changes
        elif command == 'changenode':
            self.runtime.set_node_metadata(
                get_graph(),
                payload['id'],
                payload['metadata'])
        elif command == 'changeedge':
            self.runtime.set_edge_metadata(
                get_graph(),
                payload['src'],
                payload['tgt'],
                payload['metadata'])
        elif command == 'addgroup':
            self.runtime.add_group(
                get_graph(),
                payload['name'],
                payload['nodes'],
                payload.get('metadata', None))
        elif command == 'removegroup':
            self.runtime.remove_group(
                get_graph(),
                payload['name'])
        elif command == 'renamegroup':
            self.runtime.rename_group(
                get_graph(),
                payload['from'],
                payload['to'])
        elif command == 'changegroup':
            self.runtime.change_group(
                get_graph(),
                payload['name'],
                payload.get('nodes', None),
                payload.get('metadata', None))
        elif command == 'changegraph':
            self.runtime.change_graph(
                get_graph(),
                payload.get('description', None),
                payload.get('metadata', None))
        elif command == 'renamegraph':
            self.runtime.rename_graph(
                payload['from'],
                payload['to'])

        else:
            self.logger.warn("Unknown command '%s' for protocol '%s'" %
                             (command, 'graph'))
            # FIXME: quit? dump message?
            return

        self.dispatcher.send_revision(msg)

        if send_component:
            self.dispatcher.send_info(
                protocol=b'component',
                command=b'component',
                payload=self.runtime._component_types[
                    'abc/{}'.format(get_graph())]['spec'])

    def get_network_status(self, graph_id):
        started, running = self.runtime.get_status(graph_id)
        return {
            'graph': graph_id,
            'started': started,
            'running': running,
            'time': datetime.datetime.now().isoformat()
            # 'debug': True,
        }

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
            # pass a callback that is run when the graph completes
            callback = functools.partial(
                self._send_network_status, msg, 'stopped')
            self.runtime.start(graph_id, callback)
            reply = 'started'
        elif command == 'stop':
            # FIXME: instead of sending a stop message below, we should rely on
            # the listener setup above to fire when the network stops
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

        self._send_network_status(msg, reply)

    def _send_log_record(self, record):
        """
        Parameters
        ----------
        record : logging.LogRecord
        """
        if hasattr(record, 'graph'):
            self.dispatcher.send_info(
                protocol=b'network',
                command=b'log',
                payload={
                    'graph': record.graph,
                    'message': record.getMessage()
                })

    def _send_network_status(self, msg, command):
        status = self.get_network_status(msg.graph_id)
        self.dispatcher.send_revision(msg.replace(command=command,
                                                  payload=status))

    def _send_network_data(self, connection, outport, inport, packet):
        # FIXME: I think that __str__ for Port handles the [index] check
        edge_id = '{}.{}{} -> {}.{}{}'.format(
            outport.component.name, outport.name,
            '[{}]'.format(outport.index) if outport.index else '',
            inport.component.name, inport.name,
            '[{}]'.format(inport.index) if inport.index else '')

        self.dispatcher.send_info(
            protocol=b'network',
            command=b'data',
            payload={
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

    def _send_port_opened(self, graph, component, port):
        self.dispatcher.send_info(
            protocol=b'network',
            command=b'portopen',
            payload={
                'graph': graph.name,
                'node': component.name,
                'port': port.name,
                'index': port.index,
                'type': 'inport' if port.kind == 'in' else 'outport'
            })

    def _send_port_closed(self, graph, component, port):
        self.dispatcher.send_info(
            protocol=b'network',
            command=b'portclosed',
            payload={
                'graph': graph.name,
                'node': component.name,
                'port': port.name,
                'index': port.index,
                'type': 'inport' if port.kind == 'in' else 'outport'
            })
