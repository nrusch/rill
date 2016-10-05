from rill.engine.network import Graph
from rill.engine.types import Stream
from rill.engine.exceptions import FlowError
from rill.runtime.handlers.base import GraphHandler
from rill.runtime.plumbing import Message
from rill.events.listeners.memory import get_graph_messages

from typing import Dict, Iterator

# -----------------------------------------------------------------------------
# DEPRECATED!  Use RuntimeHandler instead
# -----------------------------------------------------------------------------

ARG_REPLACEMENTS = (
    ('graph', 'graph_id'),
    ('node', 'node'),
    ('id', 'node'),
    ('from', 'from_name'),
    ('to', 'to_name')
)


def _toargs(d):
    for find, replace in ARG_REPLACEMENTS:
        if find in d:
            d[replace] = d[find]
    return d


class InMemoryGraphHandler(GraphHandler):
    """
    Propagate updates to a set of in-memory Graph objects
    """
    def __init__(self, dispatcher):
        super(InMemoryGraphHandler, self).__init__(dispatcher)
        self._graphs = {}  # type: Dict[str, Graph]
        self._component_types = {}

    COMMAND_TO_METHOD = {
        'graph': {
            # 'clear': 'new_graph',
            # 'addgraph': 'new_graph',
            'changegraph': '_set_graph_metadata',
            'renamegraph': '_rename_graph',
            # Nodes
            'addnode': '_add_node',
            'removenode': '_remove_node',
            'renamenode': '_rename_node',
            'changenode': '_set_node_metadata',
            # Edges/connections
            'addedge': '_add_edge',
            'removeedge': '_remove_edge',
            'changeedge': '_set_edge_metadata',
            # IIP / literals
            'addinitial': '_initialize_port',
            'removeinitial': '_uninitialize_port',
            # Exported ports
            'addinport': '_add_inport',
            'addoutport': '_add_outport',
            'removeinport': '_remove_inport',
            'removeoutport': '_remove_outport',
            'changeinport': '_set_inport_metadata',
            'changeoutport': '_set_outport_metadata',
            'renameinport': '_rename_inport',
            'renameoutport': '_rename_outport',
            # Groups
            'addgroup': '_add_group',
            'removegroup': '_remove_group',
            'renamegroup': '_rename_group',
            'changegroup': '_change_group',
        }
    }

    def recv_message(self, msg):
        """
        Handle a FBP graph message

        Parameters
        ----------
        msg: rill.runtime.plumbing.Message
        """
        if msg.protocol == 'graph' and msg.command == 'clear':
            payload = msg.payload.copy()
            graph_id = payload.pop('id')
            self._new_graph(graph_id, **payload)
        elif msg.protocol == 'graph' and msg.command == 'addgraph':
            payload = msg.payload.copy()
            graph_id = payload.pop('id')
            self._new_graph(graph_id, overwrite=False, **payload)
        else:
            # automatic handling
            try:
                method_name = self.COMMAND_TO_METHOD[msg.protocol][msg.command]
            except KeyError:
                raise FlowError("Unknown command '%s' for protocol '%s'" %
                                (msg.command, msg.protocol))

            method = getattr(self, method_name)
            method(**_toargs(msg.payload))

    def get_graph_messages(self, graph_id):
        """
        Parameters
        ----------
        graph_id : str

        Returns
        -------
        Iterator[rill.runtime.plumbing.Message]
        """
        for command, payload in get_graph_messages(
                self.runtime.get_graph(graph_id), graph_id):
            yield Message(b'graph', command, payload)

    # -- utilities

    @staticmethod
    def _get_port(graph, data, kind):
        return graph.get_component_port((data['node'], data['port']),
                                        index=data.get('index'),
                                        kind=kind)

    def get_graph(self, graph_id):
        """
        Parameters
        ----------
        graph_id : str
            unique identifier for the graph to create or get

        Returns
        -------
        graph : ``rill.engine.network.Graph``
            the graph object.
        """
        try:
            return self._graphs[graph_id]
        except KeyError:
            raise FlowError('Requested graph not found: {}'.format(graph_id))

    def add_graph(self, graph_id, graph):
        """
        Parameters
        ----------
        graph_id : str
        graph : ``rill.engine.network.Graph``
        """
        self._graphs[graph_id] = graph

    def _new_graph(self, graph_id, description=None, metadata=None,
                   overwrite=True):
        """
        Create a new graph.
        """
        if not overwrite and self._graphs.get(graph_id, None):
            raise FlowError('Graph already exists')

        self.logger.debug('Graph {}: Initializing'.format(graph_id))
        self.add_graph(graph_id, Graph(
            name=graph_id,
            description=description,
            metadata=metadata
        ))

    def _set_graph_metadata(self, graph_id, metadata):
        """
        Parameters
        ----------
        graph_id : str
        metadata : dict
        """
        graph = self.get_graph(graph_id)
        graph.set_metadata(metadata)
        return metadata

    def _rename_graph(self, from_name, to_name):
        """
        Parameters
        ----------
        from_name : str
        to_name : str
        """
        graph = self.get_graph(from_name)
        graph.rename(to_name)

        # FIXME: graph id and name should not be the same thing
        del self._graphs[from_name]
        self._graphs[to_name] = graph

        return graph

    def _add_node(self, graph_id, node, component, metadata=None):
        """
        Add a component instance.
        """
        self.logger.debug('Graph {}: Adding node {}({})'.format(
            graph_id, component, node))

        graph = self.get_graph(graph_id)

        component_class = self._component_types[component]['class']
        component = graph.add_component(node, component_class)
        component.metadata.update(metadata or {})
        return component

    def _remove_node(self, graph_id, node):
        """
        Destroy component instance.
        """
        self.logger.debug('Graph {}: Removing node {}'.format(
            graph_id, node))

        graph = self.get_graph(graph_id)
        graph.remove_component(node)

    def _rename_node(self, graph_id, from_name, to_name):
        """
        Rename component instance.
        """
        self.logger.debug('Graph {}: Renaming node {} to {}'.format(
            graph_id, from_name, to_name))

        graph = self.get_graph(graph_id)
        graph.rename_component(from_name, to_name)

    def _set_node_metadata(self, graph_id, node, metadata):
        graph = self.get_graph(graph_id)
        component = graph.component(node)
        graph.set_node_metadata(component, metadata)
        return component.metadata

    def _add_edge(self, graph_id, src, tgt, metadata=None):
        """
        Connect ports between components.
        """
        self.logger.debug('Graph {}: Connecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        graph = self.get_graph(graph_id)
        outport = self._get_port(graph, src, kind='out')
        inport = self._get_port(graph, tgt, kind='in')
        return graph.connect(outport, inport, metadata=metadata)

    def _remove_edge(self, graph_id, src, tgt):
        """
        Disconnect ports between components.
        """
        self.logger.debug('Graph {}: Disconnecting ports: {} -> {}'.format(
            graph_id, src, tgt))

        graph = self.get_graph(graph_id)
        graph.disconnect(self._get_port(graph, src, kind='out'),
                         self._get_port(graph, tgt, kind='in'))

    def _set_edge_metadata(self, graph_id, src, tgt, metadata):
        """
        Set metadata on edge
        """
        graph = self.get_graph(graph_id)
        outport = self._get_port(graph, src, kind='out')
        inport = self._get_port(graph, tgt, kind='in')
        edge_metadata = graph.set_edge_metadata(outport, inport, metadata)
        return edge_metadata

    def _initialize_port(self, graph_id, tgt, data):
        """
        Set the inital packet for a component inport.
        """
        self.logger.info('Graph {}: Setting IIP to {!r} on port {}'.format(
            graph_id, data, tgt))

        # FIXME: noflo-ui is sending an 'addinitial foo.IN []' even when
        # the inport is connected
        if data == []:
            return

        graph = self.get_graph(graph_id)

        target_port = self._get_port(graph, tgt, kind='in')
        # if target_port.is_connected():
        #     graph.disconnect(target_port)

        # FIXME: handle deserialization?
        if not target_port.auto_receive:
            data = Stream(data)

        graph.initialize(data, target_port)

    def _uninitialize_port(self, graph_id, tgt):
        """
        Remove the initial packet for a component inport.
        """
        self.logger.debug('Graph {}: Removing IIP from port {}'.format(
            graph_id, tgt))

        graph = self.get_graph(graph_id)

        target_port = self._get_port(graph, tgt, kind='in')
        if target_port.is_initialized():
            # FIXME: so far the case where an uninitialized port receives a uninitialize_port
            # message is when noflo initializes the inport to [] (see initialize_port as well)
            graph.uninitialize(target_port)

    def _add_export(self, graph_id, node, port, public, metadata=None):
        """
        Add inport or outport to graph
        """
        graph = self.get_graph(graph_id)
        graph.export("{}.{}".format(node, port), public, metadata or {})

    def _remove_inport(self, graph_id, public):
        """
        Remove inport from graph
        """
        graph = self.get_graph(graph_id)
        graph.remove_inport(public)

    def _set_inport_metadata(self, graph_id, public, metadata):
        """
        Change inport metadata
        """
        graph = self.get_graph(graph_id)
        graph.set_inport_metadata(public, metadata)

    def _rename_inport(self, graph_id, from_name, to_name):
        """
        Rename inport
        """
        graph = self.get_graph(graph_id)
        graph.rename_inport(from_name, to_name)

    def _remove_outport(self, graph_id, public):
        """
        Remove outport from graph
        """
        graph = self.get_graph(graph_id)
        graph.remove_outport(public)

    def _set_outport_metadata(self, graph_id, public, metadata):
        """
        Change outport metadata
        """
        graph = self.get_graph(graph_id)
        graph.set_outport_metadata(public, metadata)

    def _rename_outport(self, graph_id, from_name, to_name):
        """
        Rename outport
        """
        graph = self.get_graph(graph_id)
        graph.rename_outport(from_name, to_name)

    def _add_group(self, graph_id, name, nodes, metadata=None):
        """
        Add group to graph
        """
        graph = self.get_graph(graph_id)
        graph.add_group(name, nodes, metadata)

    def _remove_group(self, graph_id, name):
        """
        Remove group from graph
        """
        graph = self.get_graph(graph_id)
        graph.remove_group(name)

    def _rename_group(self, graph_id, from_name, to_name):
        """
        Remove group from graph
        """
        graph = self.get_graph(graph_id)
        graph.rename_group(from_name, to_name)

    def _change_group(self, graph_id, name, nodes=None, metadata=None):
        """
        Change group
        """
        graph = self.get_graph(graph_id)
        graph.change_group(name, nodes, metadata)


