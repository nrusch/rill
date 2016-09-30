from abc import ABCMeta, abstractmethod
from rill.compat import *


@add_metaclass(ABCMeta)
class GraphDispatcher(object):
    """
    Propagates updates to a graph, usually emanating from a listener, to a
    destination graph store, such as a database, socket connection, or
    in-memory representation.
    """
    def __init__(self, dispatcher):
        """
        Parameters
        ----------
        dispatcher : plumbing.MessageDispatcher
        """
        self.dispatcher = dispatcher

    @abstractmethod
    def recv_message(self, msg):
        """
        Handle a FBP graph message

        Parameters
        ----------
        msg: rill.plumbing.Message
        """

    # @abstractmethod
    # def new_graph(self, msg):
    #     """
    #     Create a new graph.
    #     """
    #
    # @abstractmethod
    # def set_graph_metadata(self, msg):
    #     """
    #     Set Graph Metadata
    #     """
    #
    # @abstractmethod
    # def rename_graph(self, msg):
    #     """
    #     Rename Graph
    #     """
    #
    # @abstractmethod
    # def add_node(self, msg):
    #     """
    #     Add a component instance.
    #     """
    #
    # @abstractmethod
    # def remove_node(self, msg):
    #     """
    #     Destroy component instance.
    #     """
    #
    # @abstractmethod
    # def rename_node(self, msg):
    #     """
    #     Rename component instance.
    #     """
    #
    # @abstractmethod
    # def set_node_metadata(self, msg):
    #     """
    #     Sends changenode event
    #     """
    #
    # @abstractmethod
    # def add_edge(self, msg):
    #     """
    #     Connect ports between components.
    #     """
    #
    # @abstractmethod
    # def remove_edge(self, msg):
    #     """
    #     Disconnect ports between components.
    #     """
    #
    # @abstractmethod
    # def set_edge_metadata(self, msg):
    #     """
    #     Send changeedge event'
    #     """
    #
    # @abstractmethod
    # def initialize_port(self, msg):
    #     """
    #     Set the inital packet for a component inport.
    #     """
    #
    # @abstractmethod
    # def uninitialize_port(self, msg):
    #     """
    #     Remove the initial packet for a component inport.
    #     """
    #
    # @abstractmethod
    # def add_inport(self, msg):
    #     """
    #     Add inport to graph
    #     """
    #
    # @abstractmethod
    # def remove_inport(self, msg):
    #     """
    #     Remove inport from graph
    #     """
    #
    # @abstractmethod
    # def set_inport_metadata(self, msg):
    #     """
    #     Send the metadata on an exported inport
    #     """
    #
    # @abstractmethod
    # def rename_inport(self, msg):
    #     """
    #     Rename inport
    #     """
    #
    # @abstractmethod
    # def add_outport(self, msg):
    #     """
    #     Add outport to graph
    #     """
    #
    # @abstractmethod
    # def remove_outport(self, msg):
    #     """
    #     Remove outport from graph
    #     """
    #
    # @abstractmethod
    # def set_outport_metadata(self, msg):
    #     """
    #     Send the metadata on an exported outport
    #     """
    #
    # @abstractmethod
    # def rename_outport(self, msg):
    #     """
    #     Rename outport
    #     """
    #
    # @abstractmethod
    # def add_group(self, msg):
    #     """
    #     Add group to graph
    #     """
    #
    # @abstractmethod
    # def remove_group(self, msg):
    #     """
    #     Remove group from graph
    #     """
    #
    # @abstractmethod
    # def rename_group(self, msg):
    #     """
    #     Rename group
    #     """
    #
    # @abstractmethod
    # def change_group(self, msg):
    #     """
    #     Change group
    #     """

