import logging
from abc import ABCMeta, abstractmethod
from rill.compat import *

from typing import Iterator


@add_metaclass(ABCMeta)
class GraphHandler(object):
    """
    Propagates updates to a graph, usually emanating from a listener, to a
    destination graph store, such as a database, socket connection, or
    in-memory representation.
    """
    def __init__(self, dispatcher):
        """
        Parameters
        ----------
        dispatcher : rill.plumbing.MessageDispatcher
        """
        self.dispatcher = dispatcher
        self.logger = logging.getLogger('{}.{}'.format(
            self.__class__.__module__, self.__class__.__name__))

    @abstractmethod
    def handle_message(self, msg):
        """
        Handle a FBP graph message

        Parameters
        ----------
        msg: rill.plumbing.Message
        """

    @abstractmethod
    def get_graph_messages(self, graph_id):
        """
        Parameters
        ----------
        graph_id : str
            unique identifier for the graph

        Returns
        -------
        Iterator[rill.plumbing.Message]
        """
