import pytest
import uuid

from rill.engine.runner import ComponentRunner
from rill.runtime.core import Runtime
from rill.events.listeners.memory import get_graph_messages
from rill.engine.network import Graph
from rill.runtime.plumbing import Message, MessageDispatcher

from rill.runtime.handlers.runtime import RuntimeHandler

from mock import MagicMock, call
from tests.components import *
from rill.compat import *

import logging
ComponentRunner.logger.setLevel(logging.DEBUG)


GRAPH_ID = 'graph1'


def get_graph(graph_name):
    graph = Graph(name=graph_name)

    gen = graph.add_component('Generate', GenerateTestData)
    gen.metadata['x'] = 5
    gen.metadata['y'] = 5

    passthru = graph.add_component('Pass', Passthru)
    outside = graph.add_component('Outside', Passthru)

    graph.connect('Generate.OUT', 'Pass.IN')
    graph.connect('Outside.OUT', 'Pass.IN')
    graph.initialize(5, 'Generate.COUNT')
    graph.export('Pass.OUT', 'OUTPORT')
    graph.export('Outside.IN', 'INPORT')
    return graph, gen, passthru, outside


def _iter_client_messages():
    graph = get_graph("My Graph")[0]
    for command, payload in get_graph_messages(graph, GRAPH_ID):
        msg = Message('graph', command, payload, id=uuid.uuid1())
        msg.identity = 'foo'
        yield msg


@pytest.fixture
def client_messages():
    return list(_iter_client_messages())


# @pytest.fixture(params=[InMemoryGraphHandler])
# def client_messages(request):
#     graph = get_graph("My Graph")
#     handler = request.param()
#     return [Message('graph', *,
#                     id=bytes(uuid.uuid1()))]


class MockDispatcher(MessageDispatcher):
    def __init__(self):
        super(MockDispatcher, self).__init__(MagicMock(), MagicMock())

def _remove_message_payload(call_object):
    args, kwargs = call_object
    args = args[0][:]
    # payload
    args.pop(2)
    # id
    args.pop(2)
    print args
    return args


def test_runtime_handler(client_messages):
    runtime = Runtime()
    runtime.register_module('tests.components')

    dispatcher = MockDispatcher()

    publish = dispatcher.publish_socket
    respond = dispatcher.response_socket

    handler = RuntimeHandler(dispatcher, runtime)
    for msg in client_messages:
        handler.handle_message(msg)

    publish_calls = [_remove_message_payload(c) for c
                     in publish.send_multipart.call_args_list]
    assert publish_calls == [
        ['graph', 'clear', '1'],
        ['graph', 'addnode', '2'],
        ['graph', 'addnode', '3'],
        ['graph', 'addnode', '4'],
        ['graph', 'addinitial', '5'],
        ['graph', 'addedge', '6'],
        ['graph', 'addedge', '7'],
        ['graph', 'addinport', '8'],
        # adding an inport generates a new component message w/ same revision
        ['component', 'component', '8'],
        ['graph', 'addoutport', '9'],
        # adding an outport generates a new component message w/ same revision
        ['component', 'component', '9'],
    ]

    # TODO: test data, started, stopped, portopened, portclosed events
    # TODO: test that errors are sent on respond socket


# -- Delete below here when Runtime is replaced by dispatchers

# FIXME: create a fixture for the network in test_network_serialization and use that here
def test_get_graph_messages():
    """
    Test that runtime can build graph with graph protocol messages
    """
    graph_id = 'graph1'
    graph_name = 'My Graph'
    runtime = Runtime()

    graph, gen, passthru, outside = get_graph(graph_name)
    runtime.add_graph(graph_id, graph)

    messages = list(get_graph_messages(runtime.get_graph(graph_id), graph_id))

    # FIXME: paste in an exact copy of the document here
    assert ('clear', {
        'id': graph_id,
        'name': graph_name
    }) in messages

    assert ('addnode', {
        'graph': graph_id,
        'id': gen.get_name(),
        'component': gen.get_type(),
        'metadata': gen.metadata
    }) in messages
    assert ('addnode', {
        'graph': graph_id,
        'id': passthru.get_name(),
        'component': passthru.get_type(),
        'metadata': passthru.metadata
    }) in messages
    assert ('addnode', {
        'graph': graph_id,
        'id': outside.get_name(),
        'component': outside.get_type(),
        'metadata': outside.metadata
    }) in messages

    assert ('addedge', {
        'graph': graph_id,
        'src': {
            'node': gen.get_name(),
            'port': 'OUT'
        },
        'tgt': {
            'node': passthru.get_name(),
            'port': 'IN'
        }
    }) in messages
    assert ('addedge', {
        'graph': graph_id,
        'src': {
            'node': outside.get_name(),
            'port': 'OUT'
        },
        'tgt': {
            'node': passthru.get_name(),
            'port': 'IN'
        }
    }) in messages

    assert ('addinitial', {
        'graph': graph_id,
        'src': {
            'data': [5],
        },
        'tgt': {
            'node': gen.get_name(),
            'port': 'COUNT'
        }
    }) in messages

    assert ('addinport', {
        'graph': graph_id,
        'public': 'INPORT',
        'node': outside.get_name(),
        'port': 'IN',
        'metadata': {}
    }) in messages
    assert ('addoutport', {
        'graph': graph_id,
        'public': 'OUTPORT',
        'node': passthru.get_name(),
        'port': 'OUT',
        'metadata': {}
    }) in messages


def test_component_updates():
    graph_id = 'graph1'
    graph_name = 'My Graph'
    runtime = Runtime()

    graph, gen, passthru, outside = get_graph(graph_name)
    runtime.add_graph(graph_id, graph)

    component = runtime._component_types.get('abc/{0}'.format(graph.name))
    assert component is not None
    assert len(component['spec']['inPorts']) == 2
    runtime.add_export(graph_id, gen.get_name(), 'COUNT', 'GEN_COUNT')
    # grab the updated component
    component = runtime._component_types.get('abc/{0}'.format(graph.name))
    assert len(component['spec']['inPorts']) == 3
