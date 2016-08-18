import pytest

import itertools
import json
import uuid

from rill.engine.utils import patch
patch()

import gevent

from mock import MagicMock, patch
import rill.plumbing as plumbing
from rill.plumbing import Client, RuntimeServer, Message
from rill.runtime import Runtime

import zmq.green as zmq
import zmq.green.eventloop.zmqstream as zmqstream


@patch.object(zmq, 'Context')
@patch.object(zmqstream, 'ZMQStream')
@patch.object(plumbing, 'is_socket_type', return_value=False)
def test_runtime_server(Context, ZMQStream, is_socket_type):
    runtime = Runtime()
    runtime.register_module('tests.components')

    server = RuntimeServer(runtime)

    test_graph = Message(
        protocol='graph',
        command='addgraph',
        payload={
            'id': 'testgraph',
            'name': 'testgraph'
        },
        id=uuid.uuid1()
    )

    server.handle_collect(test_graph, '')
    server.publisher.send_multipart.assert_called()

    result = Message.from_frames(
        *server.publisher.send_multipart.call_args[0][0])

    assert result.protocol == 'graph'
    assert result.command == 'addgraph'
    assert result.payload['id'] == 'testgraph'
    assert runtime._graphs['testgraph']

    server.publisher.send_multipart.reset_mock()
    server.collector.send_multipart.reset_mock()

    snapshot_message = Message(
        'internal', 'startsync', 'testgraph', uuid.uuid1())

    server.handle_snapshot(snapshot_message, '')

    clear_message = Message.from_frames(
        *server.collector.send_multipart.call_args_list[0][0][0])

    assert clear_message.protocol == 'graph'
    assert clear_message.command == 'clear'
    assert clear_message.payload == {
        'id': 'testgraph',
        'name': 'testgraph',
    }

    status_message = Message.from_frames(
        *server.collector.send_multipart.call_args_list[1][0][0])

    assert status_message.protocol == 'network'
    assert status_message.command == 'status'
    assert status_message.payload['graph'] == 'testgraph'
    assert status_message.payload['running'] == False
    assert status_message.payload['started'] == False

    server.publisher.send_multipart.reset_mock()
    server.collector.send_multipart.reset_mock()

    genarray = Message(
        protocol='graph',
        command='addnode',
        payload={
            'graph': 'testgraph',
            'id': 'node1',
            'component': 'tests.components/GenerateArray'
        },
        id=uuid.uuid1()
    )

    server.handle_collect(genarray, '')
    server.publisher.send_multipart.assert_called()
    genarray_result = Message.from_frames(
        *server.publisher.send_multipart.call_args[0][0])

    assert genarray_result.protocol == 'graph'
    assert genarray_result.command == 'addnode'
    assert genarray_result.payload == genarray.payload

    server.publisher.send_multipart.reset_mock()
    server.collector.send_multipart.reset_mock()

    repeat = Message(
        protocol='graph',
        command='addnode',
        payload={
            'graph': 'testgraph',
            'id': 'node2',
            'component': 'tests.components/Repeat'
        },
        id=uuid.uuid1()
    )

    server.handle_collect(repeat, '')
    server.publisher.send_multipart.assert_called()
    repeat_result = Message.from_frames(
        *server.publisher.send_multipart.call_args[0][0])

    assert repeat_result.protocol == 'graph'
    assert repeat_result.command == 'addnode'
    assert repeat_result.payload == repeat.payload

    server.publisher.send_multipart.reset_mock()
    server.collector.send_multipart.reset_mock()

    edge = Message(
        protocol='graph',
        command='addedge',
        payload={
            'graph': 'testgraph',
            'src': {
                'node': 'node1',
                'port': 'OUT'
            },
            'tgt': {
                'node': 'node2',
                'port': 'in'
            }
        },
        id=uuid.uuid1()
    )

    server.handle_collect(edge, '')
    server.publisher.send_multipart.assert_called()
    edge_result = Message.from_frames(
        *server.publisher.send_multipart.call_args[0][0])

    assert edge_result.payload['src'] == edge.payload['src']
    assert edge_result.payload['tgt'] == edge.payload['tgt']


@pytest.mark.skip(reason="integration test")
def test_runtime_flow():
    runtime = Runtime()
    runtime.register_module('tests.components')

    server = RuntimeServer(runtime, 4556)

    on_response = MagicMock()
    client = Client(on_response)

    gevent.spawn(server.start)
    client.connect('tcp://localhost', 4556)

    gevent.sleep(.01)
    expected = [
        {
            'protocol': 'runtime',
            'command': 'runtime',
            'payload': runtime.get_runtime_meta()
        }
    ]

    next_message_id = itertools.count(2).next
    for component_spec in runtime.get_all_component_specs():
        expected.append({
            'protocol': 'component',
            'command': 'component',
            'payload': component_spec,
            'message_id': next_message_id()
        })

    for i, call in enumerate(on_response.call_args_list):
        message = call[0][0].to_dict()
        expected_message = expected[i]
        assert message['payload'] == expected_message['payload']

    test_graph = {
        'protocol': 'graph',
        'command': 'addgraph',
        'payload': {
            'id': 'testgraph',
            'name': 'testgraph'
        },
        'id': uuid.uuid1()
    }

    client.send(Message(**test_graph))
    gevent.sleep(.01)
    on_response.reset_mock()

    watch_graph = {
        'protocol': 'graph',
        'command': 'watch',
        'payload': {
            'id': 'testgraph'
        },
        'id': uuid.uuid1()
    }

    client.send(Message(**watch_graph))
    gevent.sleep(.01)

    expected_clear = {
        'protocol': 'graph',
        'command': 'clear',
        'payload': {
            'id': 'testgraph',
            'name': 'testgraph',
        }
    }
    clear_message = on_response.call_args_list[0][0][0].to_dict()
    assert clear_message['payload'] == expected_clear['payload']

    status_message = on_response.call_args_list[1][0][0].to_dict()
    assert status_message['protocol'] == 'network'
    assert status_message['command'] == 'status'
    assert status_message['payload']['graph'] == 'testgraph'
    assert status_message['payload']['running'] == False
    assert status_message['payload']['started'] == False

    genarray = {
        'protocol': 'graph',
        'command': 'addnode',
        'payload': {
            'graph': 'testgraph',
            'id': 'node1',
            'component': 'tests.components/GenerateArray'
        },
        'id': uuid.uuid1()
    }
    client.send(Message(**genarray))

    gevent.sleep(.01)

    genarray_message = on_response.call_args[0][0].to_dict()
    assert genarray_message['payload'] == genarray['payload']


    repeat = {
        'protocol': 'graph',
        'command': 'addnode',
        'payload': {
            'graph': 'testgraph',
            'id': 'node2',
            'component': 'tests.components/Repeat'
        },
        'id': uuid.uuid1()
    }
    client.send(Message(**repeat))
    edge = {
        'protocol': 'graph',
        'command': 'addedge',
        'payload': {
            'graph': 'testgraph',
            'src': {
                'node': 'node1',
                'port': 'OUT'
            },
            'tgt': {
                'node': 'node2',
                'port': 'in'
            }
        },
        'id': uuid.uuid1()
    }

    client.send(Message(**edge))
    gevent.sleep(.01)

    edge_message = on_response.call_args[0][0].to_dict()
    assert edge_message['payload']['src'] == edge['payload']['src']
    assert edge_message['payload']['tgt'] == edge['payload']['tgt']

    on_response.reset_mock()

    addinitial = {
        'protocol': 'graph',
        'command': 'addinitial',
        'payload': {
            'graph': 'testgraph',
            'src': {
                'data': ['lol']
            },
            'tgt': {
                'node': 'node1',
                'port': 'COUNT'
            }
        },
        'id': uuid.uuid1()
    }

    client.send(Message(**addinitial))
    gevent.sleep(.01)

    error_message = on_response.call_args[0][0].to_dict()
    assert error_message['command'] == 'error'

    client.disconnect()
    server.stop()

