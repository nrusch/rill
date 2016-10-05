from __future__ import absolute_import, print_function

import pytest

import rill.engine.utils
rill.engine.utils.patch()

from rill.engine.network import Network, Graph, run_graph
from rill.engine.runner import ComponentRunner
from rill.engine.subnet import make_subgraph
from rill.components.basic import Capture
from rill.components.timing import SlowPass
from tests.components import *
from tests.subnets import PassthruNet
from tests.test_components import graph, discard

import logging
ComponentRunner.logger.setLevel(logging.DEBUG)


# we use socket as our canary
is_patched = rill.engine.utils.is_patched
requires_patch = pytest.mark.skipif(not is_patched,
                                    reason='requires patched gevent')


@requires_patch
def test_subnet_with_substreams(graph, discard):
    # tracing = True
    gen = graph.add_component("Generate", GenSS, COUNT=15)
    passnet = graph.add_component("Subnet", PassthruNet)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate.OUT", "Subnet.IN")
    graph.connect("Subnet.OUT", "Discard.IN")

    # graph.reset()
    # graph._build_runners()
    # graph._open_ports()
    # assert isinstance(passnet.ports['OUT'].component, Passthru)
    # assert isinstance(passnet.ports['OUT'].sender, ComponentRunner)
    # assert isinstance(passnet.ports['IN'].component, Passthru)
    # assert isinstance(passnet.ports['IN'].receiver, ComponentRunner)


    # FIXME: need a separate test for the NULL port behavior
    # graph.connect("Subnet.*SUBEND", "WTC.IN")
    run_graph(graph)

    assert dis.values == [
        '', '000015', '000014', '000013', '000012', '000011', '',
        '', '000010', '000009', '000008', '000007', '000006', '',
        '', '000005', '000004', '000003', '000002', '000001', ''
    ]


def test_subnet_decorator():
    @outport("OUT")
    @inport("IN", description='an input')
    @subnet
    def DecoratedPassNet(sub):
        sub.add_component('Head', SlowPass, DELAY=0.01)
        sub.add_component('Tail', SlowPass, DELAY=0.01)

        sub.connect('Head.OUT', 'Tail.IN')

        sub.export('Head.IN', 'IN')
        sub.export('Tail.OUT', 'OUT')

    assert issubclass(DecoratedPassNet, SubGraph)
    assert DecoratedPassNet._inport_definitions[0].description == 'an input'
    assert DecoratedPassNet.inport_definitions['IN'].description == 'an input'

    graph = Graph()

    gen = graph.add_component("Generate", GenSS, COUNT=5)
    passnet = graph.add_component("Subnet", DecoratedPassNet)
    dis = graph.add_component("Discard", Discard)

    graph.connect("Generate.OUT", "Subnet.IN")
    graph.connect("Subnet.OUT", "Discard.IN")

    run_graph(graph)

    assert dis.values == [
        '', '000005', '000004', '000003', '000002', '000001', '',
    ]


# def test_make_subgraph():


def test_name():
    root = Graph(name='root')
    passnet = root.add_component("Subnet", PassthruNet)
    child = passnet.subgraph.component("Pass")

    assert passnet._runner is None
    assert passnet.get_parents() == []
    assert passnet.get_full_name() == 'Subnet'

    assert child._runner is None
    assert child.get_parents() == []
    assert child.get_full_name() == 'Pass'

    # a component's full name is not available until all of the graphs' and
    # sub-graphs' runners have been initialized.
    # this is the result of two conflicting requirements:
    #  - graphs should not be modified during execution.  in other words,
    #    there should be no side-effects.  this means that a graph cannot have
    #    a parent graph attribute, we must track that relationship elsewhere.
    #  - delay building runners until absolutely necessary.  this means that
    #    a component, which *can* have a parent, is not available until the
    #    graph is executed.
    # we might want to revisit this if it becomes a nuisance.
    net = Network(root)
    net._build_runners()
    assert passnet._runner is not None
    assert passnet.get_parents() == [root]

    # building the network does a deep copy of the graph and its components
    snet = passnet._build_network()
    assert snet.graph is not passnet.subgraph
    assert child is passnet.subgraph.component('Pass')
    child_copy = snet.graph.component('Pass')
    assert child is not child_copy
    assert type(child) is type(child_copy)

    snet._build_runners()
    assert child_copy._runner is not None
    assert snet.parent_network is not None
    assert child_copy.get_full_name() == 'root.Subnet.Pass'


def test_initialize_subnet():
    @outport("OUT")
    @inport("IN")
    @subnet
    def PassNet(sub):
        sub.add_component('Head', Passthru)
        sub.add_component('Tail', Passthru)

        sub.connect('Head.OUT', 'Tail.IN')

        sub.export('Head.IN', 'IN')
        sub.export('Tail.OUT', 'OUT')

    graph = Graph()
    capture = graph.add_component('Capture', Capture)

    graph.add_component('Pass', PassNet)
    graph.initialize(5, 'Pass.IN')
    graph.connect('Pass.OUT', 'Capture.IN')

    run_graph(graph)

    assert capture.value == 5


def test_make_subgraph():
    sub = Graph()
    sub.add_component('Head', Passthru)
    sub.add_component('Tail', Passthru)

    sub.connect('Head.OUT', 'Tail.IN')

    sub.export('Head.IN', 'IN')
    sub.export('Tail.OUT', 'OUT')

    PassNet = make_subgraph(sub, name='PassNet')

    assert len(PassNet.inport_definitions) == 2
    assert len(PassNet.outport_definitions) == 2

    graph = Graph()
    capture = graph.add_component('Capture', Capture)

    graph.add_component('Pass', PassNet)
    graph.initialize(5, 'Pass.IN')
    graph.connect('Pass.OUT', 'Capture.IN')

    run_graph(graph)

    assert capture.value == 5

def test_get_spec():
    sub = Graph()
    sub.add_component('Head', Passthru)
    sub.add_component('Tail', Passthru)

    sub.connect('Head.OUT', 'Tail.IN')

    sub.export('Head.IN', 'IN')
    sub.export('Tail.OUT', 'OUT')

    PassNet = make_subgraph(sub, name='PassNet')
    spec = PassNet.get_spec()

    assert spec['name'] == 'abc/PassNet'
    assert len(spec['inPorts']) == 2
    assert len(spec['outPorts']) == 2


def test_rename_exports():
    graph = Graph()
    graph.add_component('Head', Passthru)
    graph.add_component('Tail', Passthru)

    graph.connect('Head.OUT', 'Tail.IN')

    graph.export('Head.IN', 'IN')
    graph.export('Tail.OUT', 'OUT')

    graph.set_inport_metadata('IN', {'x': 100})
    graph.set_outport_metadata('OUT', {'y': 100})

    graph.rename_inport('IN', 'HAPPY')
    assert graph.inports['HAPPY'] == graph.get_component_port('Head.IN')
    assert graph.inport_metadata['HAPPY'] == {
        'x': 100
    }
    assert not graph.inports.get('IN', False)
    assert not graph.inport_metadata.get('IN', False)

    graph.rename_outport('OUT', 'SAD')
    assert graph.outports['SAD'] == graph.get_component_port('Tail.OUT')
    assert graph.outport_metadata['SAD'] == {
        'y': 100
    }
    assert not graph.outports.get('OUT', False)
    assert not graph.outport_metadata.get('OUT', False)

