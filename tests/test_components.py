import pytest

import gevent.monkey
import gevent

import rill.engine.utils
rill.engine.utils.patch()

from rill.engine.exceptions import FlowError
from rill.engine.network import Network, Graph, run_graph
from rill.engine.outputport import OutputPort
from rill.engine.inputport import InputPort
from rill.engine.runner import ComponentRunner
from rill.engine.port import OUT_NULL, IN_NULL
from rill.engine.component import Component
from rill.decorators import inport, outport, component, subnet

from tests.utils import names
from tests.components import *
from tests.subnets import PassthruNet

from rill.components.basic import Counter, Sort, Inject, Repeat, Cap, Kick
from rill.components.filters import First
from rill.components.merge import Group
from rill.components.split import RoundRobinSplit, Replicate
from rill.components.math import Add
from rill.components.files import ReadLines, WriteLines, Write
from rill.components.timing import SlowPass
from rill.components.text import Prefix, LineToWords, LowerCase, StartsWith, WordsToLine

import logging
ComponentRunner.logger.setLevel(logging.DEBUG)


is_patched = rill.engine.utils.is_patched
requires_patch = pytest.mark.skipif(not is_patched,
                                    reason='requires patched gevent')

# TODO
# - test inport closing while it is waiting in a `while self.is_empty()` loop
# - define what happens when sending to a closed InputInterface: closed state
#   currently seems to be disregarded
# - test whether an InitializationConnection repeats the same value: it
#   re-opens each time it is activated, so that suggests it is the behavior, but
#   that implies that a component written to use an InitConn won't work with
#   a Conn. i.e. if a component closes that connection, there should be no more
#   values, regardless of whether it is an InitConn or Conn
# - test differences between loopers and non-loopers.
#     - is reactivating a looper safe? for example, what happens when a looper
#       calls receive_once() on a Connection, but is later re-activated?  does
#       it get the value again?
#     - what about NULL ports. do they fire once or multiple times?


def runtwice(grph, *pairs):
    """Run a network twice to ensure that it shuts down properly"""
    network = Network(grph)
    with gevent.Timeout(2):
        network.go()
        for real, ref in pairs:
            assert real.values == ref
            real.values = []
        network.go()
        for real, ref in pairs:
            assert real.values == ref


@pytest.fixture(params=[dict(default_capacity=1),
                        dict(default_capacity=2)],
                ids=['graph.capacity=1', 'graph.capacity=2'])
def graph(request):
    return Graph(**request.param)


# non-loopers are only safe when using gevent.monkey.patch_all().
# FIXME: find out why... OR stop supporting component reactivation (i.e. non-loopers)
@pytest.fixture(params=['looper', requires_patch('nonlooper')])
def discard(request):
    return DiscardLooper if request.param == 'looper' else Discard


def test_aggregation(graph, discard):
    graph.add_component("Generate", GenerateTestData)
    dis = graph.add_component("Discard", discard)
    graph.add_component("Counter", Counter)

    graph.connect("Generate.OUT", "Counter.IN")
    graph.connect("Counter.COUNT", "Discard.IN")
    graph.initialize(5, "Generate.COUNT")
    runtwice(graph, (dis, [5]))


@pytest.mark.xfail(reason='schematics objects will not pickle')
# @requires_patch
def test_pickle(graph):
    graph.add_component("Generate", GenerateTestData, COUNT=5)
    passthru = graph.add_component("Pass", SlowPass, DELAY=0.1)
    count = graph.add_component("Counter", Counter)
    dis1 = graph.add_component("Discard1", Discard)
    dis2 = graph.add_component("Discard2", Discard)

    graph.connect("Generate.OUT", "Pass.IN")
    graph.connect("Pass.OUT", "Counter.IN")
    graph.connect("Counter.COUNT", "Discard1.IN")
    graph.connect("Counter.OUT", "Discard2.IN")

    net = Network(graph)
    netrunner = gevent.spawn(net.go)
    try:
        with gevent.Timeout(.35) as timeout:
            gevent.wait([netrunner])
    except gevent.Timeout:
        print(count.execute)

    assert count.count == 4
    assert dis2.values == ['000005', '000004', '000003', '000002']

    import pickle
    # dump before terminating to get the runner statuses
    data = pickle.dumps(net)
    # FIXME: do we need to auto-terminate inside wait_for_all if there is an error?
    net.terminate()
    net.wait_for_all()
    # gevent.wait([netrunner])  # this causes more packets to be sent. no good.
    net2 = pickle.loads(data)
    assert net2.graph.component('Counter').count == 4
    assert net2.graph.component('Discard2').values == ['000005', '000004', '000003', '000002']
    net2.go(resume=True)
    assert net2.graph.component('Counter').count == 5
    assert net2.graph.component('Discard2').values == ['000005', '000004', '000003', '000002', '000001']

    # FIXME: test the case where a packet is lost due to being shut-down
    # packet counting should catch it.  to test, use this component, which
    # can be killed while it sleeps holding a packet:
    # @component
    # @outport("OUT")
    # @inport("IN")
    # @inport("DELAY", type=float, required=True)
    # def SlowPass(IN, DELAY, OUT):
    #     """
    #     Pass a stream of packets to an output stream with a delay between packets
    #     """
    #     delay = DELAY.receive_once()
    #     for p in IN:
    #         time.sleep(delay)
    #         OUT.send(p)


def test_component_with_inheritance():
    @inport('IN')
    @outport('OUT')
    class A(Component):
        def execute(self):
            pass

    @inport('OPT', type=int)
    class B(A):
        pass

    assert names(B.port_definitions().values(), include_null=True) == [
        IN_NULL, 'IN', 'OPT', OUT_NULL, 'OUT']

    graph = Graph()
    b = graph.add_component('b', B)
    assert names(b.ports, include_null=True) == [
        IN_NULL, 'IN', 'OPT', OUT_NULL, 'OUT']


def test_component_spec():
    expected = {
        'name': 'tests.components/GenerateTestData',
        'description': '"Generates stream of packets under control of a counter',
        'inPorts': [
            {
                'addressable': False,
                'static': False,
                'description': '',
                'id': 'wait',
                'required': False,
                'type': 'bang'
            },
            {
                'addressable': False,
                'static': False,
                'default': 1,
                'description': 'Count of packets to be generated',
                'id': 'COUNT',
                'required': False,
                'schema': {'type': 'int'}
            }
        ],
        'outPorts': [
            {
                'addressable': False,
                'description': '',
                'id': 'done',
                'required': False,
                'type': 'bang'
            },
            {
                'addressable': False,
                'description': 'Generated stream',
                'id': 'OUT',
                'required': False,
                'schema': {'type': 'string'}
            }
        ],
        'subgraph': False
    }
    results = GenerateTestData.get_spec()
    assert results == expected


@pytest.mark.xfail(is_patched, reason='order is ACB instead of ABC')
# @requires_patch
def test_multiple_inputs(graph, discard):
    graph.add_component("GenerateA", GenerateTestData, COUNT=5)
    graph.add_component("PrefixA", Prefix, PRE='A')
    graph.add_component("GenerateB", GenerateTestData, COUNT=5)
    graph.add_component("PrefixB", Prefix, PRE='B')
    graph.add_component("GenerateC", GenerateTestData, COUNT=5)
    graph.add_component("PrefixC", Prefix, PRE='C')
    dis = graph.add_component("Discard", discard)

    graph.connect("GenerateA.OUT", "PrefixA.IN")
    graph.connect("GenerateB.OUT", "PrefixB.IN")
    graph.connect("GenerateC.OUT", "PrefixC.IN")
    graph.connect("PrefixA.OUT", "Discard.IN")
    graph.connect("PrefixB.OUT", "Discard.IN")
    graph.connect("PrefixC.OUT", "Discard.IN")
    runtwice(graph,
        (dis,
         [
          'A000005', 'A000004', 'A000003', 'A000002', 'A000001',
          'B000005', 'B000004', 'B000003', 'B000002', 'B000001',
          'C000005', 'C000004', 'C000003', 'C000002', 'C000001',
         ]))


def test_intermediate_non_looper(graph, discard):
    """non-looping components continue processing as long as there are
    upstream packets"""
    graph.add_component("Generate", GenerateTestData)
    graph.add_component("Passthru", Passthru)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate.OUT", "Passthru.IN")
    # Passthru is a non-looper
    graph.connect("Passthru.OUT", "Discard.IN")
    graph.initialize(5, "Generate.COUNT")
    runtwice(graph,
        (dis, ['000005', '000004', '000003', '000002', '000001']))


def test_basic_connections():
    graph = Graph()
    count = graph.add_component("Count", Counter)
    dis = graph.add_component("Discard1", Discard)
    graph.add_component("Discard2", Discard)

    # ports are stored in the order they are declared
    assert names(count.outports) == ['OUT', 'COUNT']
    # assert list(count.ports._ports.keys()) == ['OUT', 'COUNT', 'NULL']

    # nothing is connected yet
    assert count.ports['OUT'].is_connected() is False
    assert dis.ports['IN'].is_connected() is False

    with pytest.raises(FlowError):
        # non-existent port
        graph.connect("Count.FOO", "Discard1.IN")

    with pytest.raises(FlowError):
        # non-existent Component
        graph.connect("Foo.FOO", "Discard1.IN")

    # make a connection
    graph.connect("Count.OUT", "Discard1.IN")

    # outports can have more than one connection
    graph.connect("Count.OUT", "Discard2.IN")

    with pytest.raises(FlowError):
        # connected ports cannot be initialized
        graph.initialize(1, "Discard1.IN")

    assert type(count.ports['OUT']) is OutputPort
    assert type(dis.ports['IN']) is InputPort
    assert count.ports['OUT'].is_connected() is True
    assert dis.ports['IN'].is_connected() is True

    # FIXME: move this to a different test
    net = Network(graph)
    net.reset()
    net._build_runners()
    net._open_ports()
    assert count.ports['OUT'].component is count
    assert isinstance(count.ports['OUT'].sender, ComponentRunner)
    assert dis.ports['IN'].component is dis
    assert isinstance(dis.ports['IN'].receiver, ComponentRunner)


def test_fixed_size_array(graph, discard):
    graph.add_component("Generate", GenerateFixedSizeArray)
    dis1 = graph.add_component("Discard1", discard)
    dis2 = graph.add_component("Discard2", discard)
    graph.connect("Generate.OUT[0]", "Discard1.IN")
    graph.connect("Generate.OUT[1]", "Discard2.IN")
    graph.initialize(5, "Generate.COUNT")
    run_graph(graph)
    assert dis1.values == ['000005', '000004', '000003', '000002', '000001']
    assert dis2.values == ['000005', '000004', '000003', '000002', '000001']


def test_unconnected_output_array_element(graph, discard):
    """if an array port does not specify fixed_size, some elements may remain
    unconnected
    """
    graph.add_component("generate", GenerateTestData)
    graph.add_component("replicate", Replicate)
    graph.add_component("discard", discard)
    graph.connect("generate.OUT", "replicate.IN")
    graph.connect("replicate.OUT[2]", "discard.IN")
    graph.initialize(10, "generate.COUNT")

    run_graph(graph)


def test_optional_fixed_size_array(graph, discard):
    """if an array port specifies fixed_size, some elements may remain
    unconnected if the array port is not required"""
    # fixed_size of 4
    graph.add_component("Generate", GenerateOptionalFixedArray)
    dis1 = graph.add_component("Discard1", discard)
    dis2 = graph.add_component("Discard2", discard)
    # only two connected
    graph.connect("Generate.OUT", "Discard1.IN")
    graph.connect("Generate.OUT[2]", "Discard2.IN")
    graph.initialize(5, "Generate.COUNT")
    run_graph(graph)

    assert dis1.values == ['000005', '000004', '000003', '000002', '000001']
    assert dis2.values == ['000005', '000004', '000003', '000002', '000001']


def test_null_ports(graph, tmpdir, discard):
    """null ports ensure proper ordering of components"""
    tempfile = str(tmpdir.join('data.txt'))
    graph.add_component("Generate", GenerateTestData, COUNT=5)
    write = graph.add_component("Write", WriteLines, FILEPATH=tempfile)
    read = graph.add_component("Read", ReadLines, FILEPATH=tempfile)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate.OUT", "Write.IN")
    graph.connect(write.port(OUT_NULL), read.port(IN_NULL))
    graph.connect("Read.OUT", "Discard.IN")
    run_graph(graph)

    assert dis.values == ['000005', '000004', '000003', '000002', '000001']


def test_null_ports2(graph, tmpdir, discard):
    """null ports ensure proper ordering of components"""
    # in this case, we give Read an input that it can consume, but the null
    # port should prevent it from happening until Write is done
    graph.add_component("Data", GenerateTestData, COUNT=2)
    graph.add_component("FileNames", GenerateTestData, COUNT=2)
    graph.add_component("Prefix", Prefix, PRE=str(tmpdir.join('file.')))
    graph.add_component("Replicate", Replicate)
    write = graph.add_component("Write", Write)
    read = graph.add_component("Read", ReadLines)

    dis = graph.add_component("Discard", discard)

    graph.connect("Data.OUT", "Write.IN")
    graph.connect("FileNames.OUT", "Prefix.IN")
    graph.connect("Prefix.OUT", "Replicate.IN")
    graph.connect("Replicate.OUT[0]", "Write.FILEPATH")
    graph.connect("Replicate.OUT[1]", "Read.FILEPATH")

    # Note that it's probably more correct to use Write.OUT which sends after
    # each file written, instead of Write.NULL which sends after all files are
    # written, but we use the latter because it reveals an interesting deadlock
    # issue with Replicate (see notes in that component for more info)
    graph.connect(write.port(OUT_NULL), read.port(IN_NULL))
    graph.connect("Read.OUT", "Discard.IN")
    run_graph(graph)

    assert dis.values == ['000002', '000001']


def test_inport_closed(graph, discard):
    graph.add_component("Generate", GenerateTestData, COUNT=5)
    graph.add_component("First", First)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate.OUT", "First.IN")
    graph.connect("First.OUT", "Discard.IN")
    run_graph(graph)

    assert dis.values == ['000005']


def test_inport_closed_propagation(graph, discard):
    """If a downstream inport is closed, the upstream component should shut
    down"""
    graph.add_component("Generate", GenerateTestDataDumb, COUNT=5)
    graph.add_component("First", First)  # receives one packet then closes IN
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate.OUT", "First.IN")
    graph.connect("First.OUT", "Discard.IN")

    run_graph(graph)

    assert dis.values == ['000005']


def test_synced_receive1(graph, discard):
    """
    components using `fn.synced` (Group) close all synced inports on the first
    exhausted inport.
    """
    graph.add_component("Generate5", GenerateTestData, COUNT=5)
    graph.add_component("Generate3", GenerateTestData, COUNT=3)
    # contains the call to synced:
    graph.add_component("Merge", Group)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate5.OUT", "Merge.IN[0]")
    graph.connect("Generate3.OUT", "Merge.IN[1]")
    graph.initialize('initial', "Merge.IN[2]")
    graph.connect("Merge.OUT", "Discard.IN")
    run_graph(graph)

    assert dis.values == [
        ('000005', '000003', 'initial'),
        ('000004', '000002', 'initial'),
        ('000003', '000001', 'initial'),
    ]


def test_synced_receive2(graph, discard):
    """
    components using `fn.synced` (Group) close all synced inports on the first
    exhausted inport.
    """
    graph.add_component("Generate5", GenerateTestData, COUNT=5)
    graph.add_component("Generate3", GenerateTestData, COUNT=3)
    # repeat infinitely:
    graph.add_component("Repeat", Repeat, IN='initial')  # FIXME: this fails when COUNT=1
    # contains the call to synced:
    graph.add_component("Merge", Group)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate5.OUT", "Merge.IN[0]")
    graph.connect("Generate3.OUT", "Merge.IN[1]")
    graph.connect('Repeat.OUT', "Merge.IN[2]")
    graph.connect("Merge.OUT", "Discard.IN")
    run_graph(graph)

    assert dis.values == [
        ('000005', '000003', 'initial'),
        ('000004', '000002', 'initial'),
        ('000003', '000001', 'initial')
    ]


def test_synced_receive3(graph, discard):
    # contains the call to synced:
    graph.add_component("Merge", Group)
    dis = graph.add_component("Discard", discard)
    graph.connect("Merge.OUT", "Discard.IN")
    run_graph(graph)

    assert dis.values == []


def test_merge_sort_drop(graph, discard):
    graph.add_component("_Generate", GenerateTestData, COUNT=4)
    graph.add_component("_Generate2", GenerateTestData, COUNT=4)
    graph.add_component("_Sort", Sort)
    dis = graph.add_component("_Discard", discard)
    graph.add_component("Passthru", Passthru)
    graph.add_component("Passthru2", Passthru)
    graph.connect("_Generate2.OUT", "Passthru2.IN")
    graph.connect("_Generate.OUT", "Passthru.IN")
    graph.connect("Passthru2.OUT", "Passthru.IN")
    graph.connect("Passthru.OUT", "_Sort.IN")
    graph.connect("_Sort.OUT", "_Discard.IN")
    run_graph(graph)
    assert dis.values == [
        '000001', '000001', '000002', '000002', '000003', '000003',
        '000004', '000004'
    ]


def test_inport_default():
    graph = Graph()
    graph.add_component("Generate", GenerateTestData)
    dis = graph.add_component("Discard", Discard)
    graph.connect("Generate.OUT", "Discard.IN")
    run_graph(graph)
    assert dis.values == ['000001']


def test_fib(graph):
    """
    Make fibonacci sequence from completely reusable parts.
    """
    graph.add_component("Add", Add)
    # oscillates between its two inputs:
    graph.add_component("Split", RoundRobinSplit)
    # kicks off the initial values:
    graph.add_component("Zero", Inject, CONST=0)
    graph.add_component("One", Inject, CONST=1)
    # doubles up the streams because they are consumed in pairs by Add:
    #   Split.OUT[0]: 0 1 1 3 3 8 8
    #   Split.OUT[1]: 1 1 2 2 5 5 13
    graph.add_component("Repeat1", Repeat, COUNT=2)
    graph.add_component("Repeat2", Repeat, COUNT=2)
    # set a max value to the sequence
    graph.add_component("Cap", Cap, MAX=30)
    pthru = graph.add_component("Passthru", Passthru)

    # need to use inject because you can't mix static value and connection
    graph.connect("Zero.OUT", "Add.IN1")
    graph.connect("Repeat1.OUT", "Add.IN1")
    graph.connect("Split.OUT[0]", "Repeat1.IN")

    # need to use inject because you can't mix static value and connection
    graph.connect("One.OUT", "Repeat2.IN")
    graph.connect("Split.OUT[1]", "Repeat2.IN")

    graph.connect("Repeat2.OUT", "Add.IN2")
    graph.connect("Add.OUT", "Cap.IN")
    graph.connect("Cap.OUT", "Passthru.IN")
    # complete the loop:
    graph.connect("Passthru.OUT", "Split.IN")
    run_graph(graph)
    # FIXME: where's the 0?
    assert pthru.values == [1, 2, 3, 5, 8, 13, 21]


def test_readme_example(graph, discard):
    graph.add_component("LineToWords", LineToWords, IN="HeLLo GoodbYe WOrld")
    graph.add_component("StartsWith", StartsWith, TEST='G')
    graph.add_component("LowerCase", LowerCase)
    graph.add_component("WordsToLine", WordsToLine)
    dis = graph.add_component("Discard", discard)

    graph.connect("LineToWords.OUT", "StartsWith.IN")
    graph.connect("StartsWith.REJ", "LowerCase.IN")
    graph.connect("LowerCase.OUT", "WordsToLine.IN")
    graph.connect("WordsToLine.OUT", "Discard.IN")
    run_graph(graph)
    assert dis.values == ['hello world']


def test_first(graph, discard):
    graph.add_component("Generate", GenerateTestData, COUNT=4)
    graph.add_component("First", First)
    dis = graph.add_component("Discard", discard)

    graph.connect("Generate.OUT", "First.IN")
    graph.connect("First.OUT", "Discard.IN")
    run_graph(graph)
    assert dis.values == ['000004']

# def test_self_starting():
#     # creates a cycle
#     self.connect(self.component("Copy", Copy).port("OUT"),
#                  self.component("CopySSt", CopySSt).port("IN"))
#     self.connect(self.component("CopySSt").port("OUT"),
#                  self.component("Copy").port("IN"))


def test_network_apply():
    graph = Graph()
    graph.add_component('Add1', Add)
    graph.add_component('Add2', Add)

    graph.connect('Add1.OUT', 'Add2.IN1')

    graph.export('Add1.IN1', 'IN1')
    graph.export('Add1.IN2', 'IN2')
    graph.export('Add2.IN2', 'IN3')
    graph.export('Add2.OUT', 'OUT')

    outputs = run_graph(graph, {
        'IN1': 1,
        'IN2': 3,
        'IN3': 6
    }, capture_results=True)

    assert outputs['OUT'] == 10


def test_network_apply_with_outputs():
    graph = Graph()
    graph.add_component('Add1', Add)
    graph.add_component('Add2', Add)
    graph.add_component('Kick', Kick)

    graph.connect('Add1.OUT', 'Add2.IN1')

    graph.export('Add1.IN1', 'IN1')
    graph.export('Add1.IN2', 'IN2')
    graph.export('Add2.IN2', 'IN3')
    graph.export('Add2.OUT', 'OUT')
    graph.export('Kick.OUT', 'Kick_OUT')

    outputs = run_graph(graph, {
        'IN1': 1,
        'IN2': 3,
        'IN3': 6
    }, ['OUT'])

    assert outputs == {'OUT': 10}

