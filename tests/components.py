import re
from rill import *
from rill.fn import range
from schematics.models import Model
from schematics.types import StringType, IntType, ModelType, ListType
from schematics.types import BooleanType

from random import random


@component(pass_context=True)
@inport("IN")
@outport("OUT")
def Passthru(self, IN, OUT):
    """Pass a stream of packets to an output stream"""
    self.values = []
    for p in IN.iter_packets():
        self.values.append(p.get_contents())
        OUT.send(p)


@inport("IN", description="Stream of packets to be discarded")
class Discard(Component):
    def execute(self):
        p = self.ports.IN.receive()
        if p is None:
            return
        self.packets.append(p)
        self.values.append(p.get_contents())
        self.drop(p)

    def init(self):
        self.values = []
        self.packets = []


@inport("IN", description="Stream of packets to be discarded")
class DiscardLooper(Component):
    def execute(self):
        for p in self.ports.IN:
            self.packets.append(p)
            self.values.append(p.get_contents())
            self.drop(p)

    def init(self):
        self.values = []
        self.packets = []


@component
@outport("OUT", description="Generated stream", type=str)
@inport("COUNT", description="Count of packets to be generated", type=int,
        default=1)
def GenerateTestData(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for i in range(count, 0, -1):
        s = "%06d" % i
        if not OUT.send(s):
            break


@component
@outport("OUT", description="Generated stream", type=str)
@inport("COUNT", description="Count of packets to be generated", type=int)
def GenerateTestDataDumb(COUNT, OUT):
    """"Generates stream of packets under control of a counter

    Fails to break if OUT is closed
    """
    count = COUNT.receive_once()
    if count is None:
        return

    for i in range(count, 0, -1):
        s = "%06d" % i
        OUT.send(s)


@component
@outport("OUT", required=False, fixed_size=4, description="Generated stream",
         type=str, array=True)
@inport("COUNT", description="Count of packets to be generated",
        type=int)
def GenerateOptionalFixedArray(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for outport in OUT:
        logger.info("writing to port %s" % outport)
        for i in range(count, 0, -1):
            s = "%06d" % i
            if outport.is_closed():
                break
            outport.send(s)


@component
@outport("OUT", required=False, description="Generated stream",
         type=str, array=True)
@inport("COUNT", description="Count of packets to be generated",
        type=int)
def GenerateArray(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for outport in OUT:
        logger.info("writing to port %s" % outport)
        for i in range(count, 0, -1):
            s = "%06d" % i
            if outport.is_closed():
                break
            outport.send(s)


@component
@outport("OUT", required=True, fixed_size=2, description="Generated stream",
         type=str, array=True)
@inport("COUNT", description="Count of packets to be generated",
        type=int)
def GenerateFixedSizeArray(COUNT, OUT):
    """"Generates stream of packets under control of a counter"""
    count = COUNT.receive_once()
    if count is None:
        return

    for outport in OUT:
        for i in range(count, 0, -1):
            s = "%06d" % i
            if OUT.is_closed():
                break
            #  if (out_port_array[k].is_connected()):
            outport.send(s)
            #  else:
            #    self.drop(p)
            #


@component
@outport("OUT", type=str)
@inport("COUNT", type=int)
def GenSS(COUNT, OUT):
    """Generates stream of 5-packet substreams under control of a counter
    """
    count = COUNT.receive_once()
    OUT.send(Packet.Type.OPEN)

    for i in range(count):
        s = "%06d" % (count - i)
        OUT.send(s)
        if i < count - 1:  # prevent empty bracket pair at end
            if i % 5 == 5 - 1:
                OUT.send(Packet.Type.CLOSE)
                OUT.send(Packet.Type.OPEN)
    OUT.send(Packet.Type.CLOSE)

# for fbp-test

@component
@inport("in", description="Stream of packets to be discarded")
def Drop(IN):
    """Discards all incoming packets"""
    IN.receive().drop()


@component
@inport("in")
@outport("out")
def Repeat(IN, OUT):
    """Pass a stream of packets to an output stream"""
    # make it a non-looper - for testing
    p = IN.receive()
    OUT.send(p)


class Person(Model):
    name = StringType(required=True)
    age = IntType(default=0, min_value=0, max_value=200)
    favorite_color = StringType(choices=[
        'cyan',
        'magenta',
        'chartreuse'
    ])
    married = BooleanType()
    phone_number = StringType(
        regex=re.compile(r'\d{3}-\d{4}'),
        max_length=8,
        min_length=8
    )


class Company(Model):
    ceo = ModelType(Person)
    address = StringType()
    employees = ListType(ModelType(Person))


@component(pass_context=True)
@inport("IN", type=ModelType(Person))
@outport("OUT")
def PassthruPerson(self, IN, OUT):
    """Pass a stream of packets to an output stream"""
    self.values = []
    for p in IN.iter_packets():
        self.values.append(p.get_contents())
        OUT.send(p)


@component(pass_context=True)
@inport("IN", type=ModelType(Company))
@outport("OUT")
def PassthruCompany(self, IN, OUT):
    """Pass a stream of packets to an output stream"""
    self.values = []
    for p in IN.iter_packets():
        self.values.append(p.get_contents())


@component(pass_context=True)
@inport("IN", type=ListType(ModelType(Person)))
@outport("OUT")
def PassthruPeople(self, IN, OUT):
    """Pass a stream of packets to an output stream"""
    self.values = []
    for p in IN.iter_packets():
        self.values.append(p.get_contents())
        OUT.send(p)
        OUT.send(p)


@component()
@inport("COUNT", type=int)
@outport("OUT", type=float)
def GenerateRandom(COUNT, OUT):
    """Generate a stream of random numbers"""
    count = COUNT.receive_once()

    for i in range(count):
        OUT.send(random())


@component()
@inport("BOOL", type=bool, static=True)
@inport("BOOL_STREAM", type=bool)
@inport("STR", type=str)
@inport("CHOICE", type=StringType(choices=[
    'cyan',
    'magenta',
    'chartreuse'
]), static=True)
@inport("CHOICE_STREAM", type=StringType(choices=[
    'cyan',
    'magenta',
    'chartreuse'
]))
@outport("OUT", type=str)
def TestFields(BOOL, STR, CHOICES):
    """Test boolean fields"""
    return

