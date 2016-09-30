import itertools
from itertools import count, cycle, repeat

try:
    zip = itertools.izip  # PY2
    zip_longest = itertools.izip_longest
    filter = itertools.ifilter
    map = itertools.imap
    range = xrange
except AttributeError:
    zip = zip
    zip_longest = itertools.zip_longest
    filter = filter
    map = map
    range = range


def current_component_runner():
    """
    Get the active Component Runner

    Returns
    -------
    ``rill.engine.runner.ComponentRunner``
    """
    import gevent
    import rill.engine.runner
    greenlet = gevent.getcurrent()
    assert isinstance(greenlet, rill.engine.runner.ComponentRunner)
    return greenlet


def current_component():
    """
    Get the active Component Runner

    Returns
    -------
    ``rill.engine.component.Component``
    """
    return current_component_runner().component


def synced(*ports):
    """
    Synchronize the receipt of packets from `ports`.

    This returns an iterable ``PortCollection`` instance that behaves like the
    function `izip` (`zip` in python3), but handles closing all the ports when
    the first drained port is encountered.

    Parameters
    ----------
    ports

    Returns
    ------
    ``rill.engine.inputport.SynchronousInputCollection``
    """
    from rill.engine.inputport import SynchronizedInputCollection
    return SynchronizedInputCollection(ports)


def eager_merged(*ports):
    from rill.engine.inputport import EagerInputCollection
    return EagerInputCollection(ports)


def load_balanced(*ports):
    from rill.engine.outputport import LoadBalancedOutputCollection
    return LoadBalancedOutputCollection(ports)


def forked(*ports):
    from rill.engine.outputport import ForkedOutputCollection
    return ForkedOutputCollection(ports)
