from __future__ import absolute_import, print_function

from rill.utils.annotations import ProxyAnnotation, FlagAnnotation
from rill.engine.portdef import InputPortDefinition, OutputPortDefinition
from rill.compat import *

from typing import Union, Callable, Type

__all__ = ['inport', 'outport', 'must_run', 'self_starting', 'component', 'subnet']


class inport(ProxyAnnotation):
    multi = True
    attribute = '_inport_definitions'
    proxy_type = InputPortDefinition


class outport(ProxyAnnotation):
    multi = True
    attribute = '_outport_definitions'
    proxy_type = OutputPortDefinition


# FIXME: could be a nice feature to make users add an explanation, e.g.
#  @must_run("file must always be opened for writing so that data is reset")
class must_run(FlagAnnotation):
    """
    A component decorated with `must_run` is activated once even if all
    upstream packets are drained.
    """
    default = True


class self_starting(FlagAnnotation):
    """
    A component decorated with `self_starting` does not need to receive any
    upstream packets to begin sending packets.
    """
    default = False


ANNOTATIONS = (
    inport,
    outport,
    must_run,
    self_starting
)


def component(name_or_func=None, **kwargs):
    """
    Decorator to create a component from a function.
    """
    from rill.engine.component import _FunctionComponent

    def decorator(func):
        name_ = name or func.__name__
        attrs = {
            'type_name': name_,
            '_pass_context': kwargs.get('pass_context', False),
            '_execute': staticmethod(func),
            '__doc__': getattr(func, '__doc__', None),
            '__module__': func.__module__,
        }
        cls = type(name_,
                   (kwargs.get('base_class', _FunctionComponent),),
                   attrs)
        # transfer annotations from func to cls
        for ann in ANNOTATIONS:
            val = ann.pop(func)
            if val is not None:
                ann.set(cls, val)
        return cls

    if callable(name_or_func):
        # @component
        if kwargs:
            raise ValueError("If you call @component with a callable **kwargs "
                             "is ignored")
        # name_or_func is the object we are decorating
        f = name_or_func
        name = None
        # call the decorator immediately
        return decorator(f)
    else:
        # @component('name')
        assert name_or_func is None or isinstance(name_or_func, basestring)
        name = name_or_func
        # return the decorator
        return decorator


def subnet(name_or_func):
    """
    Decorator for creating subnet

    Callback expects a function with one argument, the Network that will be
    wrapped by the SubGraph component.

    Parameters
    ----------
    name_or_func : Union[Callable, str]

    Returns
    -------
    subnet : Type[``rill.engine.subnet.SubGraph``]
    """
    from rill.engine.subnet import SubGraph

    def decorator(func):
        def define(cls, graph):
            func(graph)

        name_ = name or func.__name__
        attrs = {
            'name': name_,
            'define': classmethod(define),
            '__doc__': getattr(func, '__doc__', None),
            '__module__': func.__module__,
        }
        cls = type(name_, (SubGraph,), attrs)
        # transfer annotations from func to cls
        # FIXME: not sure if all of the annotations make sense for subnets...
        for ann in ANNOTATIONS:
            val = ann.pop(func)
            if val is not None:
                ann.set(cls, val)
        return cls

    if callable(name_or_func):
        # @subnet
        f = name_or_func
        name = None
        return decorator(f)
    else:
        # @subnet('name')
        assert name_or_func is None or isinstance(name_or_func, basestring)
        name = name_or_func
        return decorator
